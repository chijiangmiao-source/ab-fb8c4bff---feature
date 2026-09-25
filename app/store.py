"""标定谱系存储层。

核心不变量（均在单个 SQLite 写事务内保证）：

1. 一条失效裁决使目标记录及全部可达下游记录在同一持久化提交中失效；
2. 操作标识幂等：重复裁决返回首次结果；同一操作标识改换目标 -> 冲突且不改状态；
3. 新建推导记录时，任一前序不存在 / 已失效 / 自引用 / 成环 -> 整笔拒绝，既有结论不变；
4. 写事务串行化（BEGIN IMMEDIATE），因此“新推导”与“失效裁决”竞争后，
   不可能存在有效记录依赖失效记录；
5. 密封转交包接入：外部标识映射与整包记录在同一持久化提交建立；
   重复接入同包返回首次映射；包内任何缺失祖先 / 重复外部标识 / 摘要不符 /
   环 / 已失效依据均整笔原子拒绝，本地既有结论不改变。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from .errors import StoreError as _StoreError
from .packages import make_envelope, record_external_id, validate_envelope

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL CHECK (kind IN ('raw', 'derived')),
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('valid', 'invalid')),
    invalidated_by  TEXT,
    invalidated_at  TEXT,
    created_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS edges (
    child_id  TEXT NOT NULL REFERENCES records(id),
    parent_id TEXT NOT NULL REFERENCES records(id),
    seq       INTEGER NOT NULL,
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_edges_parent ON edges(parent_id);
CREATE TABLE IF NOT EXISTS operations (
    operation_id     TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    target_record_id TEXT NOT NULL,
    response_json    TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS packages (
    package_id        TEXT PRIMARY KEY,
    payload_digest    TEXT NOT NULL,
    root_external_id  TEXT NOT NULL,
    root_record_id    TEXT NOT NULL REFERENCES records(id),
    response_json     TEXT NOT NULL,
    created_at        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ext_mappings (
    external_id  TEXT PRIMARY KEY,
    package_id   TEXT NOT NULL REFERENCES packages(package_id),
    record_id    TEXT NOT NULL UNIQUE REFERENCES records(id),
    seq          INTEGER NOT NULL,
    created_at   TEXT NOT NULL
);
"""


# 向后兼容：历史代码从 app.store 导入 StoreError，实际定义已移至 app.errors
# （packages 校验模块也需要它，抽离可避免循环导入）。
StoreError = _StoreError


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class CalibrationStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # check_same_thread=False + 进程内互斥锁：所有写事务串行，
        # 读也走同一连接，保证读到已提交状态。
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ #
    # 读取
    # ------------------------------------------------------------------ #
    def get_record(self, record_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)
            ).fetchone()
            if row is None:
                raise StoreError("RECORD_NOT_FOUND",
                                 f"记录 {record_id} 不存在", status=404,
                                 details={"record_id": record_id})
            parents = [r["parent_id"] for r in self._conn.execute(
                "SELECT parent_id FROM edges WHERE child_id=? ORDER BY seq",
                (record_id,))]
            return self._row_to_dict(row, parents)

    def list_records(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM records ORDER BY created_at, id").fetchall()
            edge_rows = self._conn.execute(
                "SELECT child_id, parent_id FROM edges ORDER BY child_id, seq"
            ).fetchall()
        parents: dict[str, list[str]] = {}
        for e in edge_rows:
            parents.setdefault(e["child_id"], []).append(e["parent_id"])
        return [self._row_to_dict(r, parents.get(r["id"], [])) for r in rows]

    def get_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM operations WHERE operation_id=?",
                (operation_id,)).fetchone()
        if row is None:
            return None
        return json.loads(row["response_json"])

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, parents: list[str]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "parent_ids": parents,
            "invalidated_by": row["invalidated_by"],
            "invalidated_at": row["invalidated_at"],
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------ #
    # 创建原始 / 推导记录
    # ------------------------------------------------------------------ #
    def create_record(self, kind: str, payload: dict[str, Any],
                      parent_ids: list[str] | None,
                      record_id: str | None = None) -> dict[str, Any]:
        if kind not in ("raw", "derived"):
            raise StoreError("INVALID_KIND", f"未知记录类型 {kind!r}",
                             status=400, details={"kind": kind})
        parent_ids = list(parent_ids or [])

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rid = record_id or self._allocate_id_locked()

                if self._conn.execute(
                        "SELECT 1 FROM records WHERE id=?", (rid,)).fetchone():
                    raise StoreError("RECORD_ID_CONFLICT",
                                     f"记录编号 {rid} 已存在", status=409,
                                     details={"record_id": rid})

                if kind == "raw" and parent_ids:
                    raise StoreError(
                        "RAW_RECORD_HAS_PARENTS",
                        "原始标定记录不能引用前序记录", status=400,
                        details={"parent_ids": parent_ids})
                if kind == "derived" and not parent_ids:
                    raise StoreError(
                        "DERIVED_RECORD_WITHOUT_BASIS",
                        "推导标定记录必须至少选择一个当前有效的前序记录",
                        details={"record_id": rid})

                # 自引用（id 尚未落库也必须拦截）
                if rid in parent_ids:
                    raise StoreError(
                        "SELF_REFERENCE",
                        f"推导记录 {rid} 不能引用自身作为前序依据",
                        details={"record_id": rid, "parent_ids": parent_ids})

                if len(set(parent_ids)) != len(parent_ids):
                    dup = sorted({p for p in parent_ids
                                  if parent_ids.count(p) > 1})
                    raise StoreError("DUPLICATE_PARENT",
                                     "前序记录重复出现",
                                     details={"duplicate_parent_ids": dup})

                # 存在性 + 有效性校验（全部在写事务内读到的是已提交快照）
                placeholders = ",".join("?" * len(parent_ids))
                found = {r["id"]: r for r in self._conn.execute(
                    f"SELECT id, status FROM records WHERE id IN ({placeholders})",
                    parent_ids)} if parent_ids else {}
                missing = [p for p in parent_ids if p not in found]
                if missing:
                    raise StoreError(
                        "PARENT_NOT_FOUND",
                        f"前序记录 {', '.join(missing)} 不存在",
                        details={"missing_parent_ids": missing})
                invalid_parents = [p for p in parent_ids
                                   if found[p]["status"] != "valid"]
                if invalid_parents:
                    raise StoreError(
                        "PARENT_INVALID",
                        f"前序记录 {', '.join(invalid_parents)} 已失效，"
                        "不能作为新推导的依据",
                        details={"invalid_parent_ids": invalid_parents})

                # 成环检查：新节点沿 parent 方向可达自身即成环。
                if self._reaches_ancestor_locked(rid, set(parent_ids)):
                    raise StoreError(
                        "CYCLE_DETECTED",
                        "引用关系形成环",
                        details={"record_id": rid, "parent_ids": parent_ids})

                now = _utcnow()
                self._conn.execute(
                    "INSERT INTO records (id, kind, payload, status, "
                    "invalidated_by, invalidated_at, created_at) "
                    "VALUES (?, ?, ?, 'valid', NULL, NULL, ?)",
                    (rid, kind, json.dumps(payload, ensure_ascii=False), now))
                for seq, pid in enumerate(parent_ids):
                    self._conn.execute(
                        "INSERT INTO edges (child_id, parent_id, seq) "
                        "VALUES (?, ?, ?)", (rid, pid, seq))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self.get_record(rid)

    def _allocate_id_locked(self) -> str:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key='seq'").fetchone()
        seq = (int(row["value"]) + 1) if row else 1
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES('seq', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(seq),))
        return f"R{seq:06d}"

    def _reaches_ancestor_locked(self, target: str,
                                 starts: set[str]) -> bool:
        """从 starts 沿 child->parent 边向上能否到达 target。"""
        if not starts:
            return False
        frontier = set(starts)
        seen: set[str] = set()
        while frontier:
            if target in frontier:
                return True
            seen |= frontier
            qmarks = ",".join("?" * len(frontier))
            rows = self._conn.execute(
                f"SELECT parent_id FROM edges WHERE child_id IN ({qmarks})",
                tuple(frontier)).fetchall()
            frontier = {r["parent_id"] for r in rows} - seen
        return False

    # ------------------------------------------------------------------ #
    # 失效裁决（级联，单事务）
    # ------------------------------------------------------------------ #
    def invalidate(self, operation_id: str,
                   target_id: str) -> dict[str, Any]:
        if not operation_id:
            raise StoreError("OPERATION_ID_REQUIRED",
                             "失效裁决必须携带操作标识", status=400)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 1) 操作标识幂等 / 冲突判定（在同一写事务内）
                op = self._conn.execute(
                    "SELECT kind, target_record_id, response_json "
                    "FROM operations WHERE operation_id=?",
                    (operation_id,)).fetchone()
                if op is not None:
                    if (op["kind"] == "invalidate"
                            and op["target_record_id"] == target_id):
                        # 重复同一裁决：原样返回首次结果，不改状态
                        response = json.loads(op["response_json"])
                        response["replayed"] = True
                        self._conn.execute("COMMIT")
                        return response
                    raise StoreError(
                        "OPERATION_CONFLICT",
                        f"操作标识 {operation_id} 已用于 "
                        f"{op['kind']}({op['target_record_id']})，"
                        f"不能改用于 invalidate({target_id})",
                        status=409,
                        details={"operation_id": operation_id,
                                 "original_target": op["target_record_id"],
                                 "conflicting_target": target_id})

                # 2) 目标必须存在（不记录该操作标识，允许客户端修正后重试）
                target = self._conn.execute(
                    "SELECT id, status FROM records WHERE id=?", (target_id,)
                ).fetchone()
                if target is None:
                    raise StoreError(
                        "RECORD_NOT_FOUND",
                        f"裁决目标记录 {target_id} 不存在", status=404,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})
                if target["status"] != "valid":
                    # 已有稳定失效来源，不得被新裁决覆盖
                    raise StoreError(
                        "RECORD_ALREADY_INVALID",
                        f"记录 {target_id} 已失效，失效来源稳定，"
                        "不能再次裁决",
                        status=409,
                        details={"record_id": target_id,
                                 "operation_id": operation_id})

                # 3) 求目标 + 全部可达下游闭包
                closure = self._downstream_closure_locked(target_id)

                # 4) 同一提交内将闭包中仍有效的节点失效；
                #    早已失效的节点保留其首次失效来源（稳定来源）。
                now = _utcnow()
                self._conn.execute(
                    "UPDATE records SET status='invalid', "
                    "invalidated_by=?, invalidated_at=? "
                    "WHERE id IN (%s) AND status='valid'"
                    % ",".join("?" * len(closure)),
                    (target_id, now, *closure))

                response = {
                    "operation_id": operation_id,
                    "result": "completed",
                    "replayed": False,
                    "target_record_id": target_id,
                    "cascade": [
                        {"id": rid, "invalidated_by": target_id}
                        for rid in closure
                    ],
                }
                self._conn.execute(
                    "INSERT INTO operations (operation_id, kind, "
                    "target_record_id, response_json, created_at) "
                    "VALUES (?, 'invalidate', ?, ?, ?)",
                    (operation_id, target_id,
                     json.dumps(response, ensure_ascii=False), now))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return response

    def _downstream_closure_locked(self, target_id: str) -> list[str]:
        """目标及其全部可达下游（沿 parent->child 传播）。"""
        rows = self._conn.execute(
            """
            WITH RECURSIVE reach(id) AS (
                SELECT ?
                UNION
                SELECT e.child_id
                FROM edges e JOIN reach r ON e.parent_id = r.id
            )
            SELECT id FROM reach ORDER BY id
            """, (target_id,)).fetchall()
        return [r["id"] for r in rows]

    # ------------------------------------------------------------------ #
    # 密封转交包：导出 / 接入
    # ------------------------------------------------------------------ #
    def export_package(self, root_id: str) -> dict[str, Any]:
        """导出一条仍有效结论及其直接依据闭包的密封转交包。

        包内记录一律按稳定外部标识（Merkle 内容寻址）引用，与本地编号无关。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM records WHERE id=?", (root_id,)).fetchone()
            if row is None:
                raise StoreError("RECORD_NOT_FOUND",
                                 f"记录 {root_id} 不存在", status=404,
                                 details={"record_id": root_id})
            if row["status"] != "valid":
                raise StoreError(
                    "PACKAGE_ROOT_NOT_VALID",
                    f"结论 {root_id} 已失效，不能密封转交仍有效的结论",
                    status=409,
                    details={"record_id": root_id,
                             "invalidated_by": row["invalidated_by"]})

            closure = self._basis_closure_locked(root_id)
            invalid_rows = [r for r in closure if r["status"] != "valid"]
            if invalid_rows:
                # 防御性：与 assert_invariants 同源的不变量保证下不应发生
                raise StoreError(
                    "PACKAGE_BASIS_INVALID",
                    "依据闭包中存在已失效记录，不能密封转交",
                    details={"root_record_id": root_id,
                             "invalid_record_ids":
                                 sorted(r["id"] for r in invalid_rows)})

            # 自最深祖先起算外部标识（Merkle：父标识是子标识的输入）
            ordered = sorted(closure, key=lambda r: (-r["depth"], r["id"]))
            ext_of: dict[str, str] = {}
            records_out: list[dict[str, Any]] = []
            for cr in ordered:
                parent_local = [e["parent_id"] for e in self._conn.execute(
                    "SELECT parent_id FROM edges WHERE child_id=? "
                    "ORDER BY seq", (cr["id"],))]
                parent_ext = [ext_of[p] for p in parent_local]
                ext = record_external_id(
                    cr["kind"], json.loads(cr["payload"]), parent_ext)
                ext_of[cr["id"]] = ext
                records_out.append({
                    "external_id": ext,
                    "kind": cr["kind"],
                    "payload": json.loads(cr["payload"]),
                    "parent_external_ids": parent_ext,
                })

            envelope = make_envelope(ext_of[root_id], records_out)
            root_parent_ids = [e["parent_id"] for e in self._conn.execute(
                "SELECT parent_id FROM edges WHERE child_id=? ORDER BY seq",
                (root_id,))]
            return {
                "package": envelope,
                "root_record_id": root_id,
                "root_external_id": envelope["root_external_id"],
                "root_parent_ids": root_parent_ids,
                "record_count": len(records_out),
            }

    def _basis_closure_locked(self, root_id: str):
        """根结论及其全部祖先（沿 child->parent），带每个节点的最大祖先深度。"""
        return self._conn.execute(
            """
            WITH RECURSIVE anc(id, depth) AS (
                SELECT ?, 0
                UNION ALL
                SELECT e.parent_id, a.depth + 1
                FROM edges e JOIN anc a ON e.child_id = a.id
            )
            SELECT r.id, r.kind, r.payload, r.status,
                   MAX(a.depth) AS depth
            FROM anc a JOIN records r ON r.id = a.id
            GROUP BY a.id
            """, (root_id,)).fetchall()

    def import_package(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """接入密封转交包。

        校验（纯函数，锁外）：缺失祖先 / 重复外部标识 / 摘要不符 / 环等。
        接入（单写事务）：建立整包外部标识映射并写入全部记录。
        重复接入完全相同的包 -> 返回首次映射（replayed=True）；
        同一包标识载荷（摘要）不同 -> 409 PACKAGE_CONFLICT，状态不变。
        """
        # 先做纯内容校验：失败时根本不开事务
        package_id, digest, root_ext, records = validate_envelope(envelope)

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 身份判定：包标识是声明，摘要是内容真身。
                #  - 同包标识 + 同摘要：完全相同的包，返回首次映射；
                #  - 同包标识 + 不同摘要：冒用标识的不同载荷 -> 冲突；
                #  - 不同包标识 + 同摘要：内容完全一致（诚实包的标识本就由
                #    摘要派生），同样回到首次映射，保证映射按内容稳定。
                by_id = self._conn.execute(
                    "SELECT package_id, payload_digest, response_json "
                    "FROM packages WHERE package_id=?",
                    (package_id,)).fetchone()
                by_digest = self._conn.execute(
                    "SELECT package_id, payload_digest, response_json "
                    "FROM packages WHERE payload_digest=?",
                    (digest,)).fetchone()
                if by_id is not None:
                    if by_id["payload_digest"] == digest:
                        response = json.loads(by_id["response_json"])
                        response["replayed"] = True
                        self._conn.execute("COMMIT")
                        return self._attach_root_record(response)
                    raise StoreError(
                        "PACKAGE_CONFLICT",
                        f"包标识 {package_id} 已接入过不同载荷的包，"
                        "不能以同一标识接入不同内容",
                        status=409,
                        details={"package_id": package_id,
                                 "existing_digest":
                                     by_id["payload_digest"],
                                 "incoming_digest": digest})
                if by_digest is not None:
                    response = json.loads(by_digest["response_json"])
                    response["replayed"] = True
                    response["replayed_different_package_id"] = package_id
                    self._conn.execute("COMMIT")
                    return self._attach_root_record(response)

                # 包内外部标识在本地的既有映射：
                #  - 已失效依据 -> 定位拒绝（不得让新有效结论依赖失效依据）；
                #  - 仍有效的既有映射 -> 复用同一本地记录（Merkle 内容寻址：
                #    外部标识相同即内容与依据完全相同），不重复落库。
                ext_list = list(records)
                qmarks = ",".join("?" * len(ext_list))
                mapped = self._conn.execute(
                    f"SELECT m.external_id AS external_id, m.package_id "
                    f"AS package_id, m.record_id AS record_id, r.status "
                    f"AS status FROM ext_mappings m "
                    f"JOIN records r ON r.id = m.record_id "
                    f"WHERE m.external_id IN ({qmarks})",
                    tuple(ext_list)).fetchall()

                invalid_basis = [
                    {"external_id": m["external_id"],
                     "record_id": m["record_id"]}
                    for m in mapped if m["status"] != "valid"]
                if invalid_basis:
                    invalid_ids = {x["external_id"] for x in invalid_basis}
                    referenced_by: dict[str, list[str]] = {}
                    for ext in records:
                        for parent in records[ext]["parent_external_ids"]:
                            if parent in invalid_ids:
                                referenced_by.setdefault(
                                    parent, []).append(ext)
                    raise StoreError(
                        "PACKAGE_BASIS_INVALID",
                        "包引用的外部依据在本地已失效，不能接入，"
                        "以免有效结论依赖失效依据",
                        details={"invalid_basis": invalid_basis,
                                 "referenced_by": {
                                     k: sorted(set(v))
                                     for k, v in referenced_by.items()}})

                # 拓扑顺序：祖先（依据）先写，根最后写，满足边外键
                depth_of: dict[str, int] = {}

                def depth(ext: str) -> int:
                    if ext not in depth_of:
                        parents = records[ext]["parent_external_ids"]
                        depth_of[ext] = (
                            0 if not parents
                            else 1 + max(depth(p) for p in parents))
                    return depth_of[ext]

                order = sorted(records, key=lambda e: (depth(e), e))
                now = _utcnow()
                local_of: dict[str, str] = {
                    m["external_id"]: m["record_id"] for m in mapped}
                mapping: list[dict[str, Any]] = []
                imported = 0
                for ext in order:
                    rec = records[ext]
                    reused = ext in local_of
                    if reused:
                        rid = local_of[ext]
                    else:
                        rid = self._allocate_id_locked()
                        self._conn.execute(
                            "INSERT INTO records (id, kind, payload, status, "
                            "invalidated_by, invalidated_at, created_at) "
                            "VALUES (?, ?, ?, 'valid', NULL, NULL, ?)",
                            (rid, rec["kind"],
                             json.dumps(rec["payload"], ensure_ascii=False),
                             now))
                        for edge_seq, parent_ext in enumerate(
                                rec["parent_external_ids"]):
                            self._conn.execute(
                                "INSERT INTO edges "
                                "(child_id, parent_id, seq) "
                                "VALUES (?, ?, ?)",
                                (rid, local_of[parent_ext], edge_seq))
                        local_of[ext] = rid
                        imported += 1
                    mapping.append({
                        "external_id": ext,
                        "record_id": rid,
                        "reused": reused,
                    })

                root_record_id = local_of[root_ext]
                response = {
                    "result": "imported",
                    "replayed": False,
                    "package_id": package_id,
                    "payload_digest": digest,
                    "root_external_id": root_ext,
                    "root_record_id": root_record_id,
                    "record_count": len(order),
                    "imported_count": imported,
                    "reused_count": len(order) - imported,
                    "mapping": mapping,
                    "created_at": now,
                }
                self._conn.execute(
                    "INSERT INTO packages (package_id, payload_digest, "
                    "root_external_id, root_record_id, response_json, "
                    "created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (package_id, digest, root_ext, root_record_id,
                     json.dumps(response, ensure_ascii=False), now))
                # 仅为本次新写入的记录建立映射；复用记录保留其首次映射
                new_seq = 0
                for item in mapping:
                    if item["reused"]:
                        continue
                    self._conn.execute(
                        "INSERT INTO ext_mappings (external_id, package_id, "
                        "record_id, seq, created_at) VALUES (?, ?, ?, ?, ?)",
                        (item["external_id"], package_id,
                         item["record_id"], new_seq, now))
                    new_seq += 1
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self._attach_root_record(response)

    def _attach_root_record(self, response: dict[str, Any]) -> dict[str, Any]:
        response["root_record"] = self.get_record(
            response["root_record_id"])
        return response

    def get_package(self, package_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM packages WHERE package_id=?",
                (package_id,)).fetchone()
        if row is None:
            raise StoreError("PACKAGE_NOT_FOUND",
                             f"转交包 {package_id} 未接入过", status=404,
                             details={"package_id": package_id})
        return self._attach_root_record(json.loads(row["response_json"]))

    # ------------------------------------------------------------------ #
    # 完整性自检（验收用）
    # ------------------------------------------------------------------ #
    def assert_invariants(self) -> None:
        """有效记录不得依赖失效记录。"""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT c.id AS child_id, p.id AS parent_id
                FROM records c
                JOIN edges e ON e.child_id = c.id
                JOIN records p ON p.id = e.parent_id
                WHERE c.status='valid' AND p.status='invalid'
                LIMIT 1
                """).fetchone()
        if row is not None:
            raise AssertionError(
                f"不变量被破坏：有效记录 {row['child_id']} "
                f"依赖失效记录 {row['parent_id']}")
