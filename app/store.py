"""标定谱系存储层。

核心不变量（均在单个 SQLite 写事务内保证）：

1. 一条失效裁决使目标记录及全部可达下游记录在同一持久化提交中失效；
2. 操作标识幂等：重复裁决返回首次结果；同一操作标识改换目标 -> 冲突且不改状态；
3. 新建推导记录时，任一前序不存在 / 已失效 / 自引用 / 成环 -> 整笔拒绝，既有结论不变；
4. 写事务串行化（BEGIN IMMEDIATE），因此“新推导”“失效裁决”“转交接入”竞争后，
   不可能存在有效记录依赖失效记录；
5. 转交包接入：结构/摘要/图校验全部通过后，在同一提交内建立外部标识映射并
   写入全部记录；同包重复接入返回首次映射；同包标识不同载荷冲突；本地既有结论不变。

每条记录出生即带稳定外部标识 ext_id（跨实例），本地编号 id（R000001…）仅本实例有效。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

from . import package as pkg
from .errors import StoreError

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    id              TEXT PRIMARY KEY,
    ext_id          TEXT NOT NULL UNIQUE,
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
-- 外部标识 -> 本地编号映射：整包接入时在同一提交内建立
CREATE TABLE IF NOT EXISTS ext_mapping (
    ext_id     TEXT PRIMARY KEY,
    record_id  TEXT NOT NULL UNIQUE REFERENCES records(id),
    package_id TEXT,
    created_at TEXT NOT NULL
);
-- 已接入转交包登记：包标识 -> 首次摘要 / 首次映射结果
CREATE TABLE IF NOT EXISTS imports (
    package_id      TEXT PRIMARY KEY,
    digest          TEXT NOT NULL,
    root_ext_id     TEXT NOT NULL,
    root_record_id  TEXT NOT NULL,
    response_json   TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _topo_order(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """父在前的稳定拓扑序（调用前已保证无环、无缺失祖先）。"""
    by_ext = {r["ext_id"]: r for r in records}
    ordered: list[dict[str, Any]] = []
    placed: set[str] = set()

    def visit(ext: str) -> None:
        if ext in placed:
            return
        for p in by_ext[ext]["parent_ext_ids"]:
            if p in by_ext:
                visit(p)
        placed.add(ext)
        ordered.append(by_ext[ext])

    for r in records:
        visit(r["ext_id"])
    return ordered


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
            self._migrate_ext_id()

    def _migrate_ext_id(self) -> None:
        """旧库 records 可能没有 ext_id 列，补齐并回填稳定外部标识。"""
        cols = {r["name"] for r in
                self._conn.execute("PRAGMA table_info(records)")}
        if "ext_id" not in cols:
            self._conn.execute(
                "ALTER TABLE records ADD COLUMN ext_id TEXT")
            rows = self._conn.execute("SELECT id FROM records").fetchall()
            for r in rows:
                self._conn.execute(
                    "UPDATE records SET ext_id=? WHERE id=?",
                    (pkg.new_ext_id(), r["id"]))
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_records_ext "
                "ON records(ext_id)")

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

    def get_import(self, package_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM imports WHERE package_id=?",
                (package_id,)).fetchone()
        return json.loads(row["response_json"]) if row else None

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, parents: list[str]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "ext_id": row["ext_id"],
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "parent_ids": parents,
            "invalidated_by": row["invalidated_by"],
            "invalidated_at": row["invalidated_at"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _content_signature(kind: str, payload: dict[str, Any]) -> str:
        return f"{kind}|{json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))}"

    # ------------------------------------------------------------------ #
    # 创建原始 / 推导记录
    # ------------------------------------------------------------------ #
    def create_record(self, kind: str, payload: dict[str, Any],
                      parent_ids: list[str] | None,
                      record_id: str | None = None,
                      ext_id: str | None = None) -> dict[str, Any]:
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

                ext = ext_id or pkg.new_ext_id()
                if self._conn.execute(
                        "SELECT 1 FROM records WHERE ext_id=?",
                        (ext,)).fetchone():
                    raise StoreError("EXT_ID_CONFLICT",
                                     f"外部标识 {ext} 已存在", status=409,
                                     details={"ext_id": ext})

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

                self._insert_record_locked(rid, ext, kind, payload,
                                           parent_ids)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self.get_record(rid)

    def _insert_record_locked(self, rid: str, ext: str, kind: str,
                              payload: dict[str, Any],
                              parent_ids: list[str]) -> None:
        now = _utcnow()
        self._conn.execute(
            "INSERT INTO records (id, ext_id, kind, payload, status, "
            "invalidated_by, invalidated_at, created_at) "
            "VALUES (?, ?, ?, ?, 'valid', NULL, NULL, ?)",
            (rid, ext, kind, json.dumps(payload, ensure_ascii=False), now))
        for seq, pid in enumerate(parent_ids):
            self._conn.execute(
                "INSERT INTO edges (child_id, parent_id, seq) "
                "VALUES (?, ?, ?)", (rid, pid, seq))

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

    def _ancestor_closure_rows_locked(self, root_id: str) -> list[sqlite3.Row]:
        """根及其全部祖先（沿 child->parent 传播）的记录行。"""
        return self._conn.execute(
            """
            WITH RECURSIVE reach(id) AS (
                SELECT ?
                UNION
                SELECT e.parent_id
                FROM edges e JOIN reach r ON e.child_id = r.id
            )
            SELECT rec.* FROM records rec
            JOIN reach ON reach.id = rec.id
            ORDER BY rec.created_at, rec.id
            """, (root_id,)).fetchall()

    # ------------------------------------------------------------------ #
    # 密封转交包：导出
    # ------------------------------------------------------------------ #
    def export_package(self, root_id: str) -> dict[str, Any]:
        """选定一条仍有效的结论，导出其完整有效依据闭包密封包。"""
        with self._lock:
            root = self._conn.execute(
                "SELECT id, ext_id, status FROM records WHERE id=?",
                (root_id,)).fetchone()
            if root is None:
                raise StoreError("RECORD_NOT_FOUND",
                                 f"转交结论 {root_id} 不存在", status=404,
                                 details={"record_id": root_id})
            if root["status"] != "valid":
                raise StoreError(
                    "PACKAGE_ROOT_INVALID",
                    f"转交结论 {root_id} 已失效，只允许转交仍有效的结论",
                    status=409, details={"record_id": root_id})

            rows = self._ancestor_closure_rows_locked(root_id)
            invalid = [r["id"] for r in rows if r["status"] != "valid"]
            if invalid:
                # 由既有不变量，有效根不会有失效祖先；防御性拦截并定位。
                raise StoreError(
                    "PACKAGE_BASIS_INVALID",
                    f"转交闭包中存在已失效依据：{', '.join(invalid)}",
                    status=409, details={"invalid_record_ids": invalid})

            edge_rows = self._conn.execute(
                """
                WITH RECURSIVE reach(id) AS (
                    SELECT ?
                    UNION
                    SELECT e.parent_id
                    FROM edges e JOIN reach r ON e.child_id = r.id
                )
                SELECT e.child_id, e.parent_id, e.seq
                FROM edges e JOIN reach ON reach.id = e.child_id
                ORDER BY e.child_id, e.seq
                """, (root_id,)).fetchall()

        parents_by_child: dict[str, list[str]] = {}
        ext_by_local: dict[str, str] = {r["id"]: r["ext_id"] for r in rows}
        for e in edge_rows:
            parents_by_child.setdefault(e["child_id"], []).append(e["parent_id"])

        records = []
        for r in rows:
            records.append({
                "ext_id": r["ext_id"],
                "kind": r["kind"],
                "payload": json.loads(r["payload"]),
                "parent_ext_ids": [ext_by_local[p] for p in
                                   parents_by_child.get(r["id"], [])],
            })
        envelope = pkg.build_package(root["ext_id"], records)
        return envelope

    # ------------------------------------------------------------------ #
    # 密封转交包：接入（单事务：建映射 + 写全部记录）
    # ------------------------------------------------------------------ #
    def import_package(self, raw: Any) -> dict[str, Any]:
        """接入另一实例导出的密封转交包。

        - 先做与库无关的完整结构/摘要/图校验；
        - 同一 package_id 且同摘要：返回首次映射（replayed=true）；
        - 同一 package_id 不同摘要：409 PACKAGE_CONFLICT；
        - 任一外部标识已映射到**已失效**本地记录：拒绝（已失效依据可定位）；
        - 新外部标识在同一提交内分配本地编号、写映射、写全部记录与边。
        """
        envelope = pkg.parse_and_validate(raw)
        package_id = envelope["package_id"]
        digest = envelope["digest"]
        root_ext = envelope["root_ext_id"]
        records = envelope["records"]  # 父在前拓扑序

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT digest, response_json FROM imports "
                    "WHERE package_id=?", (package_id,)).fetchone()
                if existing is not None:
                    if existing["digest"] != digest:
                        raise StoreError(
                            "PACKAGE_CONFLICT",
                            f"包标识 {package_id} 已接入过不同载荷"
                            "（摘要不符），拒绝接入",
                            status=409,
                            details={"package_id": package_id,
                                     "original_digest": existing["digest"],
                                     "conflicting_digest": digest})
                    response = json.loads(existing["response_json"])
                    response["replayed"] = True
                    self._conn.execute("COMMIT")
                    return response

                # 解析既有 ext_id -> 本地记录，判定复用 / 失效 / 冲突。
                ext_records = {r["ext_id"]: r for r in records}
                mapping: dict[str, str] = {}
                reused: list[str] = []
                invalid_basis: list[str] = []
                conflicting: list[str] = []

                for ext, r in ext_records.items():
                    row = self._conn.execute(
                        "SELECT m.record_id AS rid, rec.status AS status, "
                        "rec.kind AS kind, rec.payload AS payload "
                        "FROM ext_mapping m JOIN records rec "
                        "ON rec.id = m.record_id WHERE m.ext_id=?",
                        (ext,)).fetchone()
                    if row is None:
                        continue
                    local_id = row["rid"]
                    if row["status"] != "valid":
                        invalid_basis.append(
                            {"ext_id": ext, "record_id": local_id})
                        continue
                    # 复用共享祖先：内容必须一致
                    sig_new = self._content_signature(
                        r["kind"], r["payload"])
                    sig_old = self._content_signature(
                        row["kind"], json.loads(row["payload"]))
                    # 且该既有记录在本地的来源依据（外部标识集合）必须与包
                    # 内声称的直接依据一致，否则为矛盾谱系，不能复用。
                    local_parent_exts = {
                        pr["ext_id"] for pr in self._conn.execute(
                            "SELECT p.ext_id AS ext_id FROM edges e "
                            "JOIN records p ON p.id = e.parent_id "
                            "WHERE e.child_id=?", (local_id,)).fetchall()}
                    claimed_parent_exts = set(r["parent_ext_ids"])
                    if sig_new != sig_old \
                            or local_parent_exts != claimed_parent_exts:
                        conflicting.append({
                            "ext_id": ext, "record_id": local_id,
                            "reason": ("content" if sig_new != sig_old
                                       else "basis"),
                            "local_parent_ext_ids":
                                sorted(local_parent_exts),
                            "claimed_parent_ext_ids":
                                sorted(claimed_parent_exts),
                        })
                        continue
                    mapping[ext] = local_id
                    reused.append(ext)

                if invalid_basis:
                    raise StoreError(
                        "PARENT_INVALID",
                        "转交依据中存在本地已失效记录，不能接入",
                        status=422,
                        details={"invalid_basis": invalid_basis})
                if conflicting:
                    raise StoreError(
                        "EXT_CONTENT_CONFLICT",
                        "外部标识已映射到内容不同的本地记录",
                        status=409, details={"conflicts": conflicting})

                # 为新 ext_id 分配本地编号
                now = _utcnow()
                new_nodes = [r for r in records
                             if r["ext_id"] not in mapping]
                allocated: dict[str, str] = {}
                for r in new_nodes:
                    local_id = self._allocate_id_locked()
                    allocated[r["ext_id"]] = local_id
                    mapping[r["ext_id"]] = local_id

                # 按父在前的拓扑序落库，保证子记录插入时其（同为新增的）
                # 父记录已存在，满足外键即时约束。
                ordered = _topo_order(records)
                for r in ordered:
                    if r["ext_id"] not in allocated:
                        continue  # 复用的既有节点
                    local_id = allocated[r["ext_id"]]
                    parent_local = [mapping[p]
                                    for p in r["parent_ext_ids"]]
                    self._insert_record_locked(
                        local_id, r["ext_id"], r["kind"],
                        r["payload"], parent_local)
                    self._conn.execute(
                        "INSERT INTO ext_mapping (ext_id, record_id, "
                        "package_id, created_at) VALUES (?, ?, ?, ?)",
                        (r["ext_id"], local_id, package_id, now))

                root_local = mapping[root_ext]
                root_direct_parents = [mapping[p] for p in
                                       ext_records[root_ext]["parent_ext_ids"]]
                response = {
                    "result": "imported",
                    "replayed": False,
                    "package_id": package_id,
                    "digest": digest,
                    "root_ext_id": root_ext,
                    "root_record_id": root_local,
                    "direct_parent_ids": root_direct_parents,
                    "reused_ext_ids": sorted(reused),
                    "mapping": [{"ext_id": ext, "record_id": mapping[ext]}
                                for ext in sorted(mapping)],
                }
                self._conn.execute(
                    "INSERT INTO imports (package_id, digest, root_ext_id, "
                    "root_record_id, response_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (package_id, digest, root_ext, root_local,
                     json.dumps(response, ensure_ascii=False), now))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self._with_live_import(response)

    def _with_live_import(self, response: dict[str, Any]) -> dict[str, Any]:
        """提交后回读，确保返回的本地编号/依据与库一致。"""
        root = self.get_record(response["root_record_id"])
        response["direct_parent_ids"] = root["parent_ids"]
        response["root_status"] = root["status"]
        return response

    # ------------------------------------------------------------------ #
    # 完整性自检（验收用）
    # ------------------------------------------------------------------ #
    def assert_invariants(self) -> None:
        """有效记录不得依赖失效或不存在记录；映射必须双向一致。"""
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
            orphan = self._conn.execute(
                """
                SELECT e.child_id, e.parent_id FROM edges e
                LEFT JOIN records p ON p.id = e.parent_id
                WHERE p.id IS NULL LIMIT 1
                """).fetchone()
            if orphan is not None:
                raise AssertionError(
                    f"不变量被破坏：记录 {orphan['child_id']} "
                    f"指向不存在依据 {orphan['parent_id']}")
            bad_map = self._conn.execute(
                """
                SELECT m.ext_id, m.record_id FROM ext_mapping m
                LEFT JOIN records r ON r.id = m.record_id
                WHERE r.id IS NULL OR r.ext_id != m.ext_id
                LIMIT 1
                """).fetchone()
            if bad_map is not None:
                raise AssertionError(
                    f"不变量被破坏：外部标识映射失配 {dict(bad_map.keys())}")
