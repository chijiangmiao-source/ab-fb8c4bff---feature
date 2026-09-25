"""密封转交包：按直接依据闭包导出 / 校验 / 接入另一实例的结论谱系。

包格式（JSON 对象，字段顺序固定）::

    {
      "format": "calibration-handoff/v1",
      "package_id": "<sha256 前 16 位，作为包稳定标识>",
      "root_ext_id": "<被转交结论的外部标识>",
      "records": [
        {"ext_id", "kind", "payload", "parent_ext_ids": [...]}
      ],
      "digest": "<sha256，对规范载荷（不含 digest）计算>"
    }

外部标识（ext_id）是记录跨实例的**稳定出生标识**：
- 本地创建时生成 UUID；导入时沿用包内来源标识；
- 包内所有边只按 ext_id 引用，与实例本地编号（R000001…）无关。

接入时所有结构/图/摘要校验通过后，在**同一个持久化提交**中建立
ext_id -> 本地编号映射并写入全部记录；校验失败整笔回滚，本地既有结论不变。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from typing import Any

from .errors import StoreError

FORMAT = "calibration-handoff/v1"
DIGEST_ALG = "sha256"


def package_id_for(root_ext_id: str) -> str:
    """包稳定标识：仅由被转交结论的外部标识派生。

    与载荷摘要相互独立：同一条结论的重复导出得到同一 package_id；
    若有人篡改载荷并伪造摘要，package_id 不变而摘要改变 -> 接入端冲突。
    """
    return "H-" + hashlib.sha256(
        ("handoff:" + root_ext_id).encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# 规范序列化与摘要
# --------------------------------------------------------------------------- #
def _canonical_payload(obj: Any) -> bytes:
    """规范载荷：sort_keys + 紧凑分隔，ensure_ascii=False，统一换行。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def compute_digest(envelope: dict[str, Any]) -> str:
    """对不含 digest 的信封部分计算摘要（规范载荷）。"""
    body = {k: v for k, v in envelope.items() if k != "digest"}
    return hashlib.sha256(_canonical_payload(body)).hexdigest()


def canonical_envelope(root_ext_id: str,
                       records: list[dict[str, Any]]) -> dict[str, Any]:
    """构造用于摘要的规范信封：记录按 ext_id 排序，与数组传入顺序无关。"""
    canon_records = sorted(records, key=lambda r: r["ext_id"])
    return {
        "format": FORMAT,
        "root_ext_id": root_ext_id,
        "records": canon_records,
    }


def digest_for(root_ext_id: str,
               records: list[dict[str, Any]]) -> str:
    return compute_digest(canonical_envelope(root_ext_id, records))


def new_ext_id() -> str:
    return uuid.uuid4().hex


# --------------------------------------------------------------------------- #
# 导出：由已取数的记录构造包
# --------------------------------------------------------------------------- #
def build_package(root_ext_id: str,
                  records: list[dict[str, Any]]) -> dict[str, Any]:
    """根据根及其祖先闭包记录构造密封包。

    每条输入记录须含 ext_id / kind / payload / parent_ext_ids；
    parent_ext_ids 必须全部落在集合内（闭包完整由调用方按闭包取数保证）。
    """
    by_ext = {r["ext_id"]: r for r in records}
    if root_ext_id not in by_ext:
        raise StoreError(
            "PACKAGE_ROOT_NOT_FOUND",
            f"转交根 {root_ext_id} 不在导出闭包内", status=422,
            details={"root_ext_id": root_ext_id})

    norm = []
    for r in records:
        parents = list(r.get("parent_ext_ids") or [])
        norm.append({
            "ext_id": r["ext_id"],
            "kind": r["kind"],
            "payload": r["payload"],
            "parent_ext_ids": parents,
        })
    # 稳定顺序：拓扑（父在前）+ ext_id 兜底，确定性输出
    norm.sort(key=lambda x: x["ext_id"])
    norm = _topo_sort(norm, root_ext_id)

    envelope = {
        "format": FORMAT,
        "root_ext_id": root_ext_id,
        "records": norm,
    }
    digest = digest_for(root_ext_id, norm)
    envelope["package_id"] = package_id_for(root_ext_id)
    envelope["digest"] = digest
    return envelope


def _topo_sort(records: list[dict[str, Any]],
               root_ext_id: str) -> list[dict[str, Any]]:
    """父在前的拓扑序（输入已按 ext_id 排序，稳定）。"""
    by_ext = {r["ext_id"]: r for r in records}
    ordered: list[dict[str, Any]] = []
    placed: set[str] = set()

    def visit(ext: str, stack: set[str]) -> None:
        if ext in placed:
            return
        if ext in stack:  # 理论上导出数据无环，防御
            raise StoreError("CYCLE_DETECTED", "导出谱系存在环",
                             details={"cycle_ext_ids": [ext]})
        stack.add(ext)
        for p in by_ext[ext]["parent_ext_ids"]:
            if p in by_ext:
                visit(p, stack)
        stack.discard(ext)
        placed.add(ext)
        ordered.append(by_ext[ext])

    visit(root_ext_id, set())
    # 闭包中可能有不被根可达的节点（不应出现），按 ext_id 补在后面
    for r in records:
        if r["ext_id"] not in placed:
            ordered.append(r)
    return ordered


# --------------------------------------------------------------------------- #
# 接入前校验：结构、摘要、引用图（纯函数，不触库，便于复用与测试）
# --------------------------------------------------------------------------- #
def parse_and_validate(raw: Any) -> dict[str, Any]:
    """解析并完整校验一个转交包，返回规范化信封。

    任何问题都抛 StoreError，details 尽量可定位。
    """
    if not isinstance(raw, dict):
        raise StoreError("INVALID_PACKAGE", "转交包必须是 JSON 对象",
                         status=400)

    fmt = raw.get("format")
    if fmt != FORMAT:
        raise StoreError("UNSUPPORTED_PACKAGE_FORMAT",
                         f"不支持的转交包格式 {fmt!r}", status=400,
                         details={"expected": FORMAT, "actual": fmt})

    root = raw.get("root_ext_id")
    if not isinstance(root, str) or not root:
        raise StoreError("INVALID_PACKAGE",
                         "root_ext_id 必须是非空字符串", status=400,
                         details={"field": "root_ext_id"})

    provided_package_id = raw.get("package_id")
    if provided_package_id is not None and (
            not isinstance(provided_package_id, str)
            or not provided_package_id):
        raise StoreError("INVALID_PACKAGE",
                         "package_id 必须是非空字符串", status=400,
                         details={"field": "package_id"})

    digest = raw.get("digest")
    if not isinstance(digest, str) or not digest:
        raise StoreError("INVALID_PACKAGE", "digest 必须是非空字符串",
                         status=400, details={"field": "digest"})

    records = raw.get("records")
    if not isinstance(records, list) or not records:
        raise StoreError("INVALID_PACKAGE", "records 必须是非空数组",
                         status=400, details={"field": "records"})

    # ---- 逐条结构校验 ----
    norm_records: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_ids: list[str] = []
    for i, r in enumerate(records):
        if not isinstance(r, dict):
            raise StoreError("INVALID_PACKAGE",
                             f"records[{i}] 必须是对象", status=400,
                             details={"index": i})
        ext = r.get("ext_id")
        if not isinstance(ext, str) or not ext:
            raise StoreError("INVALID_PACKAGE",
                             f"records[{i}].ext_id 非法", status=400,
                             details={"index": i, "field": "ext_id"})
        kind = r.get("kind")
        if kind not in ("raw", "derived"):
            raise StoreError("INVALID_PACKAGE",
                             f"records[{i}].kind 非法：{kind!r}", status=400,
                             details={"ext_id": ext, "kind": kind})
        if not isinstance(r.get("payload"), dict):
            raise StoreError("INVALID_PACKAGE",
                             f"records[{i}].payload 必须是对象", status=400,
                             details={"ext_id": ext})
        parents = r.get("parent_ext_ids")
        if not isinstance(parents, list) or not all(
                isinstance(p, str) and p for p in parents):
            raise StoreError(
                "INVALID_PACKAGE",
                f"records[{i}].parent_ext_ids 必须是字符串数组",
                status=400, details={"ext_id": ext})
        if ext in seen:
            duplicate_ids.append(ext)
        seen.add(ext)
        norm_records.append({
            "ext_id": ext, "kind": kind,
            "payload": r["payload"],
            "parent_ext_ids": list(parents),
        })

    # 重复外部标识（包内）
    if duplicate_ids:
        raise StoreError(
            "DUPLICATE_EXT_ID",
            f"转交包内外部标识重复：{', '.join(sorted(set(duplicate_ids)))}",
            status=422,
            details={"duplicate_ext_ids": sorted(set(duplicate_ids))})

    # ---- 摘要校验（对规范载荷重算，与数组顺序无关）----
    expected = digest_for(root, norm_records)
    if not _safe_equal(expected, digest):
        raise StoreError(
            "DIGEST_MISMATCH",
            "转交包摘要不符：规范载荷重算摘要与包内 digest 不一致，"
            "包可能已被篡改或损坏",
            status=422,
            details={"expected_digest": expected, "actual_digest": digest,
                     "algorithm": DIGEST_ALG})

    by_ext = {r["ext_id"]: r for r in norm_records}

    # ---- 包标识必须与根外部标识一致（防伪造/张冠李戴）----
    canonical_package_id = package_id_for(root)
    if (provided_package_id is not None
            and provided_package_id != canonical_package_id):
        raise StoreError(
            "PACKAGE_ID_MISMATCH",
            "包标识 package_id 与根外部标识不一致，包可能被篡改",
            status=422,
            details={"expected_package_id": canonical_package_id,
                     "actual_package_id": provided_package_id})

    # ---- 根必须在包内 ----
    if root not in by_ext:
        raise StoreError(
            "ROOT_NOT_IN_PACKAGE",
            f"转交根 {root} 不在包记录中", status=422,
            details={"root_ext_id": root})

    # ---- 缺失祖先：任何 parent_ext_id 必须落在包内（闭包完整）----
    missing: dict[str, list[str]] = {}
    invalid_edges: list[str] = []
    for r in norm_records:
        miss = [p for p in r["parent_ext_ids"] if p not in by_ext]
        if miss:
            missing[r["ext_id"]] = miss
        # 原始记录不得带依据；推导必须有依据
        if r["kind"] == "raw" and r["parent_ext_ids"]:
            invalid_edges.append(r["ext_id"])
    if missing:
        all_missing = sorted({m for ms in missing.values() for m in ms})
        raise StoreError(
            "MISSING_ANCESTOR",
            f"转交包缺少祖先记录：{', '.join(all_missing)}",
            status=422,
            details={"missing_ext_ids": all_missing,
                     "referenced_by": missing})
    if invalid_edges:
        raise StoreError(
            "INVALID_PACKAGE_SHAPE",
            "原始记录不能携带直接依据", status=422,
            details={"raw_with_parents_ext_ids": sorted(set(invalid_edges))})
    derived_no_parents = [r["ext_id"] for r in norm_records
                          if r["kind"] == "derived"
                          and not r["parent_ext_ids"]]
    if derived_no_parents:
        raise StoreError(
            "INVALID_PACKAGE_SHAPE",
            "推导记录必须至少有一个直接依据", status=422,
            details={"derived_without_parents_ext_ids":
                     sorted(set(derived_no_parents))})

    # 重复父引用
    dup_parents: dict[str, list[str]] = {}
    for r in norm_records:
        cnt = Counter(r["parent_ext_ids"])
        dups = sorted({p for p, c in cnt.items() if c > 1})
        if dups:
            dup_parents[r["ext_id"]] = dups
    if dup_parents:
        raise StoreError(
            "DUPLICATE_PARENT",
            "转交包内存在重复的直接依据引用", status=422,
            details={"duplicate_parents": dup_parents})

    # ---- 成环检测 ----
    cycle = _find_cycle(by_ext)
    if cycle:
        raise StoreError(
            "CYCLE_DETECTED", "转交包依据关系形成环", status=422,
            details={"cycle_ext_ids": cycle})

    # ---- 已失效依据：本包只允许转交仍有效的结论闭包 ----
    # 包本身不带 status（密封的是结论内容），失效状态由“依据有效性”隐式表达：
    # 我们要求包内所有记录都作为有效结论接入；若来源库根已失效则不允许导出
    # （导出端在 store 层拦截），此处保持包为全有效闭包。

    # ---- 闭包完整性：根可达全部记录（防止夹带无关记录）----
    reachable = _ancestor_closure(root, by_ext)
    orphans = sorted(seen - reachable)
    if orphans:
        raise StoreError(
            "UNREACHABLE_RECORDS",
            "包内存在自根不可达的记录", status=422,
            details={"unreachable_ext_ids": orphans})

    package_id = canonical_package_id
    envelope = {
        "format": FORMAT,
        "package_id": package_id,
        "root_ext_id": root,
        "records": norm_records,
        "digest": expected,
    }
    return envelope


def _safe_equal(a: str, b: str) -> bool:
    import hmac
    return hmac.compare_digest(a, b)


def _find_cycle(by_ext: dict[str, dict[str, Any]]) -> list[str]:
    """返回环上的 ext_id（DFS），无环返回空列表。"""
    color: dict[str, int] = {}  # 0=白 1=灰 2=黑
    cycle: list[str] = []

    def dfs(u: str, stack: list[str]) -> bool:
        color[u] = 1
        stack.append(u)
        for p in by_ext[u]["parent_ext_ids"]:
            if color.get(p, 0) == 1:
                # 找到回边，截取环
                idx = stack.index(p)
                cycle.extend(stack[idx:])
                cycle.append(p)
                return True
            if color.get(p, 0) == 0 and dfs(p, stack):
                return True
        stack.pop()
        color[u] = 2
        return False

    for ext in by_ext:
        if color.get(ext, 0) == 0 and dfs(ext, []):
            return cycle
    return []


def _ancestor_closure(root: str,
                      by_ext: dict[str, dict[str, Any]]) -> set[str]:
    seen = {root}
    stack = [root]
    while stack:
        u = stack.pop()
        for p in by_ext[u]["parent_ext_ids"]:
            if p not in seen:
                seen.add(p)
                stack.append(p)
    return seen
