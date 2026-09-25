"""密封转交包：规范载荷、稳定外部标识、摘要与校验（纯函数，不触碰数据库）。

设计要点
--------

1. 包内记录按**稳定外部标识**互相引用，与本地编号（R000001…）无关；
2. 外部标识采用 Merkle 内容寻址：记录的外部标识由其类型、规范载荷与其
   **直接依据的外部标识（按边序）** 决定，因此两套独立谱系中内容与依据
   相同的记录必然得到相同的外部标识，且标识自证身份（无法只篡改载荷）；
3. 包摘要对「根结论 + 直接依据闭包中的全部记录」做规范序列化后取 SHA-256；
4. 任何缺失祖先、重复外部标识、环、外部标识不符、摘要不符都在此处定位拒绝。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .errors import StoreError

PACKAGE_FORMAT = "calibration-package/v1"
_DIGEST_PREFIX = "sha256:"


def canonical_json(obj: Any) -> str:
    """跨实例一致的规范 JSON：键排序、紧凑分隔、UTF-8 原文、不转义非 ASCII。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def record_external_id(kind: str, payload: dict[str, Any],
                       parent_external_ids: list[str]) -> str:
    """Merkle 外部标识：X + sha256(类型 + 规范载荷 + 直接依据外部标识序列)。"""
    material = canonical_json({
        "b": list(parent_external_ids),
        "k": kind,
        "p": payload,
    })
    return "X" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def envelope_payload_digest(
        root_external_id: str,
        records: dict[str, dict[str, Any]]) -> str:
    """包摘要：对根标识与闭包内全部记录（按外部标识排序）规范序列化后取摘要。"""
    ordered = [{
        "b": list(records[e]["parent_external_ids"]),
        "e": e,
        "k": records[e]["kind"],
        "p": records[e]["payload"],
    } for e in sorted(records)]
    material = canonical_json({
        "f": PACKAGE_FORMAT,
        "records": ordered,
        "root": root_external_id,
    })
    return _DIGEST_PREFIX + hashlib.sha256(
        material.encode("utf-8")).hexdigest()


def package_id_for_digest(digest: str) -> str:
    """包标识由包摘要决定：同内容同标识（重复接入据此回到首次映射）。"""
    return "PKG-" + digest[len(_DIGEST_PREFIX):][:24]


def make_envelope(root_external_id: str,
                  records: list[dict[str, Any]]) -> dict[str, Any]:
    """根据根标识与闭包记录构造密封转交包（导出侧使用）。"""
    by_ext = {r["external_id"]: r for r in records}
    digest = envelope_payload_digest(root_external_id, by_ext)
    return {
        "format": PACKAGE_FORMAT,
        "package_id": package_id_for_digest(digest),
        "root_external_id": root_external_id,
        "payload_digest": digest,
        "records": [
            {
                "external_id": e,
                "kind": by_ext[e]["kind"],
                "payload": by_ext[e]["payload"],
                "parent_external_ids":
                    list(by_ext[e]["parent_external_ids"]),
            }
            for e in sorted(by_ext)
        ],
    }


def _malformed(field: str, reason: str) -> StoreError:
    return StoreError(
        "PACKAGE_MALFORMED",
        f"转交包格式不合法：{reason}", status=400,
        details={"field": field, "reason": reason})


def validate_envelope(
        envelope: Any
) -> tuple[str, str, str, dict[str, dict[str, Any]]]:
    """校验密封转交包，返回 (package_id, digest, root_ext, records)。

    records: external_id -> {"kind", "payload", "parent_external_ids"}。
    任何问题都抛携带定位信息的 StoreError；本函数只读，不改变任何状态。
    """
    if not isinstance(envelope, dict):
        raise _malformed("envelope", "包必须是 JSON 对象")
    if envelope.get("format") != PACKAGE_FORMAT:
        raise _malformed(
            "format", f"format 必须为 {PACKAGE_FORMAT}")
    package_id = envelope.get("package_id")
    if not isinstance(package_id, str) or not package_id:
        raise _malformed("package_id", "package_id 必须为非空字符串")
    root_ext = envelope.get("root_external_id")
    if not isinstance(root_ext, str) or not root_ext:
        raise _malformed("root_external_id",
                         "root_external_id 必须为非空字符串")
    digest = envelope.get("payload_digest")
    if not isinstance(digest, str) or not digest.startswith(_DIGEST_PREFIX):
        raise _malformed("payload_digest",
                         "payload_digest 必须为 sha256: 前缀的摘要字符串")
    raw_records = envelope.get("records")
    if not isinstance(raw_records, list) or not raw_records:
        raise _malformed("records", "records 必须为非空数组")

    records: dict[str, dict[str, Any]] = {}
    duplicates: list[str] = []
    for index, item in enumerate(raw_records):
        if not isinstance(item, dict):
            raise _malformed(f"records[{index}]", "每条记录必须是对象")
        ext = item.get("external_id")
        if not isinstance(ext, str) or not ext:
            raise _malformed(f"records[{index}].external_id",
                             "external_id 必须为非空字符串")
        kind = item.get("kind")
        if kind not in ("raw", "derived"):
            raise _malformed(f"records[{ext}].kind",
                             "kind 必须为 raw 或 derived")
        payload = item.get("payload")
        if not isinstance(payload, dict):
            raise _malformed(f"records[{ext}].payload",
                             "payload 必须为对象")
        parents = item.get("parent_external_ids")
        if not isinstance(parents, list) or not all(
                isinstance(p, str) and p for p in parents):
            raise _malformed(f"records[{ext}].parent_external_ids",
                             "parent_external_ids 必须为外部标识字符串数组")
        if ext in records:
            duplicates.append(ext)
        records[ext] = {
            "kind": kind,
            "payload": payload,
            "parent_external_ids": parents,
        }

    if duplicates:
        raise StoreError(
            "PACKAGE_DUPLICATE_EXTERNAL_ID",
            f"包内外部标识重复：{', '.join(sorted(set(duplicates)))}",
            details={"duplicate_external_ids": sorted(set(duplicates))})

    # 单条记录内部重复引用同一依据
    for ext in sorted(records):
        parents = records[ext]["parent_external_ids"]
        dup_parents = sorted({p for p in parents if parents.count(p) > 1})
        if dup_parents:
            raise StoreError(
                "PACKAGE_DUPLICATE_EXTERNAL_ID",
                f"记录 {ext} 的直接依据中外部标识重复",
                details={"record_external_id": ext,
                         "duplicate_parent_ids": dup_parents})

    # 类型与依据数量
    for ext in sorted(records):
        rec = records[ext]
        if rec["kind"] == "raw" and rec["parent_external_ids"]:
            raise StoreError(
                "PACKAGE_INVALID_RECORD",
                f"原始记录 {ext} 不能携带直接依据",
                details={"record_external_id": ext,
                         "parent_external_ids": rec["parent_external_ids"]})
        if rec["kind"] == "derived" and not rec["parent_external_ids"]:
            raise StoreError(
                "PACKAGE_INVALID_RECORD",
                f"推导记录 {ext} 必须至少有一条直接依据",
                details={"record_external_id": ext})

    # 缺失祖先：所有父标识必须在包内
    missing: set[str] = set()
    referenced_by: dict[str, list[str]] = {}
    for ext in sorted(records):
        for parent in records[ext]["parent_external_ids"]:
            if parent not in records:
                missing.add(parent)
                referenced_by.setdefault(parent, []).append(ext)
    if missing:
        raise StoreError(
            "PACKAGE_MISSING_ANCESTOR",
            "包内记录引用了未包含在包中的祖先外部标识："
            + ", ".join(sorted(missing)),
            details={"missing_external_ids": sorted(missing),
                     "referenced_by": {
                         m: sorted(set(referenced_by[m]))
                         for m in sorted(missing)}})

    if root_ext not in records:
        raise StoreError(
            "PACKAGE_ROOT_MISSING",
            f"根结论 {root_ext} 未包含在包记录中",
            details={"root_external_id": root_ext})

    # 环检测（沿 child -> parent 方向），同时给出环上定位
    cycle = _find_cycle(records)
    if cycle is not None:
        raise StoreError(
            "PACKAGE_CYCLE_DETECTED",
            "包内依据关系形成环：" + " -> ".join(cycle),
            details={"cycle_external_ids": cycle})

    # 闭包完整性：自根沿直接依据必须能到达包内每一条记录
    reachable: set[str] = {root_ext}
    frontier = [root_ext]
    while frontier:
        current = frontier.pop()
        for parent in records[current]["parent_external_ids"]:
            if parent not in reachable:
                reachable.add(parent)
                frontier.append(parent)
    unreachable = sorted(set(records) - reachable)
    if unreachable:
        raise StoreError(
            "PACKAGE_DISCONNECTED_RECORD",
            "包内存在不属于根结论依据闭包的记录："
            + ", ".join(unreachable),
            details={"root_external_id": root_ext,
                     "unreachable_external_ids": unreachable})

    # 外部标识自证：逐条重算 Merkle 标识
    mismatched: list[dict[str, str]] = []
    for ext in sorted(records):
        rec = records[ext]
        expected = record_external_id(
            rec["kind"], rec["payload"], rec["parent_external_ids"])
        if expected != ext:
            mismatched.append({"external_id": ext, "expected": expected})
    if mismatched:
        raise StoreError(
            "PACKAGE_EXTERNAL_ID_MISMATCH",
            "记录载荷与其外部标识不符（Merkle 摘要校验失败），"
            "共 " + str(len(mismatched)) + " 条",
            details={"mismatched_records": mismatched})

    # 整包摘要
    expected_digest = envelope_payload_digest(root_ext, records)
    if expected_digest != digest:
        raise StoreError(
            "PACKAGE_DIGEST_MISMATCH",
            "包摘要与规范载荷不符，包可能已被篡改",
            details={"package_id": package_id,
                     "expected_digest": expected_digest,
                     "actual_digest": digest})

    # 注：package_id 是身份声明（类似操作标识），其与既有包的冲突
    # （同标识、不同摘要）在接入事务内判 PACKAGE_CONFLICT，此处不强制
    # package_id 必须由摘要派生，以便诚实地重封一份不同内容但冒用既有
    # 包标识的包能走到冲突分支，而不是与单纯篡改摘要相混淆。

    return package_id, digest, root_ext, records


def _find_cycle(
        records: dict[str, dict[str, Any]]) -> list[str] | None:
    """返回环上的一条外部标识路径（首尾相同），无环返回 None。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = dict.fromkeys(records, WHITE)
    stack: list[str] = []

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        stack.append(node)
        for parent in records[node]["parent_external_ids"]:
            if color[parent] == GRAY:
                start = stack.index(parent)
                return stack[start:] + [parent]
            if color[parent] == WHITE:
                found = dfs(parent)
                if found is not None:
                    return found
        color[node] = BLACK
        stack.pop()
        return None

    for node in sorted(records):
        if color[node] == WHITE:
            found = dfs(node)
            if found is not None:
                return found
    return None
