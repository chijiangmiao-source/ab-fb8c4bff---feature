"""密封转交包：导出闭包、稳定外部标识、原子接入、幂等/冲突、篡改与失效依据。"""

from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import StoreError
from app.packages import (PACKAGE_FORMAT, canonical_json, envelope_payload_digest,
                          record_external_id, validate_envelope)
from app.store import CalibrationStore


@pytest.fixture()
def store(tmp_path):
    s = CalibrationStore(str(tmp_path / "t.db"))
    yield s
    s.close()


def _build_lineage(store: CalibrationStore):
    """r1(raw) <- r2 <- r3（r3 直接依据 r2、r1）；另 r4(raw) <- r5。"""
    r1 = store.create_record("raw", {"value": "raw-1", "unit": "mV"}, None)
    r2 = store.create_record("derived", {"value": "derived-2"}, [r1["id"]])
    r3 = store.create_record("derived", {"value": "derived-3"},
                             [r2["id"], r1["id"]])
    r4 = store.create_record("raw", {"value": "raw-4"}, None)
    r5 = store.create_record("derived", {"value": "derived-5"}, [r4["id"]])
    return r1, r2, r3, r4, r5


# --------------------------------------------------------------------------- #
# 规范载荷与稳定外部标识
# --------------------------------------------------------------------------- #
def test_canonical_json_is_order_independent():
    a = canonical_json({"z": 1, "a": [1, {"y": 2, "x": 3}]})
    b = canonical_json({"a": [1, {"x": 3, "y": 2}], "z": 1})
    assert a == b


def test_external_id_is_stable_across_independent_lineages(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    sb = CalibrationStore(str(tmp_path / "b.db"))
    _build_lineage(sa)
    _build_lineage(sb)  # 本地编号相同但这不是关键；内容寻址才是
    ea = sa.export_package("R000003")["package"]
    eb = sb.export_package("R000003")["package"]
    # 两套独立谱系对同一结论密封出的包逐字节一致
    assert json.dumps(ea, sort_keys=True) == json.dumps(eb, sort_keys=True)
    assert ea["format"] == PACKAGE_FORMAT
    assert ea["root_external_id"].startswith("X")
    assert len(ea["records"]) == 3
    sa.close()
    sb.close()


def test_external_id_changes_when_content_or_basis_changes(store):
    r1 = store.create_record("raw", {"value": "a"}, None)
    r2 = store.create_record("derived", {"value": "c"}, [r1["id"]])
    e = store.export_package(r2["id"])["package"]
    # 依据内容不同 -> 外部标识不同
    other = store.create_record("raw", {"value": "a2"}, None)
    r2b = store.create_record("derived", {"value": "c"}, [other["id"]])
    e2 = store.export_package(r2b["id"])["package"]
    assert e2["root_external_id"] != e["root_external_id"]
    # 直接依据边序影响标识（依据集合相同但次序不同是不同的规范声明）
    assert record_external_id("derived", {"v": 1}, ["Xa", "Xb"]) != \
        record_external_id("derived", {"v": 1}, ["Xb", "Xa"])


def test_export_contains_full_basis_closure(store):
    r1, r2, r3, r4, r5 = _build_lineage(store)
    env = store.export_package(r3["id"])["package"]
    exts = {r["external_id"] for r in env["records"]}
    assert env["root_external_id"] in exts and len(exts) == 3
    # 包内引用全部解析到包内记录（无外部悬挂）
    for rec in env["records"]:
        for p in rec["parent_external_ids"]:
            assert p in exts


def test_export_invalid_root_rejected(store):
    r1, r2, r3, r4, r5 = _build_lineage(store)
    store.invalidate("op-kill", r1["id"])
    with pytest.raises(StoreError) as ei:
        store.export_package(r3["id"])
    assert ei.value.code == "PACKAGE_ROOT_NOT_VALID"
    assert ei.value.status == 409


def test_export_missing_root_404(store):
    with pytest.raises(StoreError) as ei:
        store.export_package("R000999")
    assert ei.value.status == 404


# --------------------------------------------------------------------------- #
# 接入：映射、幂等、冲突
# --------------------------------------------------------------------------- #
def test_import_assigns_local_ids_and_preserves_direct_basis(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    # 本地已有一条原始结论：接入不得改变既有编号
    pre = sb.create_record("raw", {"value": "local-pre"}, None)
    assert pre["id"] == "R000001"
    imp = sb.import_package(env)
    assert imp["replayed"] is False
    assert imp["record_count"] == 3
    assert imp["imported_count"] == 3
    # 根是最后分配的编号
    assert imp["root_record_id"] == "R000004"
    root = sb.get_record(imp["root_record_id"])
    assert root["status"] == "valid"
    # 直接依据指向本地新记录而非来源实例编号
    assert root["parent_ids"] == ["R000003", "R000002"]
    # 既有结论不变
    assert sb.get_record("R000001")["payload"] == {"value": "local-pre"}
    sb.assert_invariants()
    sb.close()


def test_duplicate_import_returns_first_mapping(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    first = sb.import_package(env)
    n_records = len(sb.list_records())
    second = sb.import_package(copy.deepcopy(env))
    third = sb.import_package(copy.deepcopy(env))
    assert second["replayed"] is True and third["replayed"] is True
    # 返回首次映射：本地编号完全一致，未新增记录
    assert second["root_record_id"] == first["root_record_id"]
    assert third["mapping"] == first["mapping"]
    assert len(sb.list_records()) == n_records
    # 可经包标识查询首次映射
    got = sb.get_package(env["package_id"])
    assert got["root_record_id"] == first["root_record_id"]
    sb.close()


def test_same_digest_different_package_id_still_replays_first_mapping(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sa.close()
    sb = CalibrationStore(str(tmp_path / "b.db"))
    first = sb.import_package(env)
    relabeled = copy.deepcopy(env)
    relabeled["package_id"] = "PKG-relabeled-claim"
    again = sb.import_package(relabeled)
    assert again["replayed"] is True
    assert again["root_record_id"] == first["root_record_id"]
    assert len(sb.list_records()) == 3
    sb.close()


def test_same_package_id_different_payload_conflicts_atomically(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    # 另一份内容完全不同的诚实包，然后冒用第一份的包标识
    other_root = sa.create_record("raw", {"value": "unrelated"}, None)
    other = sa.export_package(other_root["id"])["package"]
    other["package_id"] = env["package_id"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    sb.import_package(env)
    before = {r["id"]: r for r in sb.list_records()}
    with pytest.raises(StoreError) as ei:
        sb.import_package(other)
    assert ei.value.code == "PACKAGE_CONFLICT"
    assert ei.value.status == 409
    assert ei.value.details["existing_digest"] == env["payload_digest"]
    assert ei.value.details["incoming_digest"] == other["payload_digest"]
    # 本地既有结论未被改变
    after = {r["id"]: r for r in sb.list_records()}
    assert set(before) == set(after)
    for rid in before:
        assert before[rid]["payload"] == after[rid]["payload"]
    # 冲突标识下仍可重放原始包
    assert sb.import_package(copy.deepcopy(env))["replayed"] is True
    sb.close()


# --------------------------------------------------------------------------- #
# 篡改与畸形包：原子拒绝，本地状态不变
# --------------------------------------------------------------------------- #
def _fresh_pair(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sb = CalibrationStore(str(tmp_path / "b.db"))
    sb.create_record("raw", {"value": "existing"}, None)
    return sa, sb, env


def test_digest_tamper_rejected_atomically(tmp_path):
    sa, sb, env = _fresh_pair(tmp_path)
    bad = copy.deepcopy(env)
    bad["payload_digest"] = "sha256:" + "0" * 64
    with pytest.raises(StoreError) as ei:
        sb.import_package(bad)
    assert ei.value.code == "PACKAGE_DIGEST_MISMATCH"
    assert len(sb.list_records()) == 1  # 既有记录不变
    sa.close(); sb.close()


def test_payload_tamper_fails_external_id_check(tmp_path):
    sa, sb, env = _fresh_pair(tmp_path)
    bad = copy.deepcopy(env)
    bad["records"][0]["payload"] = {"value": "HACKED"}
    with pytest.raises(StoreError) as ei:
        sb.import_package(bad)
    assert ei.value.code == "PACKAGE_EXTERNAL_ID_MISMATCH"
    assert ei.value.details["mismatched_records"][0]["expected"].startswith("X")
    assert len(sb.list_records()) == 1
    sa.close(); sb.close()


def test_missing_ancestor_rejected_with_location(tmp_path):
    sa, sb, env = _fresh_pair(tmp_path)
    root = env["root_external_id"]
    ancestor = next(r["external_id"] for r in env["records"]
                    if not r["parent_external_ids"]
                    and r["external_id"] != root)
    thinned = copy.deepcopy(env)
    thinned["records"] = [r for r in thinned["records"]
                          if r["external_id"] != ancestor]
    with pytest.raises(StoreError) as ei:
        sb.import_package(thinned)
    assert ei.value.code == "PACKAGE_MISSING_ANCESTOR"
    assert ancestor in ei.value.details["missing_external_ids"]
    assert ancestor in ei.value.details["referenced_by"]
    assert len(sb.list_records()) == 1
    sa.close(); sb.close()


def test_duplicate_external_id_rejected(tmp_path):
    sa, sb, env = _fresh_pair(tmp_path)
    dup = copy.deepcopy(env)
    dup["records"].append(copy.deepcopy(dup["records"][0]))
    with pytest.raises(StoreError) as ei:
        sb.import_package(dup)
    assert ei.value.code == "PACKAGE_DUPLICATE_EXTERNAL_ID"
    sa.close(); sb.close()


def test_cycle_detected_and_located():
    recs = {
        "Xa": {"kind": "derived", "payload": {},
               "parent_external_ids": ["Xb"]},
        "Xb": {"kind": "derived", "payload": {},
               "parent_external_ids": ["Xa"]},
    }
    digest = envelope_payload_digest("Xa", recs)
    env = {"format": PACKAGE_FORMAT, "package_id": "PKG-cyc",
           "root_external_id": "Xa", "payload_digest": digest,
           "records": [
               {"external_id": "Xa", **recs["Xa"]},
               {"external_id": "Xb", **recs["Xb"]},
           ]}
    with pytest.raises(StoreError) as ei:
        validate_envelope(env)
    assert ei.value.code == "PACKAGE_CYCLE_DETECTED"
    cyc = ei.value.details["cycle_external_ids"]
    assert cyc[0] == cyc[-1] and set(cyc) == {"Xa", "Xb"}


def test_disconnected_record_rejected(tmp_path):
    sa, sb, env = _fresh_pair(tmp_path)
    # 加入一条自成一体但与根闭包无关的记录
    extra_ext = record_external_id("raw", {"value": "lonely"}, [])
    env2 = copy.deepcopy(env)
    env2["records"].append({"external_id": extra_ext, "kind": "raw",
                            "payload": {"value": "lonely"},
                            "parent_external_ids": []})
    with pytest.raises(StoreError) as ei:
        sb.import_package(env2)
    assert ei.value.code in ("PACKAGE_DIGEST_MISMATCH",
                             "PACKAGE_DISCONNECTED_RECORD")
    sa.close(); sb.close()


def test_malformed_envelope_rejected(tmp_path):
    sa = CalibrationStore(str(tmp_path / "x.db"))
    for bad, code in [
        ({"format": "nope"}, "PACKAGE_MALFORMED"),
        ("not-a-dict", "PACKAGE_MALFORMED"),
    ]:
        with pytest.raises(StoreError) as ei:
            sa.import_package(bad)  # type: ignore[arg-type]
        assert ei.value.code == code
    sa.close()


# --------------------------------------------------------------------------- #
# 已失效依据 / 共享祖先 / 级联失效
# --------------------------------------------------------------------------- #
def test_import_rejected_when_shared_basis_already_invalid(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    r1, r2, r3, r4, r5 = _build_lineage(sa)
    root_pkg = sa.export_package(r3["id"])["package"]
    raw_pkg = sa.export_package(r1["id"])["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    one = sb.import_package(raw_pkg)
    sb.invalidate("op-invalid-basis", one["root_record_id"])
    n = len(sb.list_records())
    with pytest.raises(StoreError) as ei:
        sb.import_package(root_pkg)
    assert ei.value.code == "PACKAGE_BASIS_INVALID"
    detail = ei.value.details["invalid_basis"][0]
    assert detail["record_id"] == one["root_record_id"]
    # 原子拒绝：没有新记录、没有悬挂有效结论
    assert len(sb.list_records()) == n
    sb.assert_invariants()
    sb.close()


def test_shared_valid_basis_is_reused_and_cascade_still_propagates(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    r1, r2, r3, r4, r5 = _build_lineage(sa)
    raw_pkg = sa.export_package(r1["id"])["package"]
    root_pkg = sa.export_package(r3["id"])["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    one = sb.import_package(raw_pkg)
    two = sb.import_package(root_pkg)
    # 同一外部依据复用首次映射
    assert two["reused_count"] == 1 and two["imported_count"] == 2
    reused = [m for m in two["mapping"] if m["reused"]]
    assert reused[0]["record_id"] == one["root_record_id"]
    # 根的直接依据之一即该共享本地记录
    parents = sb.get_record(two["root_record_id"])["parent_ids"]
    assert one["root_record_id"] in parents

    # 既有级联失效行为：失效共享祖先，导入的下游结论一并失效
    res = sb.invalidate("op-cascade", one["root_record_id"])
    cascaded = {c["id"] for c in res["cascade"]}
    assert two["root_record_id"] in cascaded
    assert sb.get_record(two["root_record_id"])["status"] == "invalid"
    sb.assert_invariants()
    sb.close()


# --------------------------------------------------------------------------- #
# 并发接入与持久化
# --------------------------------------------------------------------------- #
def test_concurrent_identical_imports_yield_single_mapping(tmp_path):
    sa = CalibrationStore(str(tmp_path / "c.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    results: list = []

    def worker(_i: int):
        try:
            results.append(sb.import_package(copy.deepcopy(env)))
        except StoreError as e:
            results.append(e)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(worker, range(32)))
    roots = {r["root_record_id"] for r in results
             if not isinstance(r, StoreError)}
    assert roots == {results[0]["root_record_id"]}
    assert len(sb.list_records()) == 3  # 只落一份
    sb.assert_invariants()
    sb.close()


def test_concurrent_import_and_invalidation_never_orphans(tmp_path):
    """并发接入与失效裁决竞争后，不存在有效记录指向不存在或失效依据。"""
    sa = CalibrationStore(str(tmp_path / "a.db"))
    r1 = sa.create_record("raw", {"value": "pivot"}, None)
    d1 = sa.create_record("derived", {"value": "d1"}, [r1["id"]])
    pkg = sa.export_package(d1["id"])["package"]
    sa.close()

    sb = CalibrationStore(str(tmp_path / "b.db"))
    # 先接入一次建立共享依据
    first = sb.import_package(pkg)
    basis_local = first["mapping"][0]["record_id"]  # 祖先 raw 的本地编号
    errors: list[Exception] = []

    def worker(i: int):
        try:
            if i % 2 == 0:
                sb.invalidate(f"op-inv-{i}", basis_local)
            else:
                sb.import_package(copy.deepcopy(pkg))
        except StoreError:
            pass  # 失效后再接入被拒是合法结局
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(worker, range(40)))
    assert not errors
    sb.assert_invariants()
    sb.close()


def test_mapping_persists_after_reopen(tmp_path):
    sa = CalibrationStore(str(tmp_path / "a.db"))
    _build_lineage(sa)
    env = sa.export_package("R000003")["package"]
    sa.close()

    path = str(tmp_path / "b.db")
    sb = CalibrationStore(path)
    first = sb.import_package(env)
    sb.close()

    reopened = CalibrationStore(path)
    again = reopened.import_package(copy.deepcopy(env))
    assert again["replayed"] is True
    assert again["root_record_id"] == first["root_record_id"]
    got = reopened.get_package(env["package_id"])
    assert got["payload_digest"] == env["payload_digest"]
    reopened.assert_invariants()
    reopened.close()
