"""密封转交包测试：导出闭包 / 摘要校验 / 接入映射 / 幂等 / 冲突 / 原子拒绝。

使用两套独立谱系（A 外场、B 本地）模拟跨实例转交。
"""

from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import package as pkg
from app.errors import StoreError
from app.store import CalibrationStore


@pytest.fixture()
def stores(tmp_path):
    a = CalibrationStore(str(tmp_path / "lineage_a.db"))
    b = CalibrationStore(str(tmp_path / "lineage_b.db"))
    yield a, b
    a.close()
    b.close()


def _build_lineage_a(store: CalibrationStore):
    """外场谱系：r1 raw, r2 raw, r3 = f(r1,r2), root = f(r3,r1)。"""
    r1 = store.create_record("raw", {"sensor": "77K", "v": 1}, None)
    r2 = store.create_record("raw", {"sensor": "4K", "v": 2}, None)
    r3 = store.create_record("derived", {"formula": "gain", "v": 3},
                             [r1["id"], r2["id"]])
    root = store.create_record("derived", {"formula": "offset", "v": 4},
                               [r3["id"], r1["id"]])
    return r1, r2, r3, root


# --------------------------------------------------------------------------- #
# 导出
# --------------------------------------------------------------------------- #
def test_export_contains_full_ancestor_closure_with_ext_refs(stores):
    a, _ = stores
    r1, r2, r3, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    assert pack["format"] == pkg.FORMAT
    assert pack["root_ext_id"] == root["ext_id"]
    ext_ids = {r["ext_id"] for r in pack["records"]}
    assert ext_ids == {r1["ext_id"], r2["ext_id"], r3["ext_id"], root["ext_id"]}
    # 包内边全部按稳定外部标识引用，绝不出现本地 R 编号
    blob = json.dumps(pack, ensure_ascii=False)
    assert "R000001" not in blob
    root_rec = next(r for r in pack["records"]
                    if r["ext_id"] == root["ext_id"])
    assert set(root_rec["parent_ext_ids"]) == {r3["ext_id"], r1["ext_id"]}
    # 摘要可被独立重算并一致
    assert pkg.digest_for(pack["root_ext_id"],
                          [{k: v for k, v in r.items()}
                           for r in pack["records"]]) == pack["digest"]
    assert pack["package_id"] == pkg.package_id_for(root["ext_id"])


def test_export_invalid_root_rejected(stores):
    a, _ = stores
    r1, r2, r3, root = _build_lineage_a(a)
    a.invalidate("op-kill-r1", r1["id"])  # 级联使 root 失效
    with pytest.raises(StoreError) as ei:
        a.export_package(root["id"])
    assert ei.value.code == "PACKAGE_ROOT_INVALID"


def test_export_missing_root_404(stores):
    a, _ = stores
    with pytest.raises(StoreError) as ei:
        a.export_package("R000999")
    assert ei.value.status == 404


def test_export_is_deterministic(stores):
    a, _ = stores
    _, _, _, root = _build_lineage_a(a)
    p1 = a.export_package(root["id"])
    p2 = a.export_package(root["id"])
    assert p1["digest"] == p2["digest"]
    assert p1["package_id"] == p2["package_id"]


# --------------------------------------------------------------------------- #
# 接入：正常路径
# --------------------------------------------------------------------------- #
def test_import_creates_mapping_and_records_in_one_commit(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])

    before = {r["id"]: r for r in b.list_records()}
    assert before == {}
    res = b.import_package(pack)
    assert res["replayed"] is False
    local_root = b.get_record(res["root_record_id"])
    assert local_root["status"] == "valid"
    # 直接依据映射为本地编号
    assert set(local_root["parent_ids"]) == {
        m["record_id"] for m in res["mapping"]
        if m["ext_id"] in {p for p in
                           next(r for r in pack["records"]
                                if r["ext_id"] == root["ext_id"])
                           ["parent_ext_ids"]}}
    # 全部外部标识映射已建立且指向有效记录
    assert len(res["mapping"]) == 4
    for m in res["mapping"]:
        rec = b.get_record(m["record_id"])
        assert rec["ext_id"] == m["ext_id"]
        assert rec["status"] == "valid"
    b.assert_invariants()


def test_imported_records_have_independent_local_ids_same_ext(stores):
    a, b = stores
    _, _, _, root_a = _build_lineage_a(a)
    res = b.import_package(a.export_package(root_a["id"]))
    # 本地编号独立分配（B 库从 R000001 起），外部标识沿用来源
    local_root = b.get_record(res["root_record_id"])
    assert local_root["id"] == res["root_record_id"]
    assert local_root["ext_id"] == root_a["ext_id"]
    # 可按包标识查询首次接入结果
    again = b.get_import(res["package_id"])
    assert again["root_record_id"] == res["root_record_id"]


def test_import_records_ordered_child_first_still_succeeds(stores):
    """包内数组顺序即使子在前，接入也按拓扑序落库（满足外键）。"""
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    # 完全反序：root 在最前，raw 在最后
    pack["records"] = sorted(pack["records"], key=lambda r: r["ext_id"],
                             reverse=True)
    res = b.import_package(pack)
    local_root = b.get_record(res["root_record_id"])
    assert local_root["status"] == "valid"
    assert len(b.list_records()) == 4
    b.assert_invariants()


# --------------------------------------------------------------------------- #
# 幂等：重复接入相同包 -> 首次映射
# --------------------------------------------------------------------------- #
def test_duplicate_import_identical_returns_first_mapping(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    first = b.import_package(copy.deepcopy(pack))
    second = b.import_package(copy.deepcopy(pack))
    assert second["replayed"] is True
    assert second["root_record_id"] == first["root_record_id"]
    assert second["mapping"] == first["mapping"]
    # 没有产生新记录
    assert len(b.list_records()) == 4
    # 记录数组顺序打乱后仍是同一规范载荷 -> 仍视为重复
    shuffled = copy.deepcopy(pack)
    shuffled["records"] = list(reversed(shuffled["records"]))
    third = b.import_package(shuffled)
    assert third["replayed"] is True
    assert third["root_record_id"] == first["root_record_id"]


# --------------------------------------------------------------------------- #
# 冲突：同一包标识、不同载荷
# --------------------------------------------------------------------------- #
def test_same_package_id_different_payload_conflicts(stores):
    a, b = stores
    _, _, r3, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    first = b.import_package(pack)

    # 篡改某条祖先载荷，并把摘要重算为自洽值（package_id 仍由 root 派生，不变）
    tampered = copy.deepcopy(pack)
    target = next(r for r in tampered["records"]
                  if r["ext_id"] != root["ext_id"])
    target["payload"] = {"sensor": "TAMPERED", "v": 999}
    tampered["digest"] = pkg.digest_for(tampered["root_ext_id"],
                                        tampered["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(tampered)
    assert ei.value.status == 409
    assert ei.value.code == "PACKAGE_CONFLICT"
    assert ei.value.details["original_digest"] == first["digest"]
    # 本地既有结论未改变：仍为首次映射的 4 条
    assert len(b.list_records()) == 4
    b.assert_invariants()


def test_tampered_digest_without_rehash_is_rejected(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    b.import_package(copy.deepcopy(pack))
    tampered = copy.deepcopy(pack)
    tampered["records"][0]["payload"]["v"] = 123456
    # 不重算 digest -> 摘要不符
    with pytest.raises(StoreError) as ei:
        b.import_package(tampered)
    assert ei.value.code == "DIGEST_MISMATCH"
    assert len(b.list_records()) == 4


# --------------------------------------------------------------------------- #
# 各类可定位拒绝（且本地既有结论不变）
# --------------------------------------------------------------------------- #
def test_missing_ancestor_rejected(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    # 删掉一个祖先，但保留对它的引用
    removed = pack["records"][1]["ext_id"]
    pack["records"] = [r for r in pack["records"]
                       if r["ext_id"] != removed]
    pack["digest"] = pkg.digest_for(pack["root_ext_id"], pack["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(pack)
    assert ei.value.code == "MISSING_ANCESTOR"
    assert removed in ei.value.details["missing_ext_ids"]
    assert b.list_records() == []


def test_duplicate_ext_id_in_package_rejected(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])
    pack["records"][1]["ext_id"] = pack["records"][0]["ext_id"]
    pack["digest"] = pkg.digest_for(pack["root_ext_id"], pack["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(pack)
    assert ei.value.code == "DUPLICATE_EXT_ID"
    assert b.list_records() == []


def test_cycle_package_rejected(stores):
    a, b = stores
    r1 = a.create_record("raw", {"v": 1}, None)
    r2 = a.create_record("derived", {"v": 2}, [r1["id"]])
    # 手工构造一个成环包（绕过导出端）
    pack = {
        "format": pkg.FORMAT,
        "package_id": pkg.package_id_for("x"),
        "root_ext_id": "x",
        "records": [
            {"ext_id": "x", "kind": "derived", "payload": {},
             "parent_ext_ids": ["y"]},
            {"ext_id": "y", "kind": "derived", "payload": {},
             "parent_ext_ids": ["x"]},
        ],
    }
    pack["digest"] = pkg.digest_for("x", pack["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(pack)
    assert ei.value.code == "CYCLE_DETECTED"
    assert set(ei.value.details["cycle_ext_ids"]) >= {"x", "y"}
    assert b.list_records() == []


def test_unreachable_records_rejected(stores):
    _, b = stores
    pack = {
        "format": pkg.FORMAT,
        "root_ext_id": "x",
        "records": [
            {"ext_id": "x", "kind": "raw", "payload": {},
             "parent_ext_ids": []},
            {"ext_id": "z", "kind": "raw", "payload": {},
             "parent_ext_ids": []},
        ],
    }
    pack["package_id"] = pkg.package_id_for("x")
    pack["digest"] = pkg.digest_for("x", pack["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(pack)
    assert ei.value.code == "UNREACHABLE_RECORDS"
    assert ei.value.details["unreachable_ext_ids"] == ["z"]


def test_invalid_format_rejected(stores):
    _, b = stores
    with pytest.raises(StoreError) as ei:
        b.import_package({"format": "nope", "records": []})
    assert ei.value.status == 400
    assert ei.value.code == "UNSUPPORTED_PACKAGE_FORMAT"


def test_package_id_must_match_root(stores):
    _, b = stores
    pack = {
        "format": pkg.FORMAT,
        "root_ext_id": "x",
        "package_id": "H-forged",
        "records": [{"ext_id": "x", "kind": "raw", "payload": {},
                     "parent_ext_ids": []}],
    }
    pack["digest"] = pkg.digest_for("x", pack["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(pack)
    assert ei.value.code == "PACKAGE_ID_MISMATCH"


# --------------------------------------------------------------------------- #
# 共享祖先复用 + 失效依据拒绝
# --------------------------------------------------------------------------- #
def test_import_reuses_shared_valid_ancestor(stores):
    a, b = stores
    r1, r2, r3, root = _build_lineage_a(a)
    # 先只接入一个以 r1,r2 为依据的子结论 r3
    pack3 = a.export_package(r3["id"])
    res3 = b.import_package(pack3)
    assert len(res3["mapping"]) == 3
    # 再接入更大的 root 闭包：r1/r2/r3 应复用既有映射，只新增 root
    pack_root = a.export_package(root["id"])
    res_root = b.import_package(pack_root)
    assert len(b.list_records()) == 4
    assert set(res_root["reused_ext_ids"]) == {r1["ext_id"], r2["ext_id"],
                                               r3["ext_id"]}
    # root 的直接依据中 r3 映射到首次接入的本地编号
    r3_local = res3["root_record_id"]
    assert r3_local in res_root["direct_parent_ids"]


def test_same_ext_id_different_content_conflicts(stores):
    a, b = stores
    r1, r2, r3, _ = _build_lineage_a(a)
    b.import_package(a.export_package(r3["id"]))  # 接入含 r1/r2/r3
    # 伪造另一个包：复用 r1 的 ext_id 但内容不同，根为新结论
    other = {
        "format": pkg.FORMAT,
        "root_ext_id": "new-root",
        "records": [
            {"ext_id": r1["ext_id"], "kind": "raw",
             "payload": {"sensor": "DIFFERENT"}, "parent_ext_ids": []},
            {"ext_id": "new-root", "kind": "derived",
             "payload": {"v": 9}, "parent_ext_ids": [r1["ext_id"]]},
        ],
    }
    other["package_id"] = pkg.package_id_for("new-root")
    other["digest"] = pkg.digest_for("new-root", other["records"])
    with pytest.raises(StoreError) as ei:
        b.import_package(other)
    assert ei.value.status == 409
    assert ei.value.code == "EXT_CONTENT_CONFLICT"
    assert ei.value.details["conflicts"][0]["ext_id"] == r1["ext_id"]
    # 本地仍是最初 3 条
    assert len(b.list_records()) == 3


def test_import_refuses_when_shared_ancestor_locally_invalid(stores):
    a, b = stores
    r1, r2, r3, root = _build_lineage_a(a)
    res3 = b.import_package(a.export_package(r3["id"]))
    # 在本地把已接入的 r3 失效
    b.invalidate("op-local-kill", res3["root_record_id"])
    # 再接入引用 r3 的更大闭包 -> 已失效依据，定位拒绝
    with pytest.raises(StoreError) as ei:
        b.import_package(a.export_package(root["id"]))
    assert ei.value.code == "PARENT_INVALID"
    bad = ei.value.details["invalid_basis"]
    assert any(x["ext_id"] == r3["ext_id"] for x in bad)
    # 只有最初的 3 条，未新增 root
    assert len(b.list_records()) == 3


# --------------------------------------------------------------------------- #
# 接入后失效仍级联（保持原有裁决行为）
# --------------------------------------------------------------------------- #
def test_cascade_invalidation_still_works_after_import(stores):
    a, b = stores
    r1, r2, r3, root = _build_lineage_a(a)
    res = b.import_package(a.export_package(root["id"]))
    mapping = {m["ext_id"]: m["record_id"] for m in res["mapping"]}
    inv = b.invalidate("op-after-import", mapping[r1["ext_id"]])
    cascaded = {c["id"] for c in inv["cascade"]}
    # r1 -> r3 -> root 全失效，r2 不受影响
    assert cascaded == {mapping[r1["ext_id"]], mapping[r3["ext_id"]],
                        mapping[root["ext_id"]]}
    assert b.get_record(mapping[r2["ext_id"]])["status"] == "valid"
    b.assert_invariants()


# --------------------------------------------------------------------------- #
# 并发：接入与失效竞争后，无有效记录指向失效/不存在依据
# --------------------------------------------------------------------------- #
def test_concurrent_import_vs_invalidate_keeps_invariant(stores):
    a, b = stores
    packs = []
    for i in range(8):
        x = a.create_record("raw", {"v": i}, None,
                            ext_id=f"raw-{i}")
        d = a.create_record("derived", {"v": i}, [x["id"]],
                            ext_id=f"derived-{i}")
        packs.append(a.export_package(d["id"]))

    errors: list[Exception] = []

    def worker(i: int):
        try:
            if i % 2 == 0:
                b.import_package(copy.deepcopy(packs[i // 2 % len(packs)]))
            else:
                # 失效此前可能接入的某条
                rec = b.list_records()
                if rec:
                    target = rec[i % len(rec)]["id"]
                    b.invalidate(f"op-inv-{i}", target)
        except StoreError:
            pass
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(worker, range(48)))
    assert not errors
    b.assert_invariants()


def test_concurrent_duplicate_import_single_mapping(stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    pack = a.export_package(root["id"])

    def invoke():
        try:
            return b.import_package(copy.deepcopy(pack))
        except StoreError as e:
            return e

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: invoke(), range(24)))
    ok = [r for r in results if not isinstance(r, StoreError)]
    assert ok
    first_root = ok[0]["root_record_id"]
    assert all(r["root_record_id"] == first_root for r in ok)
    assert len(b.list_records()) == 4
    b.assert_invariants()


# --------------------------------------------------------------------------- #
# 重启持久化
# --------------------------------------------------------------------------- #
def test_import_mapping_persists_after_reopen(tmp_path, stores):
    a, b = stores
    _, _, _, root = _build_lineage_a(a)
    res = b.import_package(a.export_package(root["id"]))
    b.close()
    b2 = CalibrationStore(str(tmp_path / "lineage_b.db"))
    again = b2.import_package(a.export_package(root["id"]))
    assert again["replayed"] is True
    assert again["root_record_id"] == res["root_record_id"]
    assert b2.get_import(res["package_id"])["mapping"] == res["mapping"]
    b2.close()
