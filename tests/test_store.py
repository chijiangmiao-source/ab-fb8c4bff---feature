"""CalibrationStore 单元测试：级联失效 / 幂等裁决 / 引用校验 / 并发 / 重启。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.store import CalibrationStore, StoreError


@pytest.fixture()
def store(tmp_path):
    db = str(tmp_path / "test.db")
    s = CalibrationStore(db)
    yield s
    s.close()


def make_chain(store: CalibrationStore):
    """R1(raw) <- R2 <- R3, 另建 R4(raw) <- R5(derived)。"""
    r1 = store.create_record("raw", {"value": "raw-1"}, None)
    r2 = store.create_record("derived", {"value": "derived-2"}, [r1["id"]])
    r3 = store.create_record("derived", {"value": "derived-3"},
                             [r2["id"], r1["id"]])
    r4 = store.create_record("raw", {"value": "raw-4"}, None)
    r5 = store.create_record("derived", {"value": "derived-5"}, [r4["id"]])
    return [r["id"] for r in (r1, r2, r3, r4, r5)]


def test_stable_ids_and_direct_basis(store):
    ids = make_chain(store)
    assert ids == ["R000001", "R000002", "R000003", "R000004", "R000005"]
    r3 = store.get_record("R000003")
    assert r3["status"] == "valid"
    assert r3["parent_ids"] == ["R000002", "R000001"]
    assert r3["invalidated_by"] is None


def test_cascade_invalidation_single_commit(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    result = store.invalidate("op-cascade-1", r1)
    cascaded = {c["id"] for c in result["cascade"]}
    assert cascaded == {r1, r2, r3}  # R4/R5 不受影响
    for rid in (r1, r2, r3):
        rec = store.get_record(rid)
        assert rec["status"] == "invalid"
        # 稳定的失效来源：裁决目标编号
        assert rec["invalidated_by"] == r1
    assert store.get_record(r5)["status"] == "valid"
    store.assert_invariants()


def test_invalidation_of_midpoint_still_cascades_downstream(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    result = store.invalidate("op-mid", r2)
    assert {c["id"] for c in result["cascade"]} == {r2, r3}
    assert store.get_record(r1)["status"] == "valid"
    assert store.get_record(r2)["invalidated_by"] == r2
    assert store.get_record(r3)["invalidated_by"] == r2


def test_idempotent_same_operation_returns_first_result(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    first = store.invalidate("op-same", r1)
    second = store.invalidate("op-same", r1)
    third = store.invalidate("op-same", r1)
    assert first["cascade"] == second["cascade"] == third["cascade"]
    assert first["replayed"] is False
    assert second["replayed"] is True and third["replayed"] is True
    # 操作重放结果可查询
    stored = store.get_operation("op-same")
    assert stored["cascade"] == first["cascade"]
    assert stored["result"] == "completed"


def test_same_operation_different_target_conflicts_and_keeps_state(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    store.invalidate("op-conflict", r1)
    with pytest.raises(StoreError) as ei:
        store.invalidate("op-conflict", r4)
    assert ei.value.status == 409
    assert ei.value.code == "OPERATION_CONFLICT"
    assert ei.value.details["original_target"] == r1
    assert ei.value.details["conflicting_target"] == r4
    # 状态未被改变：R4/R5 仍有效
    assert store.get_record(r4)["status"] == "valid"
    assert store.get_record(r5)["status"] == "valid"


def test_reference_missing_record_rejected(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, ["R000999"])
    assert ei.value.code == "PARENT_NOT_FOUND"
    assert ei.value.details["missing_parent_ids"] == ["R000999"]


def test_self_reference_rejected(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, ["RX1"],
                            record_id="RX1")
    assert ei.value.code == "SELF_REFERENCE"
    assert "RX1" in ei.value.details["parent_ids"]
    with pytest.raises(StoreError):
        store.get_record("RX1")  # 未落库


def test_reference_invalid_record_rejected_and_existing_preserved(store):
    r1, r2, r3, r4, r5 = make_chain(store)
    store.invalidate("op-x", r4)  # R4/R5 失效
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, [r1, r4])
    assert ei.value.code == "PARENT_INVALID"
    assert ei.value.details["invalid_parent_ids"] == [r4]
    # 既有可用结论原样保留
    assert store.get_record(r1)["status"] == "valid"
    assert store.get_record(r5)["status"] == "invalid"
    assert store.get_record(r5)["invalidated_by"] == r4


def test_invalidate_missing_target_is_locatable_error(store):
    with pytest.raises(StoreError) as ei:
        store.invalidate("op-missing", "R000999")
    assert ei.value.status == 404
    assert ei.value.details["record_id"] == "R000999"
    # 未占用操作标识：修正目标后可正常使用
    make_chain(store)
    result = store.invalidate("op-missing", "R000001")
    assert result["replayed"] is False


def test_raw_record_cannot_have_parents(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("raw", {"value": "x"}, ["R000001"])
    assert ei.value.code == "RAW_RECORD_HAS_PARENTS"


def test_derived_requires_parents(store):
    with pytest.raises(StoreError) as ei:
        store.create_record("derived", {"value": "x"}, [])
    assert ei.value.code == "DERIVED_RECORD_WITHOUT_BASIS"


def test_cycle_guard_detects_ancestor_reaching_new_node(store):
    """成环守卫：API 下新节点只会指向已有节点，成环本就被结构性排除，
    此用例用一条遗留边验证守卫的判定方向正确（防御性实现）。"""
    a = store.create_record("raw", {"value": "a"}, None, record_id="A")
    c = store.create_record("raw", {"value": "c"}, None, record_id="C")
    with store._lock:
        store._conn.execute("BEGIN IMMEDIATE")
        store._conn.execute(
            "INSERT INTO edges(child_id, parent_id, seq) VALUES ('A','C',0)")
        store._conn.execute("COMMIT")
    # A 沿 parent 方向可达 C：若新节点是 C 且候选前序含 A，则成环
    assert store._reaches_ancestor_locked(c["id"], {a["id"]}) is True
    # 不可达时不误报
    d = store.create_record("raw", {"value": "d"}, None, record_id="D")
    assert store._reaches_ancestor_locked(d["id"], {a["id"]}) is False


def test_concurrent_create_vs_invalidate_never_orphans_validity(store):
    """新推导与失效裁决竞争后：不存在有效记录依赖失效记录。"""
    pivot = store.create_record("raw", {"value": "pivot"}, None)["id"]
    errors: list[Exception] = []

    def worker(i: int):
        try:
            if i % 3 == 0:
                store.invalidate(f"op-inv-{i}", pivot)
            else:
                store.create_record(
                    "derived", {"value": f"d-{i}"}, [pivot])
        except StoreError:
            pass  # 竞争失败是合法结局，关键是不变量
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(worker, range(60)))
    assert not errors
    store.assert_invariants()
    # 裁决只允许一次成功；之后所有对 pivot 的引用都被拒
    pivot_rec = store.get_record(pivot)
    assert pivot_rec["status"] == "invalid"
    children = [r for r in store.list_records()
                if pivot in r["parent_ids"]]
    assert all(c["status"] == "invalid" for c in children)


def test_concurrent_duplicate_invalidation_single_winner(store):
    p = store.create_record("raw", {"value": "p"}, None)["id"]

    def invoke():
        try:
            return store.invalidate("op-race-single", p)
        except StoreError as e:
            return e

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: invoke(), range(32)))
    completed = [r for r in results if not isinstance(r, StoreError)]
    assert completed, "至少一次裁决应成功"
    # 所有成功返回必须是同一份首次结果
    first = completed[0]["cascade"]
    assert all(r["cascade"] == first for r in completed)
    store.assert_invariants()


def test_persistence_after_reopen(tmp_path):
    db = str(tmp_path / "persist.db")
    s1 = CalibrationStore(db)
    a = s1.create_record("raw", {"value": "a"}, None)
    b = s1.create_record("derived", {"value": "b"}, [a["id"]])
    s1.invalidate("op-restart", a["id"])
    s1.close()

    s2 = CalibrationStore(db)  # 重启
    assert s2.get_record(a["id"])["status"] == "invalid"
    assert s2.get_record(b["id"])["status"] == "invalid"
    assert s2.get_record(b["id"])["invalidated_by"] == a["id"]
    # 操作重放：返回首次结果
    replayed = s2.invalidate("op-restart", a["id"])
    assert replayed["replayed"] is True
    assert {c["id"] for c in replayed["cascade"]} == {a["id"], b["id"]}
    assert s2.get_operation("op-restart")["result"] == "completed"
    s2.close()
