"""Flask 接口层测试：稳定编号、有效性、直接依据与错误反馈均经真实 HTTP 语义。"""

from __future__ import annotations

import json

import pytest

from app.server import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "api.db"))
    app.testing = True
    return app.test_client()


def post(client, path, body):
    return client.post(path, data=json.dumps(body),
                       content_type="application/json")


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "低温探测器标定谱系".encode() in resp.data


def test_full_lifecycle_over_http(client):
    # 原始记录 -> 稳定编号
    r1 = post(client, "/api/records",
              {"kind": "raw", "payload": {"value": "77K channel-0"}})
    assert r1.status_code == 201
    assert r1.get_json()["id"] == "R000001"

    r2 = post(client, "/api/records",
              {"kind": "raw", "payload": {"value": "4K channel-1"}})
    # 推导记录引用两个有效前序
    d = post(client, "/api/records", {
        "kind": "derived",
        "payload": {"value": "gain calibration"},
        "parent_ids": ["R000001", "R000002"],
    })
    assert d.status_code == 201
    body = d.get_json()
    assert body["parent_ids"] == ["R000001", "R000002"]
    assert body["status"] == "valid"

    # 列表展示稳定编号 / 有效性 / 直接依据
    listed = client.get("/api/records").get_json()
    assert len(listed) == 3
    assert {r["id"] for r in listed} == {"R000001", "R000002", "R000003"}

    # 级联失效
    inv = post(client, "/api/records/R000001/invalidate",
               {"operation_id": "http-op-1"})
    assert inv.status_code == 200
    cascaded = {c["id"] for c in inv.get_json()["cascade"]}
    assert cascaded == {"R000001", "R000003"}

    r1_after = client.get("/api/records/R000001").get_json()
    assert r1_after["status"] == "invalid"
    assert r1_after["invalidated_by"] == "R000001"
    r3_after = client.get("/api/records/R000003").get_json()
    assert r3_after["invalidated_by"] == "R000001"
    # R2 不受影响
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"

    # 重复裁决返回首次结果
    again = post(client, "/api/records/R000001/invalidate",
                 {"operation_id": "http-op-1"})
    assert again.status_code == 200
    assert again.get_json()["replayed"] is True
    assert {c["id"] for c in again.get_json()["cascade"]} == cascaded

    # 操作结果可经操作标识查询
    opq = client.get("/api/operations/http-op-1")
    assert opq.status_code == 200
    assert opq.get_json()["result"] == "completed"


def test_same_op_id_different_target_conflicts(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "b"}})
    assert post(client, "/api/records/R000001/invalidate",
                {"operation_id": "op"}).status_code == 200
    conflict = post(client, "/api/records/R000002/invalidate",
                    {"operation_id": "op"})
    assert conflict.status_code == 409
    detail = conflict.get_json()["error"]
    assert detail["code"] == "OPERATION_CONFLICT"
    assert detail["details"]["original_target"] == "R000001"
    # R2 未被牵连
    assert client.get("/api/records/R000002").get_json()["status"] == "valid"


def test_error_responses_are_locatable(client):
    # 引用不存在记录
    r = post(client, "/api/records", {
        "kind": "derived", "payload": {}, "parent_ids": ["R000999"]})
    assert r.status_code == 422
    e = r.get_json()["error"]
    assert e["code"] == "PARENT_NOT_FOUND"
    assert e["details"]["missing_parent_ids"] == ["R000999"]

    # 自引用
    r = post(client, "/api/records", {
        "kind": "derived", "record_id": "SELF",
        "payload": {}, "parent_ids": ["SELF"]})
    assert r.get_json()["error"]["code"] == "SELF_REFERENCE"

    # 裁决不存在记录
    r = post(client, "/api/records/NOPE/invalidate",
             {"operation_id": "opx"})
    assert r.status_code == 404
    assert r.get_json()["error"]["details"]["record_id"] == "NOPE"

    # 缺少操作标识
    r = post(client, "/api/records/R000001/invalidate", {})
    assert r.status_code == 400
    assert r.get_json()["error"]["code"] == "OPERATION_ID_REQUIRED"


def test_cannot_base_new_derivation_on_invalidated(client):
    post(client, "/api/records", {"kind": "raw", "payload": {"value": "a"}})
    post(client, "/api/records/R000001/invalidate",
         {"operation_id": "kill"})
    r = post(client, "/api/records", {
        "kind": "derived", "payload": {"value": "x"},
        "parent_ids": ["R000001"]})
    assert r.status_code == 422
    assert r.get_json()["error"]["code"] == "PARENT_INVALID"


# --------------------------------------------------------------------------- #
# 密封转交包：导出 / 接入 / 重复 / 篡改 / 失效依据
# --------------------------------------------------------------------------- #
@pytest.fixture()
def two_clients(tmp_path):
    app_a = create_app(str(tmp_path / "a.db"))
    app_b = create_app(str(tmp_path / "b.db"))
    app_a.testing = True
    app_b.testing = True
    return app_a.test_client(), app_b.test_client()


def _build_lineage(client):
    post(client, "/api/records",
         {"kind": "raw", "ext_id": "e-raw1", "payload": {"v": "r1"}})
    post(client, "/api/records",
         {"kind": "raw", "ext_id": "e-raw2", "payload": {"v": "r2"}})
    post(client, "/api/records", {
        "kind": "derived", "ext_id": "e-d3", "payload": {"v": "r3"},
        "parent_ids": ["R000001", "R000002"]})
    root = post(client, "/api/records", {
        "kind": "derived", "ext_id": "e-root", "payload": {"v": "root"},
        "parent_ids": ["R000003", "R000001"]})
    return root.get_json()


def test_export_import_handoff_over_http(two_clients):
    ca, cb = two_clients
    root = _build_lineage(ca)

    exp = post(ca, f"/api/records/{root['id']}/export", {})
    assert exp.status_code == 200
    pack = exp.get_json()
    assert pack["package_id"] and pack["digest"]
    assert pack["root_ext_id"] == "e-root"
    assert len(pack["records"]) == 4

    imp = post(cb, "/api/packages/import", pack)
    assert imp.status_code == 200
    body = imp.get_json()
    assert body["replayed"] is False
    local_root = cb.get(f"/api/records/{body['root_record_id']}").get_json()
    assert local_root["ext_id"] == "e-root"
    assert local_root["status"] == "valid"
    # 导入后的本地直接依据
    assert set(body["direct_parent_ids"]) == set(local_root["parent_ids"])
    assert len(body["direct_parent_ids"]) == 2


def test_import_endpoint_accepts_wrapped_package_string(two_clients):
    ca, cb = two_clients
    root = _build_lineage(ca)
    pack = post(ca, f"/api/records/{root['id']}/export", {}).get_json()
    r = post(cb, "/api/packages/import",
             {"package": json.dumps(pack)})
    assert r.status_code == 200
    assert r.get_json()["root_ext_id"] == "e-root"


def test_duplicate_import_returns_first_mapping_http(two_clients):
    ca, cb = two_clients
    root = _build_lineage(ca)
    pack = post(ca, f"/api/records/{root['id']}/export", {}).get_json()
    first = post(cb, "/api/packages/import", pack).get_json()
    second = post(cb, "/api/packages/import", pack).get_json()
    assert second["replayed"] is True
    assert second["root_record_id"] == first["root_record_id"]
    # 可按包标识查询首次映射
    got = cb.get(f"/api/imports/{pack['package_id']}")
    assert got.status_code == 200
    assert got.get_json()["mapping"] == first["mapping"]
    # 只有 4 条本地记录
    assert len(cb.get("/api/records").get_json()) == 4


def test_tampered_package_payload_conflicts_or_digest_error(two_clients):
    import copy
    ca, cb = two_clients
    root = _build_lineage(ca)
    pack = post(ca, f"/api/records/{root['id']}/export", {}).get_json()
    post(cb, "/api/packages/import", pack)

    # 篡改载荷并重算自洽摘要：package_id 不变 -> 409 PACKAGE_CONFLICT
    tampered = copy.deepcopy(pack)
    tampered["records"][0]["payload"] = {"v": "EVIL"}
    imp = post(cb, "/api/packages/import", tampered)
    assert imp.status_code == 422
    assert imp.get_json()["error"]["code"] == "DIGEST_MISMATCH"

    import hashlib
    tampered["digest"] = hashlib.sha256(
        json.dumps({
            "format": tampered["format"],
            "root_ext_id": tampered["root_ext_id"],
            "records": sorted(tampered["records"],
                              key=lambda r: r["ext_id"]),
        }, ensure_ascii=False, sort_keys=True,
            separators=(",", ":")).encode()).hexdigest()
    conflict = post(cb, "/api/packages/import", tampered)
    assert conflict.status_code == 409
    assert conflict.get_json()["error"]["code"] == "PACKAGE_CONFLICT"
    # 本地既有结论未改变
    assert len(cb.get("/api/records").get_json()) == 4


def test_import_missing_ancestor_is_locatable_http(two_clients):
    ca, cb = two_clients
    root = _build_lineage(ca)
    pack = post(ca, f"/api/records/{root['id']}/export", {}).get_json()
    removed = pack["records"][1]["ext_id"]
    pack["records"] = [r for r in pack["records"]
                       if r["ext_id"] != removed]
    import hashlib
    pack["digest"] = hashlib.sha256(
        json.dumps({
            "format": pack["format"], "root_ext_id": pack["root_ext_id"],
            "records": sorted(pack["records"], key=lambda r: r["ext_id"]),
        }, ensure_ascii=False, sort_keys=True,
            separators=(",", ":")).encode()).hexdigest()
    r = post(cb, "/api/packages/import", pack)
    assert r.status_code == 422
    err = r.get_json()["error"]
    assert err["code"] == "MISSING_ANCESTOR"
    assert removed in err["details"]["missing_ext_ids"]
    # 原子拒绝：本地无任何记录
    assert cb.get("/api/records").get_json() == []


def test_import_then_cascade_invalidation_still_correct(two_clients):
    ca, cb = two_clients
    root = _build_lineage(ca)
    pack = post(ca, f"/api/records/{root['id']}/export", {}).get_json()
    body = post(cb, "/api/packages/import", pack).get_json()
    mapping = {m["ext_id"]: m["record_id"] for m in body["mapping"]}
    inv = post(cb, f"/api/records/{mapping['e-raw1']}/invalidate",
               {"operation_id": "post-import-kill"})
    cascaded = {c["id"] for c in inv.get_json()["cascade"]}
    assert cascaded == {mapping["e-raw1"], mapping["e-d3"],
                        mapping["e-root"]}
    assert cb.get(f"/api/records/{mapping['e-raw2']}").get_json()[
        "status"] == "valid"


def test_export_invalid_root_rejected_http(two_clients):
    ca, _ = two_clients
    root = _build_lineage(ca)
    post(ca, "/api/records/R000001/invalidate", {"operation_id": "kill"})
    r = post(ca, f"/api/records/{root['id']}/export", {})
    assert r.status_code == 409
    assert r.get_json()["error"]["code"] == "PACKAGE_ROOT_INVALID"
