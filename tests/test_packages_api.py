"""密封转交包 HTTP 接口：导出、接入、重复接入、篡改拒绝与双谱系交接。"""

from __future__ import annotations

import copy
import json

import pytest

from app.server import create_app


def _build_lineage(client):
    client.post("/api/records",
                data=json.dumps({"kind": "raw",
                                 "payload": {"value": "raw-1", "unit": "mV"}}),
                content_type="application/json")
    client.post("/api/records",
                data=json.dumps({"kind": "raw",
                                 "payload": {"value": "raw-2"}}),
                content_type="application/json")
    client.post("/api/records",
                data=json.dumps({"kind": "derived",
                                 "payload": {"value": "gain"},
                                 "parent_ids": ["R000001", "R000002"]}),
                content_type="application/json")
    client.post("/api/records",
                data=json.dumps({"kind": "derived",
                                 "payload": {"value": "drift"},
                                 "parent_ids": ["R000003"]}),
                content_type="application/json")


def post_json(client, path, body):
    return client.post(path, data=json.dumps(body),
                       content_type="application/json")


@pytest.fixture()
def pair(tmp_path):
    app_a = create_app(str(tmp_path / "lineage-a.db"))
    app_b = create_app(str(tmp_path / "lineage-b.db"))
    app_a.testing = app_b.testing = True
    return app_a.test_client(), app_b.test_client()


def test_export_endpoint_returns_sealed_package(pair):
    ca, _ = pair
    _build_lineage(ca)
    resp = post_json(ca, "/api/records/R000004/export", {})
    assert resp.status_code == 200
    body = resp.get_json()
    pkg = body["package"]
    assert pkg["format"] == "calibration-package/v1"
    assert pkg["package_id"].startswith("PKG-")
    assert pkg["payload_digest"].startswith("sha256:")
    assert body["root_record_id"] == "R000004"
    assert body["root_parent_ids"] == ["R000003"]
    assert len(pkg["records"]) == 4


def test_export_invalid_conclusion_conflicts(pair):
    ca, _ = pair
    _build_lineage(ca)
    post_json(ca, "/api/records/R000001/invalidate",
              {"operation_id": "op-kill"})
    resp = post_json(ca, "/api/records/R000004/export", {})
    assert resp.status_code == 409
    assert resp.get_json()["error"]["code"] == "PACKAGE_ROOT_NOT_VALID"


def test_two_independent_lineages_handover_and_local_ids(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]

    # B 谱系先有本地记录，接入后既有编号与内容不变
    pre = post_json(cb, "/api/records",
                    {"kind": "raw", "payload": {"value": "b-local"}})
    assert pre.get_json()["id"] == "R000001"

    imp = post_json(cb, "/api/packages/import", env)
    assert imp.status_code == 201
    body = imp.get_json()
    assert body["root_record_id"] == "R000005"
    assert body["record_count"] == 4
    root = cb.get("/api/records/R000005").get_json()
    # 直接依据是 B 本地编号，且闭包完整
    assert root["parent_ids"] == ["R000004"]
    r4 = cb.get("/api/records/R000004").get_json()
    assert set(r4["parent_ids"]) == {"R000002", "R000003"}
    assert cb.get("/api/records/R000001").get_json()["payload"] == \
        {"value": "b-local"}


def test_import_accepts_wrapped_and_bare_envelope(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]
    r1 = post_json(cb, "/api/packages/import", {"package": copy.deepcopy(env)})
    assert r1.status_code == 201
    # 裸包重放
    r2 = post_json(cb, "/api/packages/import", copy.deepcopy(env))
    assert r2.status_code == 200
    assert r2.get_json()["replayed"] is True
    assert r2.get_json()["root_record_id"] == r1.get_json()["root_record_id"]


def test_duplicate_import_is_idempotent_and_queryable(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]
    first = post_json(cb, "/api/packages/import", env).get_json()
    second = post_json(cb, "/api/packages/import", copy.deepcopy(env)).get_json()
    third = post_json(cb, "/api/packages/import", copy.deepcopy(env)).get_json()
    assert second["replayed"] is True and third["replayed"] is True
    assert second["root_record_id"] == third["root_record_id"] == \
        first["root_record_id"]
    # 只写入一份记录
    assert len(cb.get("/api/records").get_json()) == 4
    # 包映射可按包标识查询
    q = cb.get(f"/api/packages/{env['package_id']}")
    assert q.status_code == 200
    assert q.get_json()["root_record_id"] == first["root_record_id"]


def test_same_package_id_different_payload_conflicts(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]
    post_json(cb, "/api/packages/import", env)

    other = post_json(ca, "/api/records/R000002/export", {}).get_json()["package"]
    other["package_id"] = env["package_id"]  # 冒用包标识
    resp = post_json(cb, "/api/packages/import", other)
    assert resp.status_code == 409
    err = resp.get_json()["error"]
    assert err["code"] == "PACKAGE_CONFLICT"
    assert err["details"]["existing_digest"] == env["payload_digest"]
    assert err["details"]["incoming_digest"] == other["payload_digest"]
    # 既有记录未被改变
    assert len(cb.get("/api/records").get_json()) == 4


def test_tampered_package_is_atomically_rejected(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]

    # 1) 篡改摘要
    bad_digest = copy.deepcopy(env)
    bad_digest["payload_digest"] = "sha256:" + "f" * 64
    r = post_json(cb, "/api/packages/import", bad_digest)
    assert r.status_code == 422
    assert r.get_json()["error"]["code"] == "PACKAGE_DIGEST_MISMATCH"

    # 2) 篡改载荷（外部标识 Merkle 校验拦截）
    bad_payload = copy.deepcopy(env)
    bad_payload["records"][0]["payload"] = {"value": "tampered"}
    r = post_json(cb, "/api/packages/import", bad_payload)
    assert r.get_json()["error"]["code"] == "PACKAGE_EXTERNAL_ID_MISMATCH"

    # 3) 删除祖先记录
    root = env["root_external_id"]
    ancestor = next(x["external_id"] for x in env["records"]
                    if not x["parent_external_ids"]
                    and x["external_id"] != root)
    thinned = copy.deepcopy(env)
    thinned["records"] = [x for x in thinned["records"]
                          if x["external_id"] != ancestor]
    r = post_json(cb, "/api/packages/import", thinned)
    assert r.status_code == 422
    err = r.get_json()["error"]
    assert err["code"] == "PACKAGE_MISSING_ANCESTOR"
    assert ancestor in err["details"]["missing_external_ids"]

    # 全部拒绝：B 谱系无任何写入
    assert cb.get("/api/records").get_json() == []


def test_import_invalid_basis_rejected_after_local_invalidation(pair):
    ca, cb = pair
    _build_lineage(ca)
    raw_pkg = post_json(ca, "/api/records/R000001/export", {}).get_json()["package"]
    root_pkg = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]

    one = post_json(cb, "/api/packages/import", raw_pkg).get_json()
    post_json(cb, f"/api/records/{one['root_record_id']}/invalidate",
              {"operation_id": "op-kill"})
    n = len(cb.get("/api/records").get_json())

    resp = post_json(cb, "/api/packages/import", root_pkg)
    assert resp.status_code == 422
    assert resp.get_json()["error"]["code"] == "PACKAGE_BASIS_INVALID"
    # 原子拒绝：无新记录，且不存在有效记录依赖失效依据
    assert len(cb.get("/api/records").get_json()) == n


def test_imported_lineage_still_supports_cascade_invalidation(pair):
    ca, cb = pair
    _build_lineage(ca)
    env = post_json(ca, "/api/records/R000004/export", {}).get_json()["package"]
    body = post_json(cb, "/api/packages/import", env).get_json()
    root_id = body["root_record_id"]

    # 失效导入闭包中的一条祖先，级联应到达导入的根结论
    inv = post_json(cb, "/api/records/R000002/invalidate",
                    {"operation_id": "op-cascade"})
    cascaded = {c["id"] for c in inv.get_json()["cascade"]}
    assert root_id in cascaded
    assert cb.get(f"/api/records/{root_id}").get_json()["status"] == "invalid"

    # 幂等重放裁决仍可用
    again = post_json(cb, "/api/records/R000002/invalidate",
                      {"operation_id": "op-cascade"})
    assert again.get_json()["replayed"] is True


def test_package_query_unknown_404(pair):
    _, cb = pair
    r = cb.get("/api/packages/PKG-nope")
    assert r.status_code == 404
    assert r.get_json()["error"]["code"] == "PACKAGE_NOT_FOUND"
