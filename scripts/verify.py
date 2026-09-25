#!/usr/bin/env python3
"""一次性验收服务 verify。

在低温探测器标定谱系这一真实业务场景下依次执行：

  1. 代码测试：pytest 全量单元/接口用例（级联失效、幂等/冲突、引用校验、
     并发竞争、重启持久化）；
  2. 构建检查：compileall 语法构建 + 应用可导入；
  3. API/HTTP 冒烟：拉起真实 gunicorn 服务（4 worker，跨进程竞争），
     经 HTTP 复现稳定编号、级联失效、操作重放、同标识换目标冲突、
     可定位错误反馈、并发竞争不变量，以及重启后谱系/失效状态/操作重放；
  4. 密封转交包：两套**独立谱系**（各自独立数据库的 gunicorn 服务）依次执行
     导出 -> 接入 -> 重复接入 -> 篡改包提交，观察映射稳定、原子拒绝，
     以及接入后既有级联失效仍正确；
  5. 可选：若设置 VERIFY_TARGET_URL，则对已运行的服务（如 compose 中的
     web 服务）追加一次真实 HTTP 冒烟；若同时设置 HANDOFF_TARGET_URL，
     则在两套外部谱系（web / web-b）间复现完整转交流程。

全部步骤通过则退出码 0，任一失败退出码 1。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PASS = "PASS"
FAIL = "FAIL"


class Report:
    def __init__(self) -> None:
        self.items: list[tuple[str, str, str]] = []
        self.ok = True

    def step(self, name: str, ok: bool, detail: str = "") -> None:
        self.items.append((PASS if ok else FAIL, name, detail))
        self.ok = self.ok and ok
        print(f"[{PASS if ok else FAIL}] {name}" + (f" — {detail}" if detail else ""))

    def check(self, name: str, cond: bool, detail: str = "") -> bool:
        self.step(name, bool(cond), detail)
        return bool(cond)


def wait_for_port(host: str, port: int, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def wait_healthy(base_url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{base_url}/health", timeout=2)
            if r.status_code == 200 and r.json().get("status") == "ok":
                return True
        except requests.RequestException:
            pass
        time.sleep(0.3)
    return False


class Server:
    """真实 gunicorn 子进程（4 worker，制造跨进程写竞争）。"""

    def __init__(self, db_path: str, port: int, workers: int = 4):
        self.db_path = db_path
        self.port = port
        self.workers = workers
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        env = os.environ.copy()
        env["CALIBRATION_DB"] = self.db_path
        env["PYTHONPATH"] = str(ROOT)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "gunicorn",
             f"--workers={self.workers}",
             f"--bind=127.0.0.1:{self.port}",
             "--timeout=30",
             "app.server:create_app()"],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )
        if not wait_for_port("127.0.0.1", self.port):
            out = self.proc.stdout.read() if self.proc.stdout else ""
            raise RuntimeError(f"服务未在端口 {self.port} 就绪\n{out}")
        if not wait_healthy(f"http://127.0.0.1:{self.port}"):
            raise RuntimeError("健康端点未返回 ok")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None


# --------------------------------------------------------------------------- #
# 阶段 1/2：代码测试 + 构建检查
# --------------------------------------------------------------------------- #
def run_code_phase(rep: Report) -> None:
    print("\n=== 阶段 1：代码测试（pytest） ===")
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=str(ROOT),
    )
    rep.step("pytest 全量用例", proc.returncode == 0,
             "退出码 0" if proc.returncode == 0 else f"退出码 {proc.returncode}")

    print("\n=== 阶段 2：构建检查 ===")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "scripts"],
        cwd=str(ROOT))
    rep.step("compileall 语法构建", proc.returncode == 0)

    proc = subprocess.run(
        [sys.executable, "-c",
         "from app.server import create_app; create_app().test_client(); "
         "print('factory ok')"],
        cwd=str(ROOT), capture_output=True, text=True)
    rep.step("应用工厂可导入并构造", proc.returncode == 0,
             proc.stdout.strip() or proc.stderr.strip()[-300:])


# --------------------------------------------------------------------------- #
# 阶段 3：自管服务的 HTTP 全链路冒烟（含重启）
# --------------------------------------------------------------------------- #
def _post(base: str, path: str, body: dict) -> requests.Response:
    return requests.post(f"{base}{path}", json=body, timeout=10)


def run_self_hosted_phase(rep: Report, workdir: Path) -> None:
    print("\n=== 阶段 3：API/HTTP 冒烟（真实 gunicorn x4 worker） ===")
    db_path = str(workdir / "verify.db")
    port = int(os.environ.get("VERIFY_PORT", "18080"))
    base = f"http://127.0.0.1:{port}"
    server = Server(db_path, port)
    server.start()
    rep.step("服务启动且 /health 可访问", True, f"{base}/health")
    try:
        _http_lifecycle(rep, base)
        _http_error_cases(rep, base)
        _http_concurrency(rep, base)
    finally:
        server.stop()

    print("\n=== 阶段 4：重启后谱系 / 失效状态 / 操作重放 ===")
    server2 = Server(db_path, port, workers=2)
    server2.start()
    try:
        _http_restart_persistence(rep, base)
    finally:
        server2.stop()


# --------------------------------------------------------------------------- #
# 阶段 4b：两套独立谱系的密封转交包全链路
# --------------------------------------------------------------------------- #
def run_handoff_phase(rep: Report, workdir: Path) -> None:
    print("\n=== 阶段 4b：密封转交包（两套独立谱系：外场 A -> 本地 B） ===")
    port_a = int(os.environ.get("VERIFY_PORT", "18080"))
    port_b = port_a + 1
    base_a = f"http://127.0.0.1:{port_a}"
    base_b = f"http://127.0.0.1:{port_b}"
    srv_a = Server(str(workdir / "lineage_a.db"), port_a, workers=4)
    srv_b = Server(str(workdir / "lineage_b.db"), port_b, workers=4)
    srv_a.start()
    srv_b.start()
    try:
        _http_handoff(rep, base_a, base_b)
        _http_handoff_concurrency(rep, base_a, base_b)
    finally:
        srv_b.stop()
        srv_a.stop()


def _build_lineage_a_http(base: str) -> dict:
    """外场谱系 A：raw1, raw2 <- d3 <- root(引用 d3 与 raw1)。返回各记录。"""
    raw1 = _post(base, "/api/records",
                 {"kind": "raw", "ext_id": "field-raw1",
                  "payload": {"value": "77K/A"}}).json()
    raw2 = _post(base, "/api/records",
                 {"kind": "raw", "ext_id": "field-raw2",
                  "payload": {"value": "4K/B"}}).json()
    d3 = _post(base, "/api/records", {
        "kind": "derived", "ext_id": "field-d3",
        "payload": {"value": "gain"},
        "parent_ids": [raw1["id"], raw2["id"]]}).json()
    root = _post(base, "/api/records", {
        "kind": "derived", "ext_id": "field-root",
        "payload": {"value": "final-offset"},
        "parent_ids": [d3["id"], raw1["id"]]}).json()
    return {"raw1": raw1, "raw2": raw2, "d3": d3, "root": root}


def _http_handoff(rep: Report, base_a: str, base_b: str) -> None:
    nodes = _build_lineage_a_http(base_a)
    root = nodes["root"]

    # --- 导出：按直接依据闭包 ---
    exp = _post(base_a, f"/api/records/{root['id']}/export", {})
    rep.check("外场导出密封包成功", exp.status_code == 200,
              f"HTTP {exp.status_code}")
    pack = exp.json()
    ext_ids = {r["ext_id"] for r in pack["records"]}
    rep.check("包内含完整祖先闭包且按稳定外部标识引用",
              exp.status_code == 200
              and ext_ids == {"field-raw1", "field-raw2",
                              "field-d3", "field-root"}
              and "R000" not in json.dumps(pack),
              f"{len(pack['records'])} 条，package_id={pack['package_id']}")
    rep.check("包摘要与包标识齐备",
              bool(pack["digest"]) and bool(pack["package_id"])
              and pack["root_ext_id"] == "field-root")

    # 已失效结论不得导出
    kill = _post(base_a, f"/api/records/{nodes['raw2']['id']}/invalidate",
                 {"operation_id": "handoff-disable-raw2"})
    # raw2 失效会级联 d3、root（它们依赖 raw2/链路）
    exp_bad = _post(base_a, f"/api/records/{root['id']}/export", {})
    rep.check("已失效结论不能导出（定位拒绝）",
              kill.status_code == 200 and exp_bad.status_code == 409
              and exp_bad.json()["error"]["code"] == "PACKAGE_ROOT_INVALID")

    # 重建一套干净 A 谱系用于接入（上面的 root 已失效）
    base_a2 = base_a
    # 直接在 B 上用原始包（内容仍是有效闭包快照）也可，但为贴合“仍有效结论”，
    # 另起一份全新结论重新导出。
    r1 = _post(base_a2, "/api/records",
               {"kind": "raw", "ext_id": "f2-raw1",
                "payload": {"value": "77K/A2"}}).json()
    r2 = _post(base_a2, "/api/records",
               {"kind": "raw", "ext_id": "f2-raw2",
                "payload": {"value": "4K/B2"}}).json()
    d3 = _post(base_a2, "/api/records", {
        "kind": "derived", "ext_id": "f2-d3",
        "payload": {"value": "gain2"},
        "parent_ids": [r1["id"], r2["id"]]}).json()
    rt = _post(base_a2, "/api/records", {
        "kind": "derived", "ext_id": "f2-root",
        "payload": {"value": "final2"},
        "parent_ids": [d3["id"], r1["id"]]}).json()
    pack = _post(base_a2, f"/api/records/{rt['id']}/export", {}).json()

    # --- 接入 B ---
    imp = _post(base_b, "/api/packages/import", pack)
    rep.check("本地接入密封包成功", imp.status_code == 200,
              f"HTTP {imp.status_code} {imp.text[:200]}")
    body = imp.json()
    mapping = {m["ext_id"]: m["record_id"] for m in body["mapping"]}
    rep.check("接入后为整包建立外部标识映射（4 条）",
              imp.status_code == 200 and len(mapping) == 4
              and set(mapping) == {"f2-raw1", "f2-raw2", "f2-d3", "f2-root"})
    local_root = requests.get(
        f"{base_b}/api/records/{body['root_record_id']}").json()
    rep.check("页面/接口可展示导入后本地编号与直接依据",
              local_root["ext_id"] == "f2-root"
              and set(local_root["parent_ids"]) ==
              {mapping["f2-d3"], mapping["f2-raw1"]},
              f"root={body['root_record_id']} basis={local_root['parent_ids']}")

    # --- 重复接入：返回首次映射 ---
    imp2 = _post(base_b, "/api/packages/import", pack)
    rep.check("重复接入完全相同的包返回首次映射",
              imp2.status_code == 200 and imp2.json()["replayed"] is True
              and imp2.json()["root_record_id"] == body["root_record_id"]
              and imp2.json()["mapping"] == body["mapping"])
    rec_count = len(requests.get(f"{base_b}/api/records").json())
    rep.check("重复接入不产生新记录", rec_count == 4, f"{rec_count} 条")

    # --- 篡改包提交：载荷改了但 package_id 不变 ---
    import copy
    import hashlib
    tampered = copy.deepcopy(pack)
    victim = next(r for r in tampered["records"]
                  if r["ext_id"] == "f2-raw1")
    victim["payload"] = {"value": "TAMPERED"}
    # 仅改载荷不改 digest -> 摘要不符
    dig_bad = _post(base_b, "/api/packages/import", tampered)
    rep.check("篡改载荷（摘要不符）被原子拒绝",
              dig_bad.status_code == 422
              and dig_bad.json()["error"]["code"] == "DIGEST_MISMATCH")
    # 连摘要一起伪造：package_id 仍由 root 派生 -> 同包标识不同载荷冲突
    canon = {
        "format": tampered["format"],
        "root_ext_id": tampered["root_ext_id"],
        "records": sorted(tampered["records"], key=lambda r: r["ext_id"]),
    }
    tampered["digest"] = hashlib.sha256(
        json.dumps(canon, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode()).hexdigest()
    conflict = _post(base_b, "/api/packages/import", tampered)
    rep.check("同一包标识不同载荷 -> 409 PACKAGE_CONFLICT",
              conflict.status_code == 409
              and conflict.json()["error"]["code"] == "PACKAGE_CONFLICT",
              conflict.text[:200])
    recs_after = requests.get(f"{base_b}/api/records").json()
    rep.check("原子拒绝后本地既有结论不改变（仍 4 条且有效）",
              len(recs_after) == 4
              and all(r["status"] == "valid" for r in recs_after))

    # 缺失祖先
    missing_pack = copy.deepcopy(pack)
    removed = missing_pack["records"][0]["ext_id"]
    missing_pack["records"] = [r for r in missing_pack["records"]
                               if r["ext_id"] != removed]
    canon = {
        "format": missing_pack["format"],
        "root_ext_id": missing_pack["root_ext_id"],
        "records": sorted(missing_pack["records"], key=lambda r: r["ext_id"]),
    }
    missing_pack["digest"] = hashlib.sha256(
        json.dumps(canon, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode()).hexdigest()
    miss = _post(base_b, "/api/packages/import", missing_pack)
    rep.check("缺失祖先被定位拒绝（MISSING_ANCESTOR）",
              miss.status_code == 422
              and miss.json()["error"]["code"] == "MISSING_ANCESTOR"
              and removed in
              miss.json()["error"]["details"]["missing_ext_ids"])

    # --- 接入后既有级联失效仍正确 ---
    inv = _post(base_b, f"/api/records/{mapping['f2-raw1']}/invalidate",
                {"operation_id": "post-import-cascade"})
    cascaded = {c["id"] for c in inv.json()["cascade"]}
    rep.check("接入后级联失效仍正确（raw1 -> d3 -> root）",
              inv.status_code == 200
              and cascaded == {mapping["f2-raw1"], mapping["f2-d3"],
                               mapping["f2-root"]}
              and requests.get(
                  f"{base_b}/api/records/{mapping['f2-raw2']}").json()[
                  "status"] == "valid",
              f"cascade={sorted(cascaded)}")
    # 失效后以失效节点为共享祖先再接入更大闭包应被拒
    # 直接用原包重复（此时根已在本地失效）-> 幂等返回首次映射，不改状态
    replay_after = _post(base_b, "/api/packages/import", pack)
    rep.check("失效后重复接入仍只回放首次映射",
              replay_after.status_code == 200
              and replay_after.json()["replayed"] is True)


def _http_handoff_concurrency(rep: Report, base_a: str,
                              base_b: str) -> None:
    """并发接入多份包与失效裁决竞争：无有效记录指向失效/不存在依据。"""
    packs = []
    for i in range(6):
        x = _post(base_a, "/api/records", {
            "kind": "raw", "ext_id": f"hc-raw-{i}",
            "payload": {"v": i}}).json()
        d = _post(base_a, "/api/records", {
            "kind": "derived", "ext_id": f"hc-d-{i}",
            "payload": {"v": i},
            "parent_ids": [x["id"]]}).json()
        packs.append(_post(base_a, f"/api/records/{d['id']}/export",
                           {}).json())

    errors: list[str] = []

    def one(i: int) -> None:
        s = requests.Session()
        try:
            if i % 2 == 0:
                s.post(f"{base_b}/api/packages/import",
                       json=packs[(i // 2) % len(packs)], timeout=15)
            else:
                recs = requests.get(f"{base_b}/api/records").json()
                if recs:
                    t = recs[i % len(recs)]["id"]
                    s.post(f"{base_b}/api/records/{t}/invalidate",
                           json={"operation_id": f"hc-inv-{i}"}, timeout=15)
        except requests.RequestException as exc:
            errors.append(str(exc))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one, range(48)))
    rep.check("转交并发期间无传输层错误", not errors, str(errors))

    records = requests.get(f"{base_b}/api/records").json()
    by_id = {r["id"]: r for r in records}
    bad = [r["id"] for r in records
           if r["status"] == "valid"
           and any(by_id.get(p, {}).get("status") in (None, "invalid")
                   for p in r["parent_ids"])]
    rep.check("并发接入后无有效记录指向不存在/失效依据", not bad,
              f"违规：{bad}")


def _http_lifecycle(rep: Report, base: str) -> None:
    a = _post(base, "/api/records",
              {"kind": "raw", "payload": {"value": "77K/A"}}).json()
    b = _post(base, "/api/records",
              {"kind": "raw", "payload": {"value": "4K/B"}}).json()
    c = _post(base, "/api/records", {
        "kind": "derived", "payload": {"value": "gain"},
        "parent_ids": [a["id"], b["id"]]}).json()
    d = _post(base, "/api/records", {
        "kind": "derived", "payload": {"value": "offset"},
        "parent_ids": [c["id"]]}).json()
    rep.check("稳定编号按序分配",
              [x["id"] for x in (a, b, c, d)] ==
              ["R000001", "R000002", "R000003", "R000004"])
    rep.check("提交即展示有效性与直接依据",
              c["status"] == "valid" and c["parent_ids"] == [a["id"], b["id"]])

    inv = _post(base, f"/api/records/{a['id']}/invalidate",
                {"operation_id": "verify-op-cascade"}).json()
    cascaded = {x["id"] for x in inv["cascade"]}
    rep.check("裁决在一次提交内级联到全部可达下游",
              cascaded == {a["id"], c["id"], d["id"]},
              f"cascade={sorted(cascaded)}")

    records = {r["id"]: r for r in requests.get(f"{base}/api/records").json()}
    rep.check("失效来源稳定且指向裁决目标",
              all(records[i]["invalidated_by"] == a["id"]
                  for i in (a["id"], c["id"], d["id"])))
    rep.check("无关节联结论保持有效", records[b["id"]]["status"] == "valid")

    again = _post(base, f"/api/records/{a['id']}/invalidate",
                  {"operation_id": "verify-op-cascade"}).json()
    rep.check("重复同一裁决返回首次结果（replayed）",
              again.get("replayed") is True
              and {x["id"] for x in again["cascade"]} == cascaded)

    conflict = _post(base, f"/api/records/{b['id']}/invalidate",
                     {"operation_id": "verify-op-cascade"})
    b_after = requests.get(f"{base}/api/records/{b['id']}").json()
    rep.check("同一操作标识改换目标 -> 409 且状态不变",
              conflict.status_code == 409
              and conflict.json()["error"]["code"] == "OPERATION_CONFLICT"
              and b_after["status"] == "valid")


def _http_error_cases(rep: Report, base: str) -> None:
    r = _post(base, "/api/records", {
        "kind": "derived", "payload": {}, "parent_ids": ["R000999"]})
    rep.check("引用不存在记录：拒绝并给出可定位反馈",
              r.status_code == 422
              and r.json()["error"]["details"]["missing_parent_ids"] == ["R000999"])

    r = _post(base, "/api/records", {
        "kind": "derived", "record_id": "LOOP",
        "payload": {}, "parent_ids": ["LOOP"]})
    rep.check("自引用：拒绝（SELF_REFERENCE），既有结论不变",
              r.status_code == 422
              and r.json()["error"]["code"] == "SELF_REFERENCE")

    r = _post(base, "/api/records", {
        "kind": "derived", "payload": {}, "parent_ids": ["R000001"]})
    rep.check("引用已失效记录：拒绝（PARENT_INVALID）",
              r.status_code == 422
              and r.json()["error"]["code"] == "PARENT_INVALID")

    r = _post(base, "/api/records/GHOST/invalidate",
              {"operation_id": "verify-op-ghost"})
    rep.check("裁决不存在记录：404 可定位",
              r.status_code == 404
              and r.json()["error"]["details"]["record_id"] == "GHOST")


def _http_concurrency(rep: Report, base: str) -> None:
    """新推导 vs 失效裁决 跨进程并发竞争。"""
    pivot = _post(base, "/api/records",
                  {"kind": "raw", "payload": {"value": "pivot"}}).json()["id"]
    # 预置多层有效下游，确保级联闭包在竞争中被真正检验
    pre1 = _post(base, "/api/records", {
        "kind": "derived", "payload": {"value": "pre1"},
        "parent_ids": [pivot]}).json()["id"]
    pre2 = _post(base, "/api/records", {
        "kind": "derived", "payload": {"value": "pre2"},
        "parent_ids": [pre1]}).json()["id"]
    errors: list[str] = []

    def one(i: int) -> None:
        s = requests.Session()
        try:
            if i % 4 == 0:
                s.post(f"{base}/api/records/{pivot}/invalidate",
                       json={"operation_id": "verify-op-race"}, timeout=15)
            else:
                s.post(f"{base}/api/records", json={
                    "kind": "derived",
                    "payload": {"value": f"race-{i}"},
                    "parent_ids": [pivot]}, timeout=15)
        except requests.RequestException as exc:
            errors.append(str(exc))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one, range(64)))

    rep.check("并发期间无传输层错误", not errors, str(errors))

    records = requests.get(f"{base}/api/records").json()
    by_id = {r["id"]: r for r in records}
    bad = [r["id"] for r in records
           if r["status"] == "valid"
           and any(by_id.get(p, {}).get("status") == "invalid"
                   for p in r["parent_ids"])]
    rep.check("竞争后不存在有效记录依赖失效记录", not bad,
              f"违规记录：{bad}" if bad else "")

    children = [r for r in records if pivot in r["parent_ids"]]
    rep.check("失效裁决闭包完整：pivot 的全部下游均失效",
              by_id[pivot]["status"] == "invalid"
              and all(c["status"] == "invalid" for c in children),
              f"pivot={pivot}, children={len(children)}")


def _http_restart_persistence(rep: Report, base: str) -> None:
    records = {r["id"]: r for r in requests.get(f"{base}/api/records").json()}
    ok = (records["R000001"]["status"] == "invalid"
          and records["R000003"]["status"] == "invalid"
          and records["R000003"]["invalidated_by"] == "R000001"
          and records["R000002"]["status"] == "valid")
    rep.check("重启后谱系与失效状态（含稳定来源）仍可查询", ok)

    replay = _post(base, "/api/records/R000001/invalidate",
                   {"operation_id": "verify-op-cascade"}).json()
    rep.check("重启后操作标识重放返回首次结果",
              replay.get("replayed") is True
              and {x["id"] for x in replay["cascade"]} ==
              {"R000001", "R000003", "R000004"})

    opq = requests.get(f"{base}/api/operations/verify-op-cascade").json()
    rep.check("操作结果可按标识查询", opq["result"] == "completed")


# --------------------------------------------------------------------------- #
# 阶段 5（可选）：对外部已运行服务冒烟（compose 中的 web）
# --------------------------------------------------------------------------- #
def run_external_phase(rep: Report, base_url: str) -> None:
    print(f"\n=== 阶段 5：对外部服务 {base_url} 冒烟 ===")
    tag = f"ext-{os.getpid()}-{int(time.time()*1000)}"
    if not rep.check("外部服务 /health 可访问", wait_healthy(base_url, 15)):
        return
    a = _post(base_url, "/api/records",
              {"kind": "raw", "payload": {"value": tag}}).json()
    b = _post(base_url, "/api/records", {
        "kind": "derived", "payload": {"value": tag + "-d"},
        "parent_ids": [a["id"]]}).json()
    inv = _post(base_url, f"/api/records/{a['id']}/invalidate",
                {"operation_id": f"{tag}-op"}).json()
    got = {x["id"] for x in inv["cascade"]}
    rep.check("外部服务级联失效正确", got == {a["id"], b["id"]},
              f"cascade={sorted(got)}")
    replay = _post(base_url, f"/api/records/{a['id']}/invalidate",
                   {"operation_id": f"{tag}-op"}).json()
    rep.check("外部服务裁决可幂等重放", replay.get("replayed") is True)


def run_external_handoff(rep: Report, base_a: str, base_b: str) -> None:
    """两套外部独立谱系（如 compose 的 web / web-b）间的真实转交冒烟。"""
    print(f"\n=== 阶段 6：两套外部谱系转交 {base_a} -> {base_b} ===")
    if not rep.check("外场 web /health 可访问", wait_healthy(base_a, 15)):
        return
    if not rep.check("本地 web-b /health 可访问", wait_healthy(base_b, 15)):
        return
    tag = f"x-{os.getpid()}-{int(time.time()*1000)}"
    r1 = _post(base_a, "/api/records",
               {"kind": "raw", "ext_id": f"{tag}-r1",
                "payload": {"value": tag + "/r1"}}).json()
    r2 = _post(base_a, "/api/records",
               {"kind": "raw", "ext_id": f"{tag}-r2",
                "payload": {"value": tag + "/r2"}}).json()
    d = _post(base_a, "/api/records", {
        "kind": "derived", "ext_id": f"{tag}-d",
        "payload": {"value": tag + "/d"},
        "parent_ids": [r1["id"], r2["id"]]}).json()
    pack = _post(base_a, f"/api/records/{d['id']}/export", {}).json()
    rep.check("外部导出含闭包与摘要",
              len(pack["records"]) == 3 and bool(pack["digest"]))

    first = _post(base_b, "/api/packages/import", pack)
    rep.check("外部接入成功", first.status_code == 200, first.text[:200])
    mapping = {m["ext_id"]: m["record_id"]
               for m in first.json()["mapping"]}
    rep.check("外部映射稳定（沿用来源外部标识）",
              set(mapping) == {f"{tag}-r1", f"{tag}-r2", f"{tag}-d"})

    again = _post(base_b, "/api/packages/import", pack)
    rep.check("外部重复接入返回首次映射",
              again.status_code == 200
              and again.json()["replayed"] is True
              and again.json()["root_record_id"] ==
              first.json()["root_record_id"])

    import copy, hashlib
    tampered = copy.deepcopy(pack)
    tampered["records"][0]["payload"] = {"value": "EVIL"}
    dig_bad = _post(base_b, "/api/packages/import", tampered)
    rep.check("外部篡改包摘要不符被拒",
              dig_bad.status_code == 422
              and dig_bad.json()["error"]["code"] == "DIGEST_MISMATCH")
    canon = {"format": tampered["format"],
             "root_ext_id": tampered["root_ext_id"],
             "records": sorted(tampered["records"],
                               key=lambda r: r["ext_id"])}
    tampered["digest"] = hashlib.sha256(
        json.dumps(canon, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode()).hexdigest()
    conflict = _post(base_b, "/api/packages/import", tampered)
    rep.check("外部同包标识不同载荷冲突",
              conflict.status_code == 409
              and conflict.json()["error"]["code"] == "PACKAGE_CONFLICT")

    inv = _post(base_b, f"/api/records/{mapping[f'{tag}-r1']}/invalidate",
                {"operation_id": f"{tag}-kill"})
    cascaded = {c["id"] for c in inv.json()["cascade"]}
    rep.check("外部谱系接入后级联失效正确",
              cascaded == {mapping[f"{tag}-r1"], mapping[f"{tag}-d"]},
              f"cascade={sorted(cascaded)}")


def main() -> int:
    print("低温探测器标定谱系 —— 一次性验收 verify")
    print(f"工作目录：{ROOT}")
    rep = Report()
    run_code_phase(rep)
    with tempfile.TemporaryDirectory() as tmp:
        try:
            run_self_hosted_phase(rep, Path(tmp))
        except Exception as exc:  # noqa: BLE001
            rep.step("HTTP 冒烟执行", False, f"{type(exc).__name__}: {exc}")
        try:
            run_handoff_phase(rep, Path(tmp))
        except Exception as exc:  # noqa: BLE001
            rep.step("密封转交包冒烟执行", False,
                     f"{type(exc).__name__}: {exc}")
    external = os.environ.get("VERIFY_TARGET_URL")
    if external:
        try:
            run_external_phase(rep, external.rstrip("/"))
        except Exception as exc:  # noqa: BLE001
            rep.step("外部服务冒烟", False, f"{type(exc).__name__}: {exc}")
    handoff_target = os.environ.get("HANDOFF_TARGET_URL")
    if external and handoff_target:
        try:
            run_external_handoff(rep, external.rstrip("/"),
                                 handoff_target.rstrip("/"))
        except Exception as exc:  # noqa: BLE001
            rep.step("外部双谱系转交冒烟", False,
                     f"{type(exc).__name__}: {exc}")

    print("\n================ 验收汇总 ================")
    for status, name, detail in rep.items:
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    print("==========================================")
    print("验收结果：" + ("全部通过 ✅" if rep.ok else "存在失败 ❌"))
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
