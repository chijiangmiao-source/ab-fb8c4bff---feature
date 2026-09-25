#!/usr/bin/env python3
"""一次性验收服务 verify。

在低温探测器标定谱系这一真实业务场景下依次执行：

  1. 代码测试：pytest 全量单元/接口用例（级联失效、幂等/冲突、引用校验、
     并发竞争、重启持久化）；
  2. 构建检查：compileall 语法构建 + 应用可导入；
  3. API/HTTP 冒烟：拉起真实 gunicorn 服务（4 worker，跨进程竞争），
     经 HTTP 复现稳定编号、级联失效、操作重放、同标识换目标冲突、
     可定位错误反馈、并发竞争不变量，以及重启后谱系/失效状态/操作重放；
  3b. 密封转交包：对两套独立谱系依次执行导出、接入、重复接入及篡改包
     提交，观察映射稳定、原子拒绝、同标识冲突、失效依据拒绝、既有级联
     失效与跨进程并发交接不变量；
  4. 可选：若设置 VERIFY_TARGET_URL，则对已运行的服务（如 compose 中的
     web 服务）追加一次真实 HTTP 冒烟。

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

    print("\n=== 阶段 4b：密封转交包 —— 两套独立谱系交接 ===")
    port_a = port
    port_b = port + 1
    srv_a = Server(str(workdir / "lineage-a.db"), port_a, workers=2)
    srv_b = Server(str(workdir / "lineage-b.db"), port_b, workers=2)
    srv_a.start()
    srv_b.start()
    try:
        _http_package_handover(rep, workdir,
                               f"http://127.0.0.1:{port_a}",
                               f"http://127.0.0.1:{port_b}")
    finally:
        srv_a.stop()
        srv_b.stop()


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
# 阶段 4b：密封转交包，两套独立谱系交接
# --------------------------------------------------------------------------- #
def _http_package_handover(rep: Report, workdir: Path,
                           base_a: str, base_b: str) -> None:
    import copy

    # 外场谱系 A：原始 -> 多层推导，形成仍有效的结论
    a1 = _post(base_a, "/api/records",
               {"kind": "raw",
                "payload": {"value": "77K/ch-A", "unit": "mV"}}).json()
    a2 = _post(base_a, "/api/records",
               {"kind": "raw", "payload": {"value": "4K/ch-B"}}).json()
    a3 = _post(base_a, "/api/records", {
        "kind": "derived", "payload": {"value": "gain"},
        "parent_ids": [a1["id"], a2["id"]]}).json()
    a4 = _post(base_a, "/api/records", {
        "kind": "derived", "payload": {"value": "final-calibration"},
        "parent_ids": [a3["id"]]}).json()

    # ---- 导出：仍有效结论及其直接依据闭包 ---- #
    exported = _post(base_a, f"/api/records/{a4['id']}/export", {}).json()
    env = exported["package"]
    rep.check("导出包含包标识与规范摘要",
              env["package_id"].startswith("PKG-")
              and env["payload_digest"].startswith("sha256:")
              and exported["record_count"] == 4
              and exported["root_parent_ids"] == [a3["id"]],
              f"package_id={env['package_id']}")

    # B 谱系先有一条本地既有结论
    pre = _post(base_b, "/api/records",
                {"kind": "raw", "payload": {"value": "b-existing"}}).json()
    assert pre["id"] == "R000001"

    # ---- 接入：同一提交建立映射并写入全部记录 ---- #
    imp = _post(base_b, "/api/packages/import", env)
    body = imp.json()
    rep.check("首次接入 201，导入后根有本地编号且直接依据为本地编号",
              imp.status_code == 201
              and body["root_record_id"] == "R000005"
              and body["record_count"] == 4
              and requests.get(
                  f"{base_b}/api/records/{body['root_record_id']}"
              ).json()["parent_ids"] == ["R000004"])
    first_root = body["root_record_id"]
    rep.check("接入不改变本地既有结论",
              requests.get(f"{base_b}/api/records/R000001").json()
              ["payload"] == {"value": "b-existing"})

    # ---- 重复接入完全相同的包：返回首次映射 ---- #
    again = _post(base_b, "/api/packages/import", copy.deepcopy(env)).json()
    rep.check("重复接入返回首次映射（replayed，映射稳定）",
              again.get("replayed") is True
              and again["root_record_id"] == first_root
              and len(requests.get(f"{base_b}/api/records").json()) == 5)
    pq = requests.get(f"{base_b}/api/packages/{env['package_id']}").json()
    rep.check("包映射可按包标识查询", pq["root_record_id"] == first_root)

    # ---- 同包标识、不同载荷：冲突，状态不变 ---- #
    other = _post(base_a, f"/api/records/{a2['id']}/export", {}).json()["package"]
    other["package_id"] = env["package_id"]
    conflict = _post(base_b, "/api/packages/import", other)
    rep.check("同一包标识载荷不同 -> 409 PACKAGE_CONFLICT 且无新写入",
              conflict.status_code == 409
              and conflict.json()["error"]["code"] == "PACKAGE_CONFLICT"
              and len(requests.get(f"{base_b}/api/records").json()) == 5)

    # ---- 篡改包提交：摘要不符 / 删祖先 / 改载荷，逐一原子拒绝 ---- #
    tamper = copy.deepcopy(env)
    tamper["payload_digest"] = "sha256:" + "0" * 64
    r = _post(base_b, "/api/packages/import", tamper)
    rep.check("篡改摘要被定位拒绝（PACKAGE_DIGEST_MISMATCH）",
              r.status_code == 422
              and r.json()["error"]["code"] == "PACKAGE_DIGEST_MISMATCH")

    tamper2 = copy.deepcopy(env)
    tamper2["records"][-1]["payload"] = {"value": "HACKED"}
    r = _post(base_b, "/api/packages/import", tamper2)
    rep.check("篡改载荷被 Merkle 外部标识校验拒绝",
              r.json()["error"]["code"] == "PACKAGE_EXTERNAL_ID_MISMATCH")

    ancestor = next(x["external_id"] for x in env["records"]
                    if not x["parent_external_ids"]
                    and x["external_id"] != env["root_external_id"])
    thinned = copy.deepcopy(env)
    thinned["records"] = [x for x in thinned["records"]
                          if x["external_id"] != ancestor]
    r = _post(base_b, "/api/packages/import", thinned)
    rep.check("缺失祖先被定位拒绝（PACKAGE_MISSING_ANCESTOR）",
              r.status_code == 422
              and r.json()["error"]["code"] == "PACKAGE_MISSING_ANCESTOR"
              and ancestor in
              r.json()["error"]["details"]["missing_external_ids"])
    rep.check("全部篡改提交后记录数仍为 5（原子拒绝）",
              len(requests.get(f"{base_b}/api/records").json()) == 5)

    # ---- 已失效依据：B 中失效导入的共享祖先后，级联与拒绝 ---- #
    raw_pkg = _post(base_a, f"/api/records/{a1['id']}/export", {}).json()["package"]
    # 不假设拓扑落库顺序：从首次接入映射按外部标识查出 a1 的本地编号
    a1_local = next(m["record_id"] for m in body["mapping"]
                    if m["external_id"] == raw_pkg["root_external_id"])
    inv = _post(base_b, f"/api/records/{a1_local}/invalidate",
                {"operation_id": "verify-pkg-invalid-basis"})
    inv_ids = {c["id"] for c in inv.json()["cascade"]}
    rep.check("既有级联失效正确：失效共享祖先后导入的根结论一并失效",
              first_root in inv_ids and "R000004" in inv_ids
              and requests.get(
                  f"{base_b}/api/records/{first_root}").json()["status"]
              == "invalid")
    # 此后再次接入根已落在失效依据上的包：定位拒绝
    r = _post(base_b, "/api/packages/import", copy.deepcopy(raw_pkg))
    rep.check("已失效依据的接入被定位拒绝（PACKAGE_BASIS_INVALID）",
              r.status_code == 422
              and r.json()["error"]["code"] == "PACKAGE_BASIS_INVALID")

    # 有效记录不得依赖失效依据（跨进程自检由记录关系推导）
    records = requests.get(f"{base_b}/api/records").json()
    by_id = {x["id"]: x for x in records}
    bad = [x["id"] for x in records
           if x["status"] == "valid"
           and any(by_id.get(p, {}).get("status") == "invalid"
                   for p in x["parent_ids"])]
    rep.check("交接与失效后不存在有效记录指向失效依据", not bad,
              f"违规：{bad}" if bad else "")

    # ---- 跨进程并发：4 worker 下并发接入同一包，以及接入与失效竞争 ---- #
    port_c = int(base_b.rsplit(":", 1)[1]) + 1
    srv_c = Server(str(workdir / "lineage-conc.db"), port_c, workers=4)
    srv_c.start()
    base_c = f"http://127.0.0.1:{port_c}"
    try:
        # 先接入一次，建立共享依据的本地映射
        first = _post(base_c, "/api/packages/import", env).json()
        basis_local = next(
            m["record_id"] for m in first["mapping"]
            if m["external_id"] == raw_pkg["root_external_id"])
        errors: list[str] = []

        def one(i: int) -> None:
            s = requests.Session()
            try:
                if i % 2 == 0:
                    s.post(f"{base_c}/api/records/{basis_local}/invalidate",
                           json={"operation_id": f"verify-conc-inv-{i}"},
                           timeout=15)
                else:
                    s.post(f"{base_c}/api/packages/import",
                          json=copy.deepcopy(env), timeout=15)
            except requests.RequestException as exc:
                errors.append(str(exc))

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(one, range(48)))
        rep.check("并发交接期间无传输层错误", not errors, str(errors))

        recs_c = requests.get(f"{base_c}/api/records").json()
        cmap = {x["id"]: x for x in recs_c}
        orphan = [x["id"] for x in recs_c
                  if x["status"] == "valid"
                  and any(cmap.get(p, {}).get("status") in (None, "invalid")
                          for p in x["parent_ids"])]
        rep.check("并发接入后不得出现有效记录指向不存在或失效依据",
                  not orphan, f"违规：{orphan}" if orphan else "")
        # 同包并发接入只产生首次映射的那一份闭包（4 条记录）
        rep.check("并发同包接入映射唯一（只落一份闭包）",
                  len(recs_c) == 4, f"实际记录数 {len(recs_c)}")
    finally:
        srv_c.stop()


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
    external = os.environ.get("VERIFY_TARGET_URL")
    if external:
        try:
            run_external_phase(rep, external.rstrip("/"))
        except Exception as exc:  # noqa: BLE001
            rep.step("外部服务冒烟", False, f"{type(exc).__name__}: {exc}")

    print("\n================ 验收汇总 ================")
    for status, name, detail in rep.items:
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    print("==========================================")
    print("验收结果：" + ("全部通过 ✅" if rep.ok else "存在失败 ❌"))
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
