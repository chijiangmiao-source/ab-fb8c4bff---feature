# 低温探测器标定谱系服务

一条原始读数失真时，需要立即复核并失效全部受影响的下游结论，而不只是标记源记录。
本服务管理低温探测器的**原始 / 推导标定记录**、它们的**依据谱系**以及**级联失效裁决**。

## 业务规则

- 用户可建立**原始记录**（传感器读数，无前序）或**推导记录**（可选一个或多个
  当前有效的前序记录作为直接依据）。
- 提交后页面经真实接口展示**稳定编号**（`R000001…`）、**有效性**和**直接依据**。
- 对任一记录发起**携带操作标识**的失效裁决后，系统在**同一持久化提交**中使该记录
  及全部可达下游记录失效，并显示**稳定的失效来源**（裁决目标编号）。
- **幂等**：重复同一裁决（同操作标识 + 同目标）返回首次结果（`replayed=true`）。
- **冲突**：同一操作标识改换目标 → `409 OPERATION_CONFLICT`，且不改变任何状态。
- 引用不存在记录、自引用、成环或引用已失效记录 → 整笔拒绝，**既有可用结论不变**，
  并返回带定位信息（`details`）的错误码。
- **并发不变量**：新推导与失效裁决竞争后，不存在有效记录依赖失效记录
  （所有写事务经 `BEGIN IMMEDIATE` + 进程内锁串行化，校验与写入在同一事务）。
- **重启持久化**：谱系、失效状态和操作重放结果存于 SQLite，重启后仍可查询。

## 密封转交包（外场标定组 → 本地谱系）

外场组把一条**仍有效的结论**连同其**完整有效依据（直接依据闭包）**转交给本地谱系：

- 工程师在页面选定结论后**导出密封转交包**：服务按该结论直接依据的祖先闭包生成
  **规范载荷与 SHA-256 摘要**；包内记录一律按**稳定外部标识 `ext_id`** 引用，
  不含任何实例本地编号（`R000001…`）。包标识 `package_id` 仅由根外部标识派生，
  与载荷摘要相互独立。
- 工程师**粘贴另一实例的包接入**：服务在**同一持久化提交**中先做全部校验，
  再为整包建立外部标识→本地编号映射并写入全部记录。
- 接入校验（任一不过即**定位拒绝、整笔回滚，本地既有结论不变**）：
  缺失祖先（`MISSING_ANCESTOR`）、包内重复外部标识（`DUPLICATE_EXT_ID`）、
  摘要不符（`DIGEST_MISMATCH`）、成环（`CYCLE_DETECTED`）、
  引用本地已失效依据（`PARENT_INVALID`）、格式/形状错误、自根不可达记录等。
- **幂等**：重复接入完全相同的包返回**首次映射**（`replayed=true`），不新增记录。
- **冲突**：同一 `package_id` 但载荷摘要不同 → `409 PACKAGE_CONFLICT`。
- 共享祖先复用：接入更大闭包时，内容一致的已映射有效祖先直接复用；
  若该祖先在本地已失效则定位拒绝。
- **并发不变量**：接入与失效裁决竞争后，不存在有效记录指向不存在或失效依据
  （接入同样经 `BEGIN IMMEDIATE` + 进程内锁串行化，校验与写入同事务）。
- 每条记录出生即带跨实例稳定 `ext_id`（UUID），本地编号与外部标识解耦。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 谱系管理页面 |
| GET | `/health` | 健康端点（真实读库探活） |
| POST | `/api/records` | 建立原始/推导记录 |
| GET | `/api/records` | 全部记录（编号/有效性/直接依据） |
| GET | `/api/records/<id>` | 单条记录 |
| POST | `/api/records/<id>/invalidate` | 失效裁决（请求体含 `operation_id`） |
| GET | `/api/operations/<operation_id>` | 查询裁决首次结果 |
| POST | `/api/records/<id>/export` | 导出该结论的密封转交包（祖先闭包+摘要） |
| POST | `/api/packages/import` | 接入密封转交包（包对象，或 `{"package": …}`） |
| GET | `/api/imports/<package_id>` | 查询某转交包首次接入映射 |

错误响应形如：

```json
{"error": {"code": "PARENT_INVALID", "message": "…",
           "details": {"invalid_parent_ids": ["R000001"]}}}
```

错误码：`PARENT_NOT_FOUND` / `SELF_REFERENCE` / `CYCLE_DETECTED` /
`PARENT_INVALID` / `RECORD_NOT_FOUND` / `RECORD_ALREADY_INVALID` /
`OPERATION_CONFLICT` / `OPERATION_ID_REQUIRED`；转交包：
`PACKAGE_ROOT_INVALID` / `INVALID_PACKAGE` / `UNSUPPORTED_PACKAGE_FORMAT` /
`DIGEST_MISMATCH` / `PACKAGE_ID_MISMATCH` / `DUPLICATE_EXT_ID` /
`MISSING_ANCESTOR` / `DUPLICATE_PARENT` / `UNREACHABLE_RECORDS` /
`PACKAGE_CONFLICT` / `EXT_CONTENT_CONFLICT`。

转交包示例（实际为紧凑 JSON）：

```json
{
  "format": "calibration-handoff/v1",
  "package_id": "H-a1b2c3…",
  "root_ext_id": "<根结论外部标识>",
  "records": [
    {"ext_id": "…", "kind": "raw", "payload": {}, "parent_ext_ids": []},
    {"ext_id": "…", "kind": "derived", "payload": {},
     "parent_ext_ids": ["<祖先外部标识>"]}
  ],
  "digest": "<sha256，对排序后规范载荷计算>"
}
```

接入成功返回：

```json
{"result": "imported", "replayed": false,
 "package_id": "H-…", "digest": "…",
 "root_ext_id": "…", "root_record_id": "R000004",
 "direct_parent_ids": ["R000003", "R000001"],
 "reused_ext_ids": [],
 "mapping": [{"ext_id": "…", "record_id": "R000001"}]}
```

## 快速开始（宿主机）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./verify            # 一次性验收；退出码 0/1
.venv/bin/python -m app   # 启动页面与服务，默认 http://localhost:8080
```

## Docker Compose

```bash
# 启动两套独立谱系（web 外场 :8080、web-b 本地 :8081；端口可配置）
HOST_PORT=9090 HOST_PORT_B=9091 docker compose up -d --build
curl http://localhost:9090/health
curl http://localhost:9091/health

# 一次性验收服务 verify：复现级联失效、并发竞争与两套谱系间密封转交，
# 完成代码测试、构建检查及 API/HTTP 冒烟后退出，以退出码报告结果
docker compose --profile verify run --rm verify
```

`verify` 服务做完整验收（pytest → compileall/工厂导入 →
真实 gunicorn x4 worker 的 HTTP 全链路与跨进程并发竞争 → 重启持久化 →
**两套独立谱系依次导出 / 接入 / 重复接入 / 篡改包提交**），
并通过 `VERIFY_TARGET_URL`（外场 `web`）与 `HANDOFF_TARGET_URL`
（本地 `web-b`）对 compose 中两套服务追加真实 HTTP 转交冒烟。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8080` | Compose 中外场 `web` 映射到宿主机的端口 |
| `HOST_PORT_B` | `8081` | Compose 中本地 `web-b` 映射到宿主机的端口 |
| `CALIBRATION_PORT` | `8080` | 容器内监听端口 |
| `CALIBRATION_DB` | 仓库下 `data/calibration.db` | SQLite 路径（容器内 `/data/calibration.db`，命名卷持久化） |
| `VERIFY_PORT` | `18080` | `verify` 自管 HTTP 服务端口 |
| `VERIFY_TARGET_URL` | — | 设置后对该已运行服务追加冒烟（外部转交的外场端） |
| `HANDOFF_TARGET_URL` | — | 外部转交冒烟的本地端（与 `VERIFY_TARGET_URL` 成对） |

## 测试

```bash
.venv/bin/pytest -q          # 52 个单元/接口用例
./verify                     # 一次性验收（含跨进程并发、重启与双谱系转交）
```

## 关键实现位置

- `app/package.py`：转交包规范载荷/SHA-256 摘要、包标识派生、结构/闭包/
  重复标识/成环/摘要等纯函数校验。
- `app/store.py`：单事务级联失效（递归 CTE 求下游闭包）、操作标识幂等/冲突、
  四类引用校验、`BEGIN IMMEDIATE` 串行化、祖先闭包导出、接入单事务
  （建 `ext_mapping` 映射 + 写全部记录 + `imports` 登记）、完整性自检。
- `app/server.py`：页面、健康端点与 JSON API（含导出/接入/映射查询）、
  统一可定位错误体。
- `scripts/verify.py` / `verify`：一次性验收服务（含两套独立谱系转交）。
- `tests/`：存储层与 HTTP 接口用例（含转交、并发竞争与重启持久化）。
