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

错误响应形如：

```json
{"error": {"code": "PARENT_INVALID", "message": "…",
           "details": {"invalid_parent_ids": ["R000001"]}}}
```

错误码：`PARENT_NOT_FOUND` / `SELF_REFERENCE` / `CYCLE_DETECTED` /
`PARENT_INVALID` / `RECORD_NOT_FOUND` / `RECORD_ALREADY_INVALID` /
`OPERATION_CONFLICT` / `OPERATION_ID_REQUIRED` 等。

## 快速开始（宿主机）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./verify            # 一次性验收；退出码 0/1
.venv/bin/python -m app   # 启动页面与服务，默认 http://localhost:8080
```

## Docker Compose

```bash
# 启动页面与服务（宿主机端口可配置）
HOST_PORT=9090 docker compose up -d --build
curl http://localhost:9090/health

# 一次性验收服务 verify：复现级联失效与并发竞争不变量，
# 完成代码测试、构建检查及 API/HTTP 冒烟后退出，以退出码报告结果
docker compose --profile verify run --rm verify
```

`verify` 服务自带独立数据库做完整四阶段验收（pytest → compileall/工厂导入 →
真实 gunicorn x4 worker 的 HTTP 全链路与跨进程并发竞争 → 重启持久化），
并通过 `VERIFY_TARGET_URL` 对 compose 中的 `web` 服务追加一次真实 HTTP 冒烟。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8080` | Compose 映射到宿主机的端口 |
| `CALIBRATION_PORT` | `8080` | 容器内监听端口 |
| `CALIBRATION_DB` | 仓库下 `data/calibration.db` | SQLite 路径（容器内 `/data/calibration.db`，命名卷持久化） |
| `VERIFY_PORT` | `18080` | `verify` 自管 HTTP 服务端口 |
| `VERIFY_TARGET_URL` | — | 设置后对该已运行服务追加冒烟 |

## 测试

```bash
.venv/bin/pytest -q          # 21 个单元/接口用例
./verify                     # 一次性验收（含跨进程并发与重启）
```

## 关键实现位置

- `app/store.py`：单事务级联失效（递归 CTE 求下游闭包）、操作标识幂等/冲突、
  四类引用校验、`BEGIN IMMEDIATE` 串行化、完整性自检。
- `app/server.py`：页面、健康端点与 JSON API、统一可定位错误体。
- `scripts/verify.py` / `verify`：一次性验收服务。
- `tests/`：存储层与 HTTP 接口用例（含 60+ 线程并发竞争与重启持久化）。
