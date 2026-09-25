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

## 密封转交包（外场 → 本地谱系）

- 在页面选定一条**仍有效**结论即可导出**密封转交包**：包内包含该结论及其
  **直接依据闭包**（全部祖先），记录一律按**稳定外部标识**互相引用，与本地
  编号无关。
- 外部标识采用 **Merkle 内容寻址**
  （`X + sha256(类型 + 规范载荷 + 直接依据外部标识序列)`），因此两套独立谱系中
  内容与依据相同的记录必然得到相同标识，且标识自证身份。
- **规范载荷与摘要**：canonical JSON（键排序、紧凑分隔）；`payload_digest`
  覆盖根与闭包全部记录，`package_id` 为包标识。
- **接入**在**同一持久化提交**中为整包建立外部标识→本地编号映射并写入全部记录；
  导入后返回本地编号与直接依据。
  - 重复接入完全相同的包（同摘要）→ 返回首次映射（`replayed=true`）；
  - 同一包标识、不同载荷（摘要）→ `409 PACKAGE_CONFLICT`，状态不变；
  - 缺失祖先、重复外部标识、摘要不符、外部标识不符、环、包内游离记录、
    依据在本地已失效 → 整笔**原子拒绝**并给出 `details` 定位；
  - 接入**不改变本地既有结论**；共享的有效祖先复用首次映射，接入后的谱系
    仍参与原有级联失效裁决。

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
| POST | `/api/records/<id>/export` | 导出仍有效结论的密封转交包 |
| POST | `/api/packages/import` | 接入转交包（裸包或 `{"package": …}`） |
| GET | `/api/packages/<package_id>` | 查询首次接入映射 |

错误响应形如：

```json
{"error": {"code": "PARENT_INVALID", "message": "…",
           "details": {"invalid_parent_ids": ["R000001"]}}}
```

记录错误码：`PARENT_NOT_FOUND` / `SELF_REFERENCE` / `CYCLE_DETECTED` /
`PARENT_INVALID` / `RECORD_NOT_FOUND` / `RECORD_ALREADY_INVALID` /
`OPERATION_CONFLICT` / `OPERATION_ID_REQUIRED` 等。

转交包错误码：`PACKAGE_MALFORMED` / `PACKAGE_MISSING_ANCESTOR` /
`PACKAGE_DUPLICATE_EXTERNAL_ID` / `PACKAGE_CYCLE_DETECTED` /
`PACKAGE_EXTERNAL_ID_MISMATCH` / `PACKAGE_DIGEST_MISMATCH` /
`PACKAGE_ROOT_MISSING` / `PACKAGE_ROOT_NOT_VALID` /
`PACKAGE_DISCONNECTED_RECORD` / `PACKAGE_CONFLICT` /
`PACKAGE_BASIS_INVALID` / `PACKAGE_NOT_FOUND`。

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
.venv/bin/pytest -q          # 单元/接口用例（创建/查询/失效 + 转交包导出/接入）
./verify                     # 一次性验收（含跨进程并发、双谱系交接与重启）
```

## 关键实现位置

- `app/packages.py`：规范载荷、Merkle 稳定外部标识、包摘要与全部纯函数校验
  （缺失祖先 / 重复标识 / 环 / 标识不符 / 摘要不符 / 游离记录）。
- `app/store.py`：单事务级联失效（递归 CTE 求下游闭包）、操作标识幂等/冲突、
  四类引用校验、`BEGIN IMMEDIATE` 串行化、完整性自检；密封转交包的导出闭包、
  单事务接入（外部标识映射 + 全部记录）、同包幂等与同标识冲突。
- `app/errors.py`：可定位错误体（供 store 与 packages 共用，避免循环导入）。
- `app/server.py`：页面、健康端点与 JSON API、统一可定位错误体。
- `scripts/verify.py` / `verify`：一次性验收服务，含两套独立谱系导出 → 接入 →
  重复接入 → 篡改包提交，以及 4 worker 跨进程并发交接。
- `tests/`：存储层与 HTTP 接口用例（含转交包、60+ 线程并发竞争与重启持久化）。
