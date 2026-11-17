# 固件构建流水线 API

FastAPI + SQLite 实现的 DAG 构建流水线：接收至多 12 步的 JSON DAG，在挂载工作目录中
执行真实 shell 命令，全局最多 2 步并行，带依赖调度、取消、超时、进程组清理和重启恢复。

## 功能与语义对照

| 需求 | 实现 |
| --- | --- |
| 至多 12 步 JSON DAG，含命令/依赖/超时 | `POST /builds`，Pydantic 校验 + 重复 id / 悬空依赖 / 自依赖 / 有环检测（400/422） |
| 挂载工作目录执行真实命令 | 每构建独立目录 `$PIPELINE_DATA_DIR/workdir/<build_id>/`，compose 挂载命名卷 `/data` |
| 全局最多两步并行 | 单 worker 内 `asyncio.Semaphore(2)`，名额覆盖步骤全生命周期 |
| 依赖全成功才启动 | 调度器按 DAG 推进，依赖未全部 `succeeded` 不启动 |
| 失败使后代跳过，独立分支继续 | 上游出现失败/取消/中断即置后代 `skipped`；无依赖关系的分支不受影响 |
| 整次取消停止所有未完成步骤 | `POST /builds/{id}/cancel`，per-build 事件即时通知；未启动步骤不再启动 |
| 超时/取消终止对应进程组 | 每步 `start_new_session=True` 独立进程组，`killpg(PGID, SIGTERM→SIGKILL)` |
| 主命令退出后慢子进程清理并判失败 | 退出后扫描 `/proc` 检查组内残留，有残留即清理且步骤判 `failed` |
| 进程组清空前不释放名额/不确认终态 | 信号量在清理完成、终态落库后才释放；期间状态可见为 `cleaning_up` |
| 慢清理不阻塞另一名额 | 清理是异步等待，只占自己的名额，另一名额正常调度其他步骤/构建 |
| 重启先清理旧残留再标中断 | startup 恢复：PID+starttime 双重身份确认后终止旧组，再统一置 `interrupted` |
| 未跑步骤不再启动、不误杀后来构建 | 恢复不恢复调度；身份不符（PID 复用）的进程绝不触碰 |
| 重复取消不改终态 | 终态构建上的取消请求幂等忽略，返回说明，不写库 |
| 逐步状态与日志 | `GET /builds/{id}` 全量状态；`GET /builds/{id}/steps/{sid}/logs`（支持 `?tail=N`） |
| 两次构建产物核对 | `POST /verifications`：两个不同的成功构建号 + 工作目录内同一相对路径，返回各自大小、SHA-256 与一致结论 |
| 越界路径/符号链接不参与核对 | 路径归一化拒绝绝对路径与 `..`；从工作目录根逐分量 `O_NOFOLLOW` 打开，任一分量是符号链接即拒绝 |
| 读取期间变化/读取失败不确认 | 读取前后 `fstat` 比对 dev/ino/mode/size/mtime/ctime，任一变化或读失败即 409/4xx 且不落库 |
| 摘要与结论一同持久化 | `verifications` 表同事务写入两侧摘要与结论；`UNIQUE(build_lo, build_hi, path)` |
| 同一构建对并发只确认一份 | `INSERT OR IGNORE` 抢占，落败并发请求返回已确认的那一份（全库唯一） |
| 后续工作目录变化不改写结果 | 核对结果只插不改；已确认的构建对直接返回落库结果，不再读工作目录 |
| 重启后仍可查询 | 结果存于 SQLite（命名卷持久化），`GET /verifications/{id}` / `GET /verifications` 随时可查 |

步骤状态机：`pending → running → cleaning_up → succeeded|failed|cancelled|interrupted`，
失败后代为 `skipped`。构建终态：全部成功 `succeeded`；有失败 `failed`；
取消导致的终止 `cancelled`；重启中断 `interrupted`。

## 快速启动（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# 健康检查: http://localhost:8000/health
```

容器以 `tini` 作为 PID 1 回收孤儿/僵尸进程；数据全部落在命名卷 `pipeline-data`
（SQLite、工作目录、步骤日志），重启不丢、恢复逻辑依赖它。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
PIPELINE_DATA_DIR=./data .venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 示例构建

`examples/` 下提供三类（另含超时示例）：

- `success_build.json` — prepare → compile → unit-test → package 全成功链路
- `failure_build.json` — 编译失败、后代跳过、独立分支继续
- `slow_child_build.json` — 主命令退出 0 但派生 `sleep 300` 慢子进程，验证清理与判失败
- `timeout_build.json` — 2s 超时终止睡眠 60s 的进程组

一键演示（API 已启动时）：

```bash
.venv/bin/python examples/run_demo.py            # 默认 http://127.0.0.1:8000
```

手动提交：

```bash
curl -s -X POST localhost:8000/builds \
  -H 'Content-Type: application/json' \
  --data @examples/success_build.json
curl -s localhost:8000/builds/1
curl -s localhost:8000/builds/1/steps/compile/logs
curl -s -X POST localhost:8000/builds/1/cancel
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/builds` | 提交 DAG 构建（201 返回含逐步状态） |
| GET | `/builds?limit=N` | 构建列表（新→旧） |
| GET | `/builds/{id}` | 构建与全部步骤状态 |
| POST | `/builds/{id}/cancel` | 整次取消（幂等） |
| GET | `/builds/{id}/steps/{step_id}/logs?tail=N` | 步骤日志 |
| POST | `/verifications` | 发起产物核对（201 新确认 / 200 返回已确认结果） |
| GET | `/verifications?build_a=&build_b=&path=` | 核对结果列表（可按构建对/单构建/路径过滤） |
| GET | `/verifications/{id}` | 单条核对结果 |
| GET | `/health` | 健康检查与最近一次启动恢复报告 |
| GET | `/docs` | Swagger UI |

## 产物核对（可复现性校验）

固件发布工程师提交两个**不同的已成功**构建号与两侧工作目录内**同一相对路径**，
API 读取两侧普通文件，返回各自大小、SHA-256 与一致结论：

```bash
curl -s -X POST localhost:8000/verifications \
  -H 'Content-Type: application/json' \
  -d '{"build_a": 1, "build_b": 2, "path": "out/fw.bin"}'
```

语义要点：

- 响应中 `build_a`/`build_b` 按构建号**升序归一化**（`a_*` 对应较小编号一侧），
  因此 `(1,2)` 与 `(2,1)` 是同一份核对。
- 路径必须落在构建工作目录内：绝对路径、`..` 越界、空路径一律 400；
  路径上**任何分量是符号链接**都拒绝参与核对（400），即使指向目录内部。
- 读取前后各做一次元数据比对（dev/ino/mode/size/mtime/ctime），
  任一文件在**读取期间变化**或**读取失败**时返回 409/4xx，**不确认结果、不落库**。
- 两侧摘要与结论一同写入 SQLite；`UNIQUE(build_lo, build_hi, path)` 保证
  **同一构建对同一路径的并发请求只确认一份结果**，其余并发请求拿到同一条记录。
- 结果**只插不改**：确认之后工作目录再被改动，重复发起仍返回原确认结果（200），
  重启服务后仍可通过 `GET /verifications/{id}` 查询。

## 进程组与重启恢复的关键设计

1. 每个步骤由 `/bin/sh -c <command>` 在新会话中启动，主命令为进程组组长；
   命令派生出的所有子进程默认同组，可被 `killpg` 一次性覆盖。
2. 步骤终态确认前必须满足：`/proc` 扫描该 PGID 已无任何非僵尸成员。
   清理序列为 SIGTERM 宽限（默认 5s）→ SIGKILL → 硬等排空（默认 60s 有界）。
3. 主命令退出码为 0 但组内仍有成员时，清理后步骤判 `failed`，杜绝"误发包"。
4. SQLite 中记录每步的 `pid / pgid / proc_starttime`。重启恢复时先校验
   `/proc/<pid>/stat` 的 starttime 与 pgid 均匹配才终止；PID 已被复用的
   后来构建进程不会被误杀。组长已死但组内仍有成员时，因 PGID 不可能被复用，
   残留成员仍会被清理。
5. 恢复在应用开始接受请求前完成；旧构建统一置 `interrupted`，调度不恢复。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_validation.py`：DAG 校验、12 步上限、超时正数、进程身份识别。
- `tests/test_integration.py`：以真实 uvicorn 子进程端到端验证成功、失败跳过、
  慢子进程清理判失败、超时、并行上限 2、整次取消、重复取消幂等、慢清理不占
  另一名额、重启恢复（含不误杀后来构建）、错误请求与文档接口。
- `tests/test_verification.py`：产物核对全链路 —— 一致/不一致结论、并发只确认
  一份、确认后工作目录篡改不改写结果、重启后可查询、越界路径/符号链接/目录/
  非成功构建等拒绝路径，以及路径归一化与读期间变化检测的单元测试。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PIPELINE_DATA_DIR` | `./data` | SQLite/工作目录/日志根目录 |
| `PIPELINE_MAX_STEPS` | `12` | 单构建步骤上限 |
| `PIPELINE_MAX_PARALLEL` | `2` | 全局并行名额 |
| `PIPELINE_TIMEOUT_CAP_SECONDS` | `86400` | 单步超时上限 |
| `PIPELINE_TERM_GRACE_SECONDS` | `5` | SIGTERM 宽限 |
| `PIPELINE_KILL_GRACE_SECONDS` | `60` | SIGKILL 后硬等上限 |
