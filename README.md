# idempotent-task-queue-python

基于 FastAPI + SQLite 的本地幂等任务队列服务：任务提交、查询与本地消费，
不依赖任何外部中间件，状态落盘（SQLite WAL），重启后可继续消费未完成任务。

## 快速开始

```bash
uv sync
uv run python main.py          # 监听 127.0.0.1:8000
```

提交与查询任务：

```bash
curl -X POST localhost:8000/tasks \
  -H 'content-type: application/json' \
  -d '{"idempotency_key": "order-123", "payload": "hello"}'
curl localhost:8000/tasks/order-123
```

## 投递与去重语义

- **幂等提交**：`POST /tasks` 以 `idempotency_key` 为唯一键（数据库唯一约束）。
  同一个键无论重复提交多少次、是否并发，只会创建一条任务、最多执行一次。
  重复提交返回 `200/201` 且 `deduplicated: true`，并直接带回已有记录
  （含已落定任务的 `result`），不会重复执行。
- **至少一次投递**：任务只有在处理器返回后才被标记为终态。消费到一半进程
  重启时，租约（lease）到期后任务会被自动重新入队并再次投递，因此处理器
  必须自身幂等。
- **原子状态流转**：所有状态变更都在 `BEGIN IMMEDIATE` 事务内完成；领取任务
  是单条 `UPDATE ... WHERE status='PENDING'` 的 compare-and-swap，同一任务
  不会被两个消费者同时拿到。
- **背压**：未完成任务数（PENDING + RUNNING）达到 `TASK_MAX_PENDING` 时，
  新提交返回 `429`，`detail.reason = "queue_full"`，任务不会被静默丢弃。

## 重试与失败策略

- 处理器抛异常 → 记录 `last_error`，若未达 `TASK_MAX_ATTEMPTS` 则回到
  PENDING 等待重投；达到上限后进入终态 `FAILED`，保留最近一次失败原因，
  不再投递、也不会丢失（记录仍在库中可查）。
- 状态机：`PENDING → RUNNING → SUCCEEDED / FAILED`，
  `RUNNING` 租约到期自动回到 `PENDING`（重投），失败未达上限也回到 `PENDING`。
- 任务记录字段：`attempts`（已投递次数）、`max_attempts`、`result`、
  `last_error`、`status`。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASK_DB_PATH` | `tasks.db` | SQLite 数据库文件路径 |
| `TASK_MAX_PENDING` | `1000` | 未完成任务上限，超过即拒绝新提交（429） |
| `TASK_MAX_ATTEMPTS` | `3` | 单任务最大投递次数，耗尽进入 FAILED |
| `TASK_LEASE_SECONDS` | `30.0` | 消费租约时长，到期未 ack 自动重投 |
| `TASK_WORKER_COUNT` | `2` | 本地消费者线程数 |
| `TASK_POLL_INTERVAL` | `0.2` | 队列空时消费者的轮询间隔（秒） |

## 本地验证

```bash
uv run pytest        # 7 个用例，日志含幂等键、投递次数与判定依据，可重复运行
```

覆盖场景：并发重复提交只执行一次、消费中途崩溃/重启后重投、重试耗尽进入
FAILED 并保留失败原因、队列满返回 429 且原因可区分、已完成任务重复提交
直接返回已有结果。

手动验证重启恢复：

```bash
TASK_DB_PATH=/tmp/tq.db uv run python main.py &
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key":"k1","payload":"x"}'
kill -9 $!                                  # 模拟崩溃
TASK_DB_PATH=/tmp/tq.db uv run python main.py &   # 重启后未完成任务继续被消费
curl localhost:8000/tasks/k1
```

## 代码结构

- `app/config.py` — 环境变量配置
- `app/store.py` — SQLite 持久化与原子状态流转（提交去重、租约领取、ack/重试）
- `app/worker.py` — 本地消费者循环（租约、重试、优雅停止）
- `app/service.py` — store 与消费者池的组装与生命周期
- `app/api.py` — FastAPI 接口层（提交 / 查询 / 背压拒绝）
- `tests/test_queue.py` — 并发去重、崩溃重投、重试上限、背压拒绝等用例
