# 本地幂等任务队列（Python / FastAPI / SQLite）

在示例服务基础上补了一条**纯本地、无外部中间件**的异步任务投递与执行链路：
任务状态全部落在 SQLite 文件里，重启后已落地的状态继续可用。

## 组成

| 模块 | 职责 |
| --- | --- |
| `app/api.py` | HTTP 接口层：提交、查询、本地消费 |
| `app/worker.py` | 执行层：领取任务、调用处理器、失败重试/终结判定、日志 |
| `app/store.py` | 状态记录层：SQLite 持久化，所有状态流转都是单条原子事务 |
| `app/models.py` / `app/errors.py` / `app/config.py` | 模型、错误类型、配置 |

## 投递与去重语义

- **幂等键去重**：`tasks.idempotency_key` 上有 `UNIQUE` 约束。提交在一个
  `BEGIN IMMEDIATE` 事务里先按幂等键查重，再插入。无论串行还是并发提交，
  同一个幂等键**最多真正执行一次**。
- 重复提交不会报错覆盖，而是返回已有任务，`replayed=true`，并给出可区分的
  `reason`：
  - `idempotent_duplicate_pending`：已有任务排队中（HTTP 200）
  - `idempotent_duplicate_in_flight`：已有任务执行中（HTTP 200）
  - `idempotent_replay_succeeded`：已成功，响应里带原始结果（HTTP 200）
  - `idempotent_replay_failed`：已终态失败，响应里带最近一次失败原因（HTTP 200）
  - 全新提交返回 `accepted`（HTTP 201）。

## 至少一次投递、重试与失败策略

- 消费时 `fetch_for_delivery()` 在一个原子事务里把任务从 `pending` 翻成
  `running`、`attempts+1` 并写入租约到期时间（`lease_expires_at`）。
  两个消费者不可能同时拿到同一个任务（进程内用锁串行化，跨进程由 SQLite
  文件锁保证）。
- **进程在处理中途被杀**：任务停留在 `running` 且租约到期后，会再次被
  `fetch_for_delivery()` 领取（这就是"至少一次"）。因此处理器应尽量写成
  幂等的。
- 处理器抛异常：`attempts < max_attempts` 时任务回到 `pending` 等待重新
  投递；`attempts >= max_attempts` 时进入终态 `failed`，并保留最近一次
  `last_error`，不会无限重试，任务也不会丢失。
- 状态流转：`pending → running → succeeded`（终态）或
  `pending → running → pending → … → failed`（终态）。

## 背压

- 待处理任务数达到 `TASK_QUEUE_MAX_PENDING` 时，新的**不同**幂等键提交会被
  明确拒绝：HTTP `503`，响应体 `detail.reason = "queue_full"`，并带当前上限。
  不会静默丢弃，也不会无限排队。
- 重复幂等键永远优先按去重处理，不占用队列容量（不会误报 queue_full）。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASK_QUEUE_DB` | `./task_queue.db` | SQLite 文件路径（自动建表，WAL 模式） |
| `TASK_QUEUE_MAX_ATTEMPTS` | `3` | 最大投递次数，达到后进入 failed |
| `TASK_QUEUE_LEASE_SECONDS` | `30` | 消费租约秒数；超时未完成视为消费者挂掉，可重新投递 |
| `TASK_QUEUE_MAX_PENDING` | `1000` | pending 队列容量上限，超过拒绝新键提交（503 queue_full） |
| `TASK_QUEUE_WORKER` | `1` | 设为 `0` 时不启动后台消费线程，只用手动消费接口 |
| `TASK_QUEUE_LOG_LEVEL` | `INFO` | 日志级别 |

## 本地验证

```bash
uv sync --group dev

# 1) 跑全部单测（每个测试用独立临时 db，可任意重复运行、互不干扰）
uv run pytest

# 2) 起服务（默认带后台消费线程）
uv run python main.py
```

另开终端：

```bash
# 提交
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","task_type":"echo","payload":{"x":1}}'

# 立刻重复提交同样的 key -> 200 + idempotent_duplicate_*，不会执行第二遍
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"k1","task_type":"echo","payload":{"x":2}}'

# 查询（按 id 或按幂等键）
curl -s http://127.0.0.1:8000/tasks/by-key/k1

# 手动消费一条（关闭后台线程 TASK_QUEUE_WORKER=0 时用）
curl -s -X POST http://127.0.0.1:8000/tasks/consume

curl -s http://127.0.0.1:8000/health
```

重启进程后再查询，`succeeded/failed/pending/running(可重投)` 等状态都还在。

## 测试覆盖

`tests/test_queue.py` 覆盖：

1. 16 线程并发重复提交同一幂等键 —— 只插入一次、处理器只执行一次；
2. 处理反复失败 —— 按 `max_attempts` 重试，次数用尽进入 `failed` 且保留
   最近错误，失败任务不再投递；
3. 租约过期模拟消费中途崩溃/重启 —— 同一任务被重新投递、投递次数递增；
4. 先失败两次后成功 —— 重试期间恢复，最终 `succeeded`；
5. 关闭重开数据库 —— 状态在"重启"后继续可用；
6. 两个消费者并发领取 —— 不会拿到同一个任务；
7. 队列打满 —— 存储层抛 `QueueFull`；
8. HTTP 层区分拒绝原因：`accepted` / 各类 `idempotent_*` / `503 queue_full`。

单测日志（`task_queue.worker` / `task_queue.api` 两个 logger）会打印
**幂等键、投递次数（delivery_count/attempts）和判定依据
（accepted / requeue-redeliver / terminal-failure / terminal-success /
queue_full 等）**。

## 内置处理器

服务自带一个 `echo` 处理器，结果为 `{"echoed": <payload>}`。新增任务类型时
在 `main.py` 里通过 `worker.register(task_type, handler)` 注册即可，handler
接收 `payload: dict`，返回可 JSON 序列化的 dict。
