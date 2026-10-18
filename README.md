# Agentic Kit Worker

独立部署的 Agent 执行进程。从 PostgreSQL 领取任务，用 Eino ADK `ChatModelAgent + Runner` 执行单 Agent ReAct。Eino 管理模型／工具循环、消息上下文和 checkpoint；平台保存 checkpoint、执行权与工具结果，进程重启后由其他实例接管。

与 `agentic-kit-api v0.5.3-0.20261017191628-5f8a296bfacc`（main 提交 `5f8a296`）配套，Worker 在 `go.mod` 中固定引用。使用 Go 1.27.1、server-go v0.9.0，Eino 保持 v0.9.2。两个仓库可独立克隆和构建。运行方式、模块认证与可选 go work 联合开发见 [API 启动说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/README.md)。

## 项目结构

入口位于 `cmd/worker`，server-go 生命周期与资源装配位于 `internal/service`，执行器位于 `internal/engine`，模型与工具适配位于 `internal/adapters`。数据库访问统一由 API 的 `pkg/durable` 使用 GORM 管理，worker 复用其中的事务和状态操作；[目录与依赖说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/layout.md)。

## Agent 架构与执行规则

发布配置中的提示词、模型、工具和轮次上限装配为一个 Eino `ChatModelAgent`。当前架构是单 Agent ReAct；模型依据任务与工具结果逐步决定下一步。没有独立 Planner/Executor，也没有多 Agent 编排。工具的 `returnDirectly` 与 EndRun 由执行边界按结果处理：错误反馈继续交给模型，成功结果提交后通过内部终止响应结束本轮，不再调用模型，也不额外消耗 `MaxTurns` 的真实模型预算。`internal/engine/engine.go` 创建 Agent 和 Runner，消费事件并处理恢复，不再维护另一套模型／工具循环。

1. 平台从数据库领取任务并定期续期；失去执行权后取消本地执行。
2. `boundaries.go` 的模型 middleware 记录模型调用，给工具调用分配稳定编号。每批工具执行前通过 Eino 原生工具中断创建 checkpoint。
3. `checkpoint.go` 实现 Eino `CheckPointStore`。Eino 负责 checkpoint 的编解码；平台在同一事务保存 checkpoint 与模型决策，再由 `Runner.Resume` 继续执行工具。
4. 工具 middleware 先记录 Started 意图，再调用实际工具。工具结果、公开消息和当前 checkpoint 引用一起提交。崩溃后 Eino 从 checkpoint 恢复，middleware 向 Eino 返回已经保存的工具结果，不重复调用外部工具。
5. 只读工具允许重试；幂等工具使用固定操作编号；结果不明确的 Unsafe 工具进入 NeedsReview。澄清工具进入 WaitingClarification。两者都保存 Eino checkpoint，人工结果提交后重新排队并恢复。
6. 同轮工具按模型返回顺序执行。前一个工具等待确认或失败时，后面的工具不产生副作用。

`engine.Build` 继续接收 Eino `ToolCallingChatModel` 与 `InvokableTool`；远端工具使用通用 HTTP POST：URL 为完整调用地址，直接发送模型生成的 JSON 参数并返回响应正文。工具类型仅接受 `HTTP` 或空值，其他类型在装配时拒绝。当前使用完整 Generate，SSE 展示步骤进度与完整结果，不逐 token 推送。

### 升级边界

执行状态使用 version 3，其中 `checkpoint` 是 Eino 生成的原生数据，直返步骤与最终输出随工具结果持久化。旧 version 1/2 的活动状态不转换、不从头执行，新版会明确拒绝。切换 Worker 前，须让旧 Worker 完成或取消所有旧格式的活动、等待澄清和待确认运行；保留历史记录，新业务轮次创建新的 Run。不要将新旧 Worker 同时接入执行队列。当前 Eino 版本固定为 `v0.9.2`，后续升级要再次验证已有 checkpoint 的恢复。

## 配置

应用配置位于 [`conf/server/config.toml`](conf/server/config.toml)。从仓库根目录启动前，设置指向 API 所用 PostgreSQL 的 `DATABASE_URL`；该值必填，配置文件不提供默认连接地址或密码：

```sh
export DATABASE_URL='postgres://<user>:<password>@<host>:5432/<database>?sslmode=require'
go run ./cmd/worker -log-config conf/server/logger.toml
```

应用通过 server-go v0.9.0 的 `app.Run` 加载一份配置快照。配置路径按 `-config`、`CONFIG_FILE`、`SERVER_CONFIG_PATH`、`conf/server/config.toml` 的顺序选取，文件必须存在。自定义文件可通过 `go run ./cmd/worker -config /absolute/path/config.toml` 指定。

应用配置支持框架的 `${env:NAME[:default]}`、`${file:/path}` 和 `${secret:...}` 占位符。环境变量通过文件中的占位符读取，不自动覆盖任意 TOML 字段。占位符缺少必需值或无法解析时，启动失败。随附配置使用以下字段：

| TOML 字段 | 占位符环境变量 | 默认值或要求 |
| --- | --- | --- |
| DB.Driver | — | PostgreSQL |
| DB.DSN | DATABASE_URL | 必填，与 API 使用相同数据库 |
| DB.AutoMigrate | — | false，启动不迁移；部署前准备数据库结构 |
| DB.MaxOpenConns / DB.MaxIdleConns | — | 20 / 5 |
| DB.ConnMaxIdleTime | — | 300 秒 |
| HTTPServer.Listen | LISTEN_ADDR | 127.0.0.1:8081 |
| RunMode | GIN_MODE | release |
| HTTPServer.ReadTimeout / WriteTimeout / IdleTimeout | — | 10000 / 10000 / 60000 毫秒 |
| Worker.Concurrency | WORKER_CONCURRENCY | 4，必须大于 0 |
| Worker.Lease | WORKER_LEASE | 30s，至少 300ms；每三分之一租约时间续期 |
| Worker.Poll | WORKER_POLL | 1s，必须大于 0 |

独立日志配置可通过 `-log-config` 或 `LOG_CONFIG_FILE` 指定，命令行参数优先；未指定时使用框架默认日志配置。随附 [`conf/server/logger.toml`](conf/server/logger.toml) 使用 `Level = "Info"` 和 Console 输出。日志文件由 `logger.ParseConfigFile` 直接解析，不支持应用配置的占位符。修改数据库、Worker 或日志配置后重启进程。

运行时由框架 `db.Module` 创建和关闭唯一的数据库连接池，`pkg/durable.New` 借用该连接，负责已有事务与状态操作。HTTPHost 的 `Start` 启动执行协程；SIGTERM 退出时先停止 HTTP 接入，再取消并等待 Worker，最后由运行时关闭数据库。框架提供 `/healthz`（服务已启动）和 `/ping`（进程存活），它们不检查数据库的持续可用性。本地默认仅监听回环地址；Docker 与 Helm 监听 `0.0.0.0:8081`，Helm 使用这两个端点探测。

Worker 不使用 Redis；事件通知由 API 的 Outbox Relay 处理。模型和工具凭据由任务快照中的 APIKeyEnv/TokenEnv 指定，通过运行环境提供，实际凭据不存数据库。Worker 构建不需要 CGO，固定依赖见 go.mod/go.sum。

API 主程序提交任务的总期限为 24 小时，包括等待澄清；默认最多 5 次连续执行尝试。模型/HTTP 工具单次请求超时为 2 分钟；终态后不再接管。修改这些值时应结合真实任务时长评估，不代表已测得的性能指标。

## 验证重启恢复

```bash
export TEST_DATABASE_URL='postgres://agentic:agentic_local@127.0.0.1:55432/agentic?sslmode=disable'
make integration
```

Eino 接入回归覆盖工具调用前已有原生 checkpoint、结果不明时暂停并接受人工确认、同轮多工具顺序、重复 provider ID、跨 checkpoint 的轮次上限、提示词原文与历史，以及旧状态拒绝执行。

`TestProcessKillResumesWithoutRepeatingTool` 会通过真实 HTTP API 提交任务，构建并启动真实 `cmd/worker` 二进制，在工具结果保存后强制杀掉该进程，再启动替代执行者。它检查 runID 不变、epoch 增加、工具调用仅一次、任务最终完成，并验证无 Redis 情况下的 SSE 回放。模型和工具服务是受控 HTTP 测试服务，不调用付费模型。

`TestWorkerHealthAndShutdown` 验证真实 Worker 启动后 `/healthz` 返回 200，并在 SIGTERM 后成功退出。独立交付验证使用 `GOWORK=off`，避免测试依赖相邻 API 源码。

[故障语义](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/design.md)

Agent 定义通过 API 管理和发布。worker 始终读取 Run 快照，编辑草稿或发布新版本不会改变正在执行的任务。v0.2 需要配套的新会话存储，升级范围见 API README；不与 v0.1 API 混用。

## Coding 工具与 Kubernetes

HTTP 工具请求由 Worker 注入 `X-Agentic-Run-ID`、`X-Agentic-Epoch`、冻结的 `X-Agentic-Scope` 和稳定 Idempotency-Key。scope 来自平台接受的应用输入，工具参数不能覆盖。工具服务应同时验证运行时执行权与业务写入资格。Coding 使用每轮独立 AIO 沙箱，平台持有 GitLab 凭据。Worker 只持有 Coding 网关凭据。

Helm Chart 位于 `charts/agentic-kit-worker`。`existingSecret` 指定已存在的 Secret，提供 `DATABASE_URL` 及任务使用的模型、工具凭据。`applicationConfig` 与 `loggerConfig` 接受原生 TOML，分别生成应用和日志 ConfigMap，以只读方式挂载；容器显式传入 `-config /conf/server/config.toml -log-config /conf/server/logger.toml`。调整日志级别或输出时直接修改 `loggerConfig` 中的 `Level`、`[[Outputs]]`。Chart 保持非 root 和只读根文件系统，默认 Console 输出；File 输出需要另行提供可写挂载。配置内容变化会触发 Pod 更新。

可在本地检查渲染结果：

```sh
helm lint charts/agentic-kit-worker --set existingSecret=worker-runtime
helm template worker charts/agentic-kit-worker --set existingSecret=worker-runtime
```

Docker 镜像使用 Go 1.27.1 构建，运行层为 scratch，包含 `conf/server/`，以 UID/GID 65532 运行；默认使用 `/conf/server/logger.toml`，可通过只读根文件系统运行。构建过程从固定的 API 模块版本下载依赖，不需要相邻 API checkout。GitHub CLI 登录后，使用 BuildKit 临时挂载私有模块认证。凭据不写入镜像层：

```sh
WORKER_GITHUB_NETRC="machine github.com login x-access-token password $(gh auth token)" \
  docker build --secret id=github_netrc,env=WORKER_GITHUB_NETRC -t agentic-kit-worker:dev .
```

## AIO 文件与命令工具

Agent 的 `toolsets:["filesystem"]` 由 `internal/adapters/aio` 适配到 Eino 原生文件工具与 Shell。官方 `sandbox-sdk-go v0.0.5` 对接固定的 AIO 1.11.0 协议。可信 ExecutionSpec 冻结 provider、URL、TokenEnv 和工作目录，普通会话提交不能提供沙箱地址。SDK 禁用写操作的隐式重试。构建工具前执行 readiness 与接管隔离。

`endRun:true` 使用 Eino `ReturnDirectly` 结束运行。参数由 JSON Schema 库校验。终止调用参数无效或与其他工具混用时，边界将其转为不可执行的原生工具错误反馈，使模型修正后再结束；不执行混合批次的副作用。该校验使用 jsonschema/v6，避免重复实现 JSON Schema 规范。
