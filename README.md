# Agentic Kit Worker

独立部署的 Agent 执行进程。从 PostgreSQL 领取任务，用 Eino ADK `ChatModelAgent + Runner` 执行单 Agent ReAct。Eino 管理模型／工具循环、消息上下文和 checkpoint；平台保存 checkpoint、执行权与工具结果，进程重启后由其他实例接管。

Worker 在 `go.mod` 中固定配套的 `agentic-kit-api` 版本。构建前需确保该版本能从 Go 模块源下载。当前使用 Go 1.27.1、server-go v0.17.1 和 Eino v0.9.2。运行方式、模块认证与可选 `go work` 联合开发见 [API 启动说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/README.md)。
私有 GitHub 模块的模块校验将关闭：`GONOSUMDB=github.com/FlyTOmeLight/*`（默认 Go 环境通过 `GOPRIVATE` 也能达到相同效果）。

## 项目结构

Worker 在 `boundaryModel` 中用 API 的 `context-v4` 统一构建每次模型请求，真实工具定义和 steer 一起计入预算。它用 `cjk-weighted-v1` 估算 token，并随用量保存 `UsageAnchor`。下一轮输入估算增长时，可用上一轮实际 prompt token 加上估算增量判断是否提前压缩。

接近预算时，Worker 先预览旧工具结果、缩短长参数，必要时用带步骤 ID 的短条替代旧结果。仍超过目标时，Worker 按完整工具批次保留近期消息，把较早的内容分块交给模型滚动摘要。每块沿用 Run 的冻结计价并单独记录用量。连续三次可归因的摘要失败会打开熔断器，计数随执行状态保存。摘要无法达到目标时，Worker 可从模型输入移除最早的完整轮次，保留原始目标、人工澄清或审核结果及 steer。

只有供应方明确返回上下文溢出且没有用量时，Worker 才清除该次派发标记，写入 `ModelOverflowRejected` 并缩小输入。主模型最多重试五次，达到输入下限、请求无法继续缩小或重试用完仍被拒绝时进入 `Failed`。无法确定消费的调用仍进入 `Blocked`，由人工通过 `Unblock` 恢复。

上下文压缩只改当前请求的消息投影。Eino checkpoint、`durable_step` 和 `agent_message` 保留原始事实。启用工具或工具集的 Run 可调用只读工具 `read_step_result`，通过本 Run 的步骤 ID 和字节偏移读取完整工具结果，按 UTF-8 边界分页。Run 成功后摘要和保留边界才提升到 Session，后续 Run 冻结该摘要及保留的消息。

配置 `SubAgents` 或 `SelfDelegate` 的主 Run 可通过 `delegate_subagent` 委派任务。各子 Run 使用冻结的模型和提示词，在独立会话中执行。Worker 在同一事务中创建子 Run、保存父 checkpoint 并挂起父 Run。子 Run 成功、失败或取消后，父 Run 从等待的工具步骤恢复。同轮多个委派按顺序执行。子 Run 不提供内置委派工具，调用用户澄清工具时会收到反馈并继续执行。

配置 `Skills` 的 Run（及继承或自有 `Skills` 的子 Run）在上下文中挂载冻结的 `skills` 目录摘要，并注册只读内置工具 `load_skill`。Worker 通过可选 `[ControlPlane]` 配置凭当前有效任务租约调用 API `/internal/v1/worker/runs/{id}/skills/read` 按需读取冻结清单内的 `SKILL.md` 及辅助文件，结果由 `durable_step` 存档，崩溃恢复时直接重放而不重复拉取。当前轮最新一次成功的 `load_skill`（`SKILL.md` 且不超过 16 KiB 与 `MaxToolResultBytes`）受 `Exempt` 保护；被摘要或轮次裁剪淘汰后以 `skill` PinnedBlock 携带，预算极紧时自动降级为重载提示避免触发 `ContextBudgetExceeded`。

入口位于 `cmd/worker`，server-go 生命周期与资源装配位于 `internal/service`，执行器位于 `internal/engine`，模型与工具适配位于 `internal/adapters`。数据库访问统一由 API 的 `pkg/durable` 使用 GORM 管理，worker 复用其中的事务和状态操作；[目录与依赖说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/layout.md)。

## Agent 架构与执行规则

发布配置中的提示词、模型、工具和轮次上限装配为一个 Eino `ChatModelAgent`。当前架构是单 Agent ReAct；模型依据任务与工具结果逐步决定下一步。没有独立 Planner/Executor，也没有多 Agent 编排。工具的 `returnDirectly` 与 EndRun 由执行边界按结果处理：错误反馈继续交给模型，成功结果提交后通过内部终止响应结束本轮，不再调用模型，也不额外消耗 `MaxTurns` 的真实模型预算。`internal/engine/engine.go` 创建 Agent 和 Runner，消费事件并处理恢复，不再维护另一套模型／工具循环。

1. 平台从数据库领取任务并定期续期；失去执行权后取消本地执行。
2. `boundaries.go` 的模型 middleware 记录模型调用，给工具调用分配稳定编号。每批工具执行前通过 Eino 原生工具中断创建 checkpoint。
3. `checkpoint.go` 实现 Eino `CheckPointStore`。Eino 负责 checkpoint 的编解码；平台在同一事务保存 checkpoint 与模型决策，再由 `Runner.Resume` 继续执行工具。
4. 工具 middleware 先记录 Started 意图，再调用实际工具。工具结果、公开消息和当前 checkpoint 引用一起提交。崩溃后 Eino 从 checkpoint 恢复，middleware 向 Eino 返回已经保存的工具结果，不重复调用外部工具。
5. 只读工具允许重试；幂等工具使用固定操作编号；结果不明确的 Unsafe 工具进入 NeedsReview。澄清工具进入 WaitingClarification。两者都保存 Eino checkpoint，人工结果提交后重新排队并恢复。
6. 同轮工具按模型返回顺序执行。前一个工具等待确认或失败时，后面的工具不产生副作用。

`engine.Build` 继续接收 Eino `ToolCallingChatModel` 与 `InvokableTool`；远端静态工具使用通用 HTTP POST（`kind` 仅接受 `HTTP` 或空值），另支持标准 Streamable HTTP MCP 服务（按 `mcp__<server>__<tool>` 暴露、配置驱动策略与会话自动清理）以及多模态首问公网 `https://` 媒体映射与工作区文件产物自动追踪。模型调用默认开启带 `include_usage` 的流式采集，将逐 token 思考与正文增量推送到可选 Redis 瞬态流（支持 `model.stream=false` 与首帧前 `stream_options` 400 自动回退，以及分离的首帧超时与分片间空闲超时）。静态工具与运行时装配工具总数统一限制不超过 128 个。

### 升级边界

执行状态使用 version 3，其中 `checkpoint` 是 Eino 生成的原生数据，直返步骤与最终输出随工具结果持久化。旧 version 1/2 的活动状态不转换、不从头执行，新版会明确拒绝。切换 Worker 前，须让旧 Worker 完成或取消所有旧格式的活动、等待澄清和待确认运行；保留历史记录，新业务轮次创建新的 Run。不要将新旧 Worker 同时接入执行队列。当前 Eino 版本固定为 `v0.9.2`，后续升级要再次验证已有 checkpoint 的恢复。

## 配置

应用配置位于 [`conf/server/config.toml`](conf/server/config.toml)。从仓库根目录启动前，设置指向 API 所用 PostgreSQL 的 `DATABASE_URL`；该值必填，配置文件不提供默认连接地址或密码：

```sh
export DATABASE_URL='postgres://<user>:<password>@<host>:5432/<database>?sslmode=require'
go run ./cmd/worker -log-config conf/server/logger.toml
```

应用通过 server-go v0.17.1 的 `app.Run` 加载一份配置快照。配置路径按 `-config`、`CONFIG_FILE`、`SERVER_CONFIG_PATH`、`conf/server/config.toml` 的顺序选取，文件必须存在。自定义文件可通过 `go run ./cmd/worker -config /absolute/path/config.toml` 指定。

应用配置支持框架的 `${env:NAME[:default]}`、`${file:/path}` 和 `${secret:...}` 占位符。环境变量通过文件中的占位符读取，不自动覆盖任意 TOML 字段。占位符缺少必需值或无法解析时，启动失败。随附配置使用以下字段：

| TOML 字段 | 占位符环境变量 | 默认值或要求 |
| --- | --- | --- |
| DB.Driver | — | PostgreSQL |
| DB.DSN | DATABASE_URL | 必填，与 API 使用相同数据库 |
| DB.AutoMigrate | — | false，启动不迁移；部署前准备数据库结构 |
| DB.MaxOpenConns / DB.MaxIdleConns | — | 20 / 5 |
| DB.ConnMaxIdleTime | — | 300 秒 |
| Redis.Addrs / Redis.DB | — | 可选，配置后与 API 共享 Redis 推送实时逐 token/思考增量 |
| HTTPServer.Listen | LISTEN_ADDR | 127.0.0.1:8081 |
| RunMode | GIN_MODE | release |
| HTTPServer.ReadTimeout / WriteTimeout / IdleTimeout | — | 10000 / 10000 / 60000 毫秒 |
| Worker.Concurrency | WORKER_CONCURRENCY | 4，必须大于 0 |
| Worker.Lease | WORKER_LEASE | 30s，至少 300ms；每三分之一租约时间续期 |
| Worker.Poll | WORKER_POLL | 1s，必须大于 0 |
| Worker.StreamFirstChunkTimeout | WORKER_STREAM_FIRST_CHUNK_TIMEOUT | 5m，流式首帧到达超时（同步驱动模型 HTTP `ResponseHeaderTimeout`，下限 2m） |
| Worker.StreamIdleTimeout | WORKER_STREAM_IDLE_TIMEOUT | 2m，首帧到达后的分片间空闲超时 |
| Credentials.AllowedEnvPrefixes / AllowedEnvVars | — | 凭据白名单，限制 Agent 的 `apiKeyEnv`/HTTP 与 MCP `tokenEnv` 可读环境变量；平台保留变量（含 `AGENTIC_API_TOKEN`、`SANDBOX_CENTER_TOKEN`）、`ControlPlane.TokenEnv`、`Evaluator.TokenEnv` 与当前 Run 绑定的 `Filesystem.TokenEnv` 始终禁读。**多租户生产环境必须配置** |
| ControlPlane.BaseURL / ControlPlane.TokenEnv | AGENTIC_API_URL / AGENTIC_API_TOKEN_ENV | 可选，配置后启用 `load_skill` 经 API 按需读取冻结 Skill 文件，空则不连接 API |
| Compaction.MaxInputBytes | COMPACTION_MAX_INPUT_BYTES | 65536，未配置模型窗口时用于提前触发字节压缩，0 表示沿用快照的字节阈值 |
| Evaluator.BaseURL / Evaluator.TokenEnv | SANDBOX_CENTER_URL / SANDBOX_CENTER_TOKEN_ENV | 可选，配置后启用沙箱 Goal 验收，空则保持 fail-closed |

### 模型计价

API 在接受 Run 时冻结计价方式。Worker 只读取 `Spec.PricingFrozen` 和 `Spec.PricingSnapshot`，不查询部署价格表。Worker 配置中的 `[Pricing]` 参数（包括 `Models = []`）会明确导致启动失败，请将价格表迁移到 API。框架会丢弃不含任何参数的空 TOML 表，空 `[Pricing]` 与未配置等效。

`PricingFrozen = true` 且 `PricingSnapshot = nil` 表示仅 token 计量，费用记录为 0；如果 Goal 设置 `MaxCostUSD > 0`，则在模型或摘要调用前失败。有快照时按 USD / 1M tokens 计算：`Version` 必须非空且不含换行或回车，`Model`、`BaseURL` 必须与 Run 模型完全匹配，输入、缓存输入和输出价格必须是非负有限数。缓存输入价省略时使用输入价，显式 0 表示免费。`PricingFrozen = false` 的旧 Run 在适配器构建前失败，须经 API 重新提交。

主模型和摘要都使用 Run 的同一模型与计价快照；恢复、Goal 续跑和子 Run 的费用依据冻结快照，API 后续改价不改变已有 Run。当前没有独立摘要模型入口；将来若引入不同摘要模型，必须同时冻结其模型身份和价格。

有计价快照时，如果 provider 只返回正数 `TotalTokens`，无法拆分输入输出，Worker 将已知 token、零费用和 `PricingFailure` 同事务保存，再返回计价错误。接管先检查持久化的 `PricingFailure`，在应用 PendingModel、继续摘要或请求模型前拒绝执行。这里的零费用表示未完成计价，不表示供应方免费；仅 token 计量模式仍正常记录总量。该机制不支持根据历史 usage 补价或回填。

费用预算依据已记录 usage 在调用边界检查，没有额度预占。单次模型调用可能使实际费用超过 `MaxCostUSD`，因此不保证硬预算上限。

独立日志配置可通过 `-log-config` 或 `LOG_CONFIG_FILE` 指定，命令行参数优先；未指定时使用框架默认日志配置。随附 [`conf/server/logger.toml`](conf/server/logger.toml) 使用 `Level = "Info"` 和 Console 输出。日志文件由 `logger.ParseConfigFile` 直接解析，不支持应用配置的占位符。修改数据库、Worker 或日志配置后重启进程。

运行时由框架 `db.Module` 创建和关闭唯一的数据库连接池，`pkg/durable.New` 借用该连接，负责已有事务与状态操作。HTTPHost 的 `Start` 启动执行协程；SIGTERM 退出时先停止 HTTP 接入，再取消并等待 Worker，最后由运行时关闭数据库。框架提供 `/healthz`（服务已启动）和 `/ping`（进程存活），它们不检查数据库的持续可用性。本地默认仅监听回环地址；Docker 与 Helm 监听 `0.0.0.0:8081`，Helm 使用这两个端点探测。

权威事件通知由 API 的 Outbox Relay 处理；Worker 配置可选 `[Redis]` 后额外推送瞬态模型增量（未配置或 Redis 故障不影响权威执行）。模型和工具凭据由任务快照中的 `APIKeyEnv`/`TokenEnv` 指定，经平台保留名拦截与可选 `[Credentials]` 白名单校验后通过运行环境提供，实际凭据不存数据库。Worker 构建不需要 CGO，固定依赖见 go.mod/go.sum。

API 主程序提交任务的总期限为 24 小时，包括等待澄清；默认最多 5 次连续执行尝试。模型/HTTP 工具单次请求超时为 2 分钟；终态后不再接管。修改这些值时应结合真实任务时长评估，不代表已测得的性能指标。

## 验证重启恢复

```bash
export TEST_DATABASE_URL='postgres://agentic:agentic_local@127.0.0.1:55432/agentic?sslmode=disable'
make integration
```

Eino 接入回归覆盖工具调用前已有原生 checkpoint、结果不明时暂停并接受人工确认、同轮多工具顺序、重复 provider ID、跨 checkpoint 的轮次上限、提示词原文与历史，以及旧状态拒绝执行。

`TestProcessKillResumesWithoutRepeatingTool` 会通过真实 HTTP API 提交任务，构建并启动真实 `cmd/worker` 二进制，在工具结果保存、后续模型请求已派发但 usage 尚未提交时强制杀掉该进程，再启动替代执行者。它检查 runID 不变、epoch 增加、工具调用仅一次、任务进入 `Blocked`，并验证无 Redis 情况下的 SSE 回放包含 `RunBlocked`。此时消费未知，Worker 不会自动重放模型请求。

人工检查后可调用 API 的 `POST /v1/runs/{id}/unblock`，提交当前阻断 epoch 和非空 reason，接受未知消费并授权重新派发。`TestUnblockResumesWithoutRepeatingCompletedTools` 覆盖主模型和摘要的人工恢复：保留 checkpoint 与已完成工具，不把未知 usage 补成 0。模型和工具服务是受控 HTTP 测试服务，不调用付费模型。

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
