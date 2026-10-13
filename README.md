# Agentic Kit Worker

独立部署的 Agent 执行进程。从 PostgreSQL 领取任务，用 Eino ADK `ChatModelAgent + Runner` 执行单 Agent ReAct。Eino 管理模型／工具循环、消息上下文和 checkpoint；平台保存 checkpoint、执行权与工具结果，进程重启后由其他实例接管。

与 `agentic-kit-api v0.5.1` 配套。该版本包含 `returnDirectly`、AIO 文件系统绑定和执行事件契约，Worker 在 `go.mod` 中固定引用。两个仓库可独立克隆和构建。运行方式、模块认证与可选 go work 联合开发见 [API 启动说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/README.md)。

## 项目结构

入口位于 `cmd/worker`，server-go 生命周期与资源装配位于 `internal/service`，执行器位于 `internal/engine`，模型与工具适配位于 `internal/adapters`。数据库访问统一由 API 的 `pkg/durable` 使用 GORM 管理，worker 复用其中的事务和状态操作；[目录与依赖说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/layout.md)。

## Agent 架构与执行规则

发布配置中的提示词、模型、工具和轮次上限装配为一个 Eino `ChatModelAgent`。当前架构是单 Agent ReAct；模型依据任务与工具结果逐步决定下一步。没有独立 Planner/Executor，也没有多 Agent 编排。工具的 `returnDirectly` 配置直接映射到 Eino `ToolsConfig.ReturnDirectly`；例如 Coding 的 finish 成功后立即结束本轮，保留工具结果作为最终输出。`internal/engine/engine.go` 创建 Agent 和 Runner，消费事件并处理恢复，不再维护另一套模型／工具循环。

1. 平台从数据库领取任务并定期续期；失去执行权后取消本地执行。
2. `boundaries.go` 的模型 middleware 记录模型调用，给工具调用分配稳定编号。每批工具执行前通过 Eino 原生工具中断创建 checkpoint。
3. `checkpoint.go` 实现 Eino `CheckPointStore`。Eino 负责 checkpoint 的编解码；平台在同一事务保存 checkpoint 与模型决策，再由 `Runner.Resume` 继续执行工具。
4. 工具 middleware 先记录 Started 意图，再调用实际工具。工具结果、公开消息和当前 checkpoint 引用一起提交。崩溃后 Eino 从 checkpoint 恢复，middleware 向 Eino 返回已经保存的工具结果，不重复调用外部工具。
5. 只读工具允许重试；幂等工具使用固定操作编号；结果不明确的 Unsafe 工具进入 NeedsReview。澄清工具进入 WaitingClarification。两者都保存 Eino checkpoint，人工结果提交后重新排队并恢复。
6. 同轮工具按模型返回顺序执行。前一个工具等待确认或失败时，后面的工具不产生副作用。

`engine.Build` 继续接收 Eino `ToolCallingChatModel` 与 `InvokableTool`；远端工具使用通用 HTTP POST：URL 为完整调用地址，直接发送模型生成的 JSON 参数并返回响应正文。工具类型仅接受 `HTTP` 或空值，其他类型在装配时拒绝。当前使用完整 Generate，SSE 展示步骤进度与完整结果，不逐 token 推送。

### 升级边界

执行状态使用 version 2，其中 `checkpoint` 是 Eino 生成的原生数据。旧 version 1 的手写循环状态不能交给 Runner 恢复；新版会明确拒绝，不把旧任务从头执行。切换 Worker 前，须让旧 Worker 完成或取消所有旧格式的活动、等待澄清和待确认运行；保留历史记录，新业务轮次创建新的 Run。不要将新旧 Worker 同时接入执行队列。当前 Eino 版本固定为 `v0.9.2`，后续升级要再次验证已有 checkpoint 的恢复。

## 配置

复制 `configs/config.example.toml` 为 `configs/config.local.toml`，执行 `go run ./cmd/worker -config configs/config.local.toml`。也可通过 CONFIG_FILE 指定文件；未指定文件时读取环境变量和默认值。配置使用 server-go v0.3.1 的 `conf.Manager` 独立快照；Viper 保留严格字段校验和环境变量覆盖，应用和 HTTPHost 使用同一份有效配置。也支持 `SERVER_CONFIG_PATH` 与框架的 `${env: NAME[:default]}`、`${file: /path}`、`${secret: ...}` 占位符。未指定任何文件时继续使用环境变量和默认值，修改配置后需重启。

| TOML 字段 | 环境变量 | 默认值 |
| --- | --- | --- |
| database.url | DATABASE_URL | 必填 |
| HTTPServer.Listen | LISTEN_ADDR | 127.0.0.1:8081 |
| RunMode | GIN_MODE | release |
| worker.concurrency | WORKER_CONCURRENCY | 4 |
| worker.lease | WORKER_LEASE | 30s，至少 300ms |
| worker.poll | WORKER_POLL | 1s，必须大于 0 |

环境变量 > TOML > 默认值；空环境变量也覆盖文件。未知字段、非法时长、非正并发数和读取错误均在连接数据库前报告。模型和工具密钥仍由环境变量提供。server-go v0.3.1 提供配置快照、Bootstrap、HTTPHost 和日志，使用其已发布的公开模块；Viper v1.21.0 用于字段校验和环境覆盖，均为 MIT 许可，Worker 构建不需要 CGO。固定依赖见 go.mod/go.sum。


Bootstrap 在启动时连接数据库，HTTPHost 的 `Start` 启动执行协程。SIGTERM 退出时先停止 HTTP 接入，再取消并等待 Worker，最后关闭数据库。执行器继续使用 `pkg/durable` 的事务与连接，不额外创建框架数据库池。框架提供 `/healthz`（服务已启动）和 `/ping`（进程存活），它们不检查数据库的持续可用性。本地默认仅监听回环地址；Docker 与 Helm 监听 `0.0.0.0:8081`，Helm 使用这两个端点探测。

- `DATABASE_URL`：与 API 相同的 PostgreSQL。
- `WORKER_CONCURRENCY`：默认 4，每个实例同时执行的任务数。
- `WORKER_LEASE`：默认 30s，每三分之一租约时间续期一次。
- 模型和工具凭据：由任务快照中的 APIKeyEnv/TokenEnv 指定；实际凭据不存数据库。

API 主程序提交任务的总期限为 24 小时，包括等待澄清；默认最多 5 次连续执行尝试。模型/HTTP 工具单次请求超时为 2 分钟；终态后不再接管。修改这些值时应结合真实任务时长评估，不代表已测得的性能指标。

## 验证重启恢复

```bash
export TEST_DATABASE_URL='postgres://agentic:agentic_local@127.0.0.1:55432/agentic?sslmode=disable'
make integration
```

Eino 接入回归覆盖工具调用前已有原生 checkpoint、结果不明时暂停并接受人工确认、同轮多工具顺序、重复 provider ID、跨 checkpoint 的轮次上限、提示词原文与历史，以及旧状态拒绝执行。

`TestProcessKillResumesWithoutRepeatingTool` 会通过真实 HTTP API 提交任务，构建并启动真实 `cmd/worker` 二进制，在工具结果保存后强制杀掉该进程，再启动替代执行者。它检查 runID 不变、epoch 增加、工具调用仅一次、任务最终完成，并验证无 Redis 情况下的 SSE 回放。模型和工具服务是受控 HTTP 测试服务，不调用付费模型。

`TestWorkerHealthAndShutdown` 验证真实 Worker 启动后 `/healthz` 返回 200，并在 SIGTERM 后成功退出。独立交付验证使用 `GOWORK=off`，避免测试依赖相邻 API 源码。

[完整迁移范围](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/migration.md) · [故障语义](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/design.md)

Agent 定义通过 API 管理和发布。worker 始终读取 Run 快照，编辑草稿或发布新版本不会改变正在执行的任务。v0.2 需要配套的新会话存储，升级范围见 API README；不与 v0.1 API 混用。

## Coding 工具与 Kubernetes

HTTP 工具请求由 Worker 注入 `X-Agentic-Run-ID`、`X-Agentic-Epoch`、冻结的 `X-Agentic-Scope` 和稳定 Idempotency-Key。scope 来自平台接受的应用输入，工具参数不能覆盖。工具服务应同时验证运行时执行权与业务写入资格。Coding 使用每轮独立 AIO 沙箱，平台持有 GitLab 凭据。Worker 只持有 Coding 网关凭据。

Helm Chart 位于 `charts/agentic-kit-worker`，用 `existingSecret` 指定运行时数据库与工具凭据。构建过程从固定的 API 模块版本下载依赖，不需要相邻 API checkout。GitHub CLI 登录后，使用 BuildKit 临时挂载私有模块认证。凭据不写入镜像层：

```sh
WORKER_GITHUB_NETRC="machine github.com login x-access-token password $(gh auth token)" \
  docker build --secret id=github_netrc,env=WORKER_GITHUB_NETRC -t agentic-kit-worker:dev .
```

## AIO 文件与命令工具

Agent 的 `toolsets:["filesystem"]` 由 `internal/adapters/aio` 适配到 Eino 原生文件工具与 Shell。官方 `sandbox-sdk-go v0.0.5` 对接固定的 AIO 1.11.0 协议。可信 ExecutionSpec 冻结 provider、URL、TokenEnv 和工作目录，普通会话提交不能提供沙箱地址。SDK 禁用写操作的隐式重试。构建工具前执行 readiness 与接管隔离。

`endRun:true` 使用 Eino `ReturnDirectly` 结束运行。参数由 JSON Schema 库校验。终止调用参数无效或与其他工具混用时，边界将其转为不可执行的原生工具错误反馈，使模型修正后再结束；不执行混合批次的副作用。该校验使用 jsonschema/v6，避免重复实现 JSON Schema 规范。
