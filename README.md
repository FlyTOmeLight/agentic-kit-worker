# Agentic Kit Worker

独立部署的 Agent 执行进程。从 PostgreSQL 领取任务，调用 Eino 模型和工具，在模型决策与工具结果之间保存状态；进程重启后，其他实例可以接管同一任务。

与 `agentic-kit-api` 仓库配套。运行方式见 [API 启动说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/README.md)。Go module 引用 GitHub 上 API 的 `v0.1.0` 版本，可独立克隆和构建；私有模块认证与可选 go work 联合开发见 API 启动说明。

## 项目结构

入口位于 `cmd/worker`，执行器位于 `internal/engine`，模型与工具适配位于 `internal/adapters`。跨仓库只依赖 API 的 `pkg/durable`；[目录与依赖说明](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/layout.md)。

## 执行规则

1. 数据库行锁领取任务，递增 epoch；定期续期，失去执行权立即取消本地执行。
2. 完整保存模型回复、工具名称和参数后，才执行工具。
3. 调用工具前保存 Started 意图；成功后，在同一事务里保存工具结果、上下文和事件。
4. 重启后加载已保存上下文。已经完成的工具不会再次执行；未完成的模型调用可能重新生成。
5. 只读工具允许重试；幂等工具携带跨执行批次不变的操作编号；不安全工具遇到不明确结果转 NeedsReview。
6. 澄清问题保存为 WaitingClarification，不占用 worker；回答入库后重新排队。

`engine.Build` 接受原系统使用的 Eino `ToolCallingChatModel` 与 `InvokableTool`，已实现 OpenAI 兼容模型、HTTP 工具以及原 AppForge MCP/OpenAPI 调用协议。这里重写的是执行状态管理，不复制旧 ADK 的仅澄清 checkpoint 语义。

当前模型使用完整 Generate，SSE 展示步骤进度与完整结果，不逐 token 推送。工具按模型返回的顺序执行，不并发执行同一轮工具。

## 配置

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

`TestProcessKillResumesWithoutRepeatingTool` 会通过真实 HTTP API 提交任务，启动独立子进程 worker，在工具结果保存后强制杀掉该进程，再启动替代执行者。它检查 runID 不变、epoch 增加、工具调用仅一次、任务最终完成，并验证无 Redis 情况下的 SSE 回放。模型和工具服务是受控 HTTP 测试服务，不调用付费模型。

[完整迁移范围](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/migration.md) · [故障语义](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/design.md)
