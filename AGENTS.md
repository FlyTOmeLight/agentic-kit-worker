# Agentic Kit Worker 项目约定

## 目录与依赖

- 调整目录或包边界前，读取 [项目布局](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/layout.md)。`cmd/worker/` 负责入口与依赖装配，`internal/engine/` 负责执行、领取与续期，`internal/adapters/` 负责模型和工具接入。
- 跨仓库只引用API 的 `pkg/durable/`；不导入 API 的 internal 包。跨仓库集成测试通过真实 HTTP 接口访问 API。
- 默认依赖 go.mod 固定的 GitHub API 模块版本，本地联合开发使用 go work；启动、配置和测试环境见 README.md，具体命令以 Makefile 为准。

## 恢复与工具契约

- 修改恢复边界、重试或取消行为前，读取 [恢复设计](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/design.md)，同时核对 API 的共享存储实现。
- 模型决策持久化后才执行工具；工具结果与步骤上下文一起提交。恢复复用已提交结果，工具操作编号在不同执行批次间保持不变。
- 工具重试策略区分 ReadOnly、Idempotent、Unsafe。Idempotent 必须有真实的下游去重契约；不确定的 Unsafe 工具结果进入待确认状态，不能自动重复执行。
- 原业务功能是否已迁移，以 [迁移清单](https://github.com/FlyTOmeLight/agentic-kit-api/blob/main/docs/migration.md) 为准。

## 验证

- 涉及调度、步骤存档、恢复或工具重试时，配置隔离测试数据库并执行 `make integration`，覆盖强杀进程后接管和已完成工具不重复执行的行为。
- 改动工具适配时检查实际请求协议、取消传播和重试策略；改动共享存储契约时同时验证API 仓库。
