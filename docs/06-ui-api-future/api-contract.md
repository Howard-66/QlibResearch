# Future API Contract

预留三类接口：

- 任务接口：导出 panel、运行 workflow、训练发布、同步产物
- 查询接口：任务状态、recipe 配置、产物列表、运行摘要、诊断结果
- 发布接口：选定模型、设置 latest、同步到 `ValueInvesting`

任务状态建议固定为：`queued / running / succeeded / failed / cancelled`。
