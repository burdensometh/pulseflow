受控窗口归属回归的已处理业务数据。state 是原运行目录，consumer.sqlite 是独立消费库；原始源日志及已有结果均保留。复制目录后可用 PulseFlow(copy/state) 打开。

该目录曾由归属公式存在缺陷的版本生成（size 不能整除 slide 时漏掉重叠窗口，负时间网格用零截断除法导致归属错误）。现已通过 `PulseFlow.repair.reconcile()` 在不重置源位点、不清空管道的前提下就地补正：

- `window_events`/`window_status` 按修正后的左闭右开网格归属增量对齐，缺失窗口补建，多余归属移除；
- 发生变化的窗口结果通过既有版本化变更流以更高 revision 发布（错误窗口 [-9,2) 升至 revision 2，补建的 [-15,-4)、[-12,-1) 为 revision 1；指标文档未变化的窗口不产生虚假 revision）；
- 每个管道在 `audit_events` 留有一条 `window_membership_repaired` 记录，重复执行修复为零差异 no-op；
- `consumer.sqlite` 已通过正常 catch-up 消费新变更流，物化结果与修正后的管道一致；
- `journal`、源位点、processed/late 记录、水位以及 `artifacts/` 中的不可变 JSONL 导出（revision 1 的历史快照）均原样保留。

修复后 `consistency.check` 对 flow 与 replayed 两个管道均通过，且 replay 对比一致。它现在是修复后的回归基线，可用于验证修复幂等性与下游收敛。
