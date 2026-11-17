# PulseFlow

原创的事件时间流处理与实时 ETL 引擎，Python 3.11+ 标准库实现。主要数据链路是：分区事件日志 → 时态维度关联/表达式转换 → 有状态去重与序列匹配 → 事件时间窗口 → 版本化变更流 → 独立消费库与分层聚合。

本项目由当前对话原创实现，不声称来自第三方上游。与 CatalogSync 的来源抓取、DocWeave 的文档获取、SchemaLake 的批次文件快照不同，PulseFlow 的核心是源位点、事件时间、水位、窗口归属和计算状态恢复。它不实现看板、报表页面、CRUD 后台、权限系统或独立命令行工具。

## 使用

所有数据生成、测试、构建与行为验证均在指定 ECS 的 Linux Docker 中执行，控制端只编辑与传输文件。源码可直接导入，不需安装第三方运行依赖。

```python
from pulseflow import PulseFlow

app = PulseFlow('/tmp/pulseflow-demo')
app.journal.create('readings', 1)
app.engine.create('windows', 'readings', {
    'window': {'kind': 'tumbling', 'size_ms': 10000},
    'metrics': ['count', 'sum', 'p95'],
})
app.journal.append('readings', 'batch1', [
    {'partition': 0, 'offset': 0, 'event': {
        'id': 'reading-1', 'key': 'station-a', 'time': 0, 'value': '1.25',
    }},
    {'partition': 0, 'offset': 1, 'event': {
        'id': 'reading-2', 'key': 'station-a', 'time': 10000, 'value': '2.50',
    }},
])
app.engine.drain('windows')
print(app.results.rows('windows'))
```

在 ECS 容器源码目录运行 `python3 -m unittest discover -s tests -v`。`pulseflow.service.serving(app)` 可启动仅监听环回地址的操作 API；HTTP 与 Python 服务使用相同数据链路。操作接口面向可信隔离环境，不提供公网权限管理。

## 明确的当前契约

- 源分区从位点 0 连续追加，位点内容和通道内事件 ID 不可重用为其他数据。批次重放幂等；同批异内容拒绝。源记录与源位点在同一事务中提交。
- 时间支持带时区 ISO 文本或整数毫秒，包括 epoch 前负时间；拒绝隐式时区和子毫秒值。测量值使用精确十进制，拒绝 float、NaN、无穷与超出精度的值。
- 窗口是左闭右开区间；支持 tumbling 与任意合法 slide 的 hopping，可带网格偏移。统计含行数/非空数、精确和/均值、极值、nearest-rank p50/p95 和精确 distinct。均值四舍五入到 12 位小数。
- 分区最大事件时间或显式声明的时间减 delay，再对所有源分区取最小值；未见任何事件且未声明的分区阻止水位前进。全局水位单向推进；事件时间严格小于当前水位的记录保存为 late，不再更新窗口。等于水位仍可接收。
- 当窗口右边界不大于水位时发布结果。窗口事件全部持久化在状态库中；max_window_events 是内存记录预算而非容量上限，超出预算的合法大窗口不再被拒绝：关闭时按预算分块流式扫描（任意时刻最多预算条记录进入 Python 集合），精确 distinct 与 nearest-rank 分位数由磁盘支撑的排序/去重完成，和、均值、极值与完整内存计算逐位一致，不引入近似算法；当前仍没有窗口状态自动回收。
- 处理顺序使用持久源日志的 sequence，每个 chunk 的源进度、算子状态、水位、结果和变更流一起提交。任一转换错误使整个 chunk 回滚；当前不支持按故障分区隔离，也没有自动修复事件。
- Worker 使用租约与递增 fence；过期或被替代的 worker 不能提交。状态按 Adler32(key) mod buckets 分桶，当前 hash_version=1；分桶影响窗口与键状态的实际读写路径。
- 维度按 valid_from 左闭区间解释，同一 valid_from 取最高 revision，删除版本阻止回退到更早值。事件在处理时按自己的事件时间关联维度。后来补发的历史维度更正不会自动重算已处理事件和已发布结果。
- 去重基于事件时间 TTL，不更改源位点；null 去重字段不参与。最近值按 (event_time,event_id) 裁决。A→B 序列匹配使用每键最新 A，一次匹配消费该 A；它不是全重叠模式匹配器。
- 结果按 key 保留单调 revision，变更流是 upsert/delete；独立 Consumer 将数据和 cursor 事务提交，并绑定真实源实例及计划身份。不可变 JSONL/CSV 导出保留当时结果。
- Rollup 消费同一变更流，将已关闭窗口按 key-to-group 映射汇总；替换/删除输入会撤回旧贡献。当前每次从缓存输入重建汇总结果，且输出与 rollup cursor 在同一目录事务中提交。
- v1 保存点保存真实分区进度、窗口、键状态、去重、late、结果及变更流，校验源前缀后恢复到同一 catalog 的新管道；允许压缩，不承诺跨不同日志 sequence 布局的目录直接恢复。不能改变 state buckets/hash 版本；现有算子状态表在保存点中显式列举。
- Replay 创建独立新管道，从同一源日志顺序完整处理，可比较结果和事件去向。它不提供活跃管道的新计划热切换、消费者别名切换或历史结果自动更新。
- SQLiteIngress 使用只读源快照采集递增整数序列，将源前缀指纹、连接器水位与事件日志一并提交；已提交源前缀被修改会拒绝继续。Queries.series 对单键已接受事件提供前值保持/线性插值网格和精确时间积分，允许覆盖缺口，单独记录覆盖时长。
- 文件摄取只接受完整 JSONL 行，保存前缀 hash 与字节/源位点；改写已摄取前缀被拒绝。冻结文件分块导入允许已成功前缀可见。Journal exchange 在受限原子批量内验证并导入完整分区前缀。

当前没有 session window、双事件流 interval join、空闲分区自动排除、历史维度修订传播、状态重分桶迁移或通用算子插件生命周期。未来扩展需要以当前真实实现和可观察缺口为依据。

作者题面、设计卡、私有探针、轨迹及服务器凭据不属于本源码仓库。
