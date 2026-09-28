## C1/C2 补充：基于真实调用链 (graph.json) 的证据

### C1 完成时间沿调用链累积 + 链路动态性

| load    |   traces |   len_mean |   len_max |   distinct_paths |   distinct_ratio |   path_entropy_bits |
|:--------|---------:|-----------:|----------:|-----------------:|-----------------:|--------------------:|
| 1 user  |       46 |       2.83 |         7 |               12 |             0.32 |                3.37 |
| 3 users |      122 |       2.63 |         6 |               16 |             0.15 |                3.7  |
| 5 users |      173 |       2.98 |         9 |               22 |             0.15 |                3.79 |


要点：调用链长度高度可变 (最长 7–9 层)，不同请求路径各异 (路径熵见上表)，且完成时间随深度单调累积、跨 trace 分散显著 → 无法为一次逻辑请求划定统一的观测时间窗口。

图: `c1r_completion_by_depth.png`, `c1r_chain_dynamism.png`；表: `chain_completion_by_depth.csv`, `chain_dynamism.csv`


### C2 顶点调用稀疏 + 执行时间方差极大

（仅统计**实际执行 span**：节点名含 `/` 的 Group/Agent；已剔除 time≈0 的组入口路由 span。）


各负载下顶点执行时间 CV 中位数：{'1 user': 0.44, '3 users': 0.48, '5 users': 0.54}；5 users 最高：OCRParser=2.81, DirectionAgent=1.15, Excel=0.85


要点：绝大多数工具顶点只在少数 trace 中被调用 (coverage <14%，极稀疏)；执行时间方差方面，**计算/IO 密集型智能体 CV 极大**（OCR≈2.5、planner≈1.45），整体中位 CV≈0.5、约 1/5 顶点 CV≥1 → 主导时延的关键服务其正常态波动就极大，故障引起的特征变化难以与这种固有稀疏+高方差区分。

图: `c2r_vertex_sparsity_variance.png`；表: `chain_vertex_stats.csv`
