# Motivation 分析结果 (normal thingo, 1/3/5 users)

采样间隔 5s。三组正常态数据。trace/log 维度的 trace pkl 在三组中均为空 (normal/inbound/outbound/abnormal 长度为 0)，本身即稀疏性的极端证据。


## C1 指标滞后性 → 时间窗口难确定

### C1.1 整体：QPS(驱动信号) 与资源响应的互相关峰值滞后

| load    | response   |   peak_lag_s |   peak_corr |
|:--------|:-----------|-------------:|------------:|
| 1 user  | CPU        |          -45 |        0.56 |
| 1 user  | Memory     |          -45 |        0.21 |
| 3 users | CPU        |           55 |        0.72 |
| 3 users | Memory     |           40 |        0.3  |
| 5 users | CPU        |           30 |        0.81 |
| 5 users | Memory     |           35 |        0.15 |


要点：CPU 相对请求存在 10–35s 的正向滞后 (峰值相关随负载升高而增强)，而内存的最佳滞后与 CPU 不一致且相关性弱 (缓慢累积、长时间不回落)。不同指标的响应滞后不一致 → 无法用单一固定时间窗口对齐所有信号。

图: `c1_lag_ccf.png`, `c1_lag_overlay_10user.png`


### C1.2 沿调用链路：滞后随链路长度累积

调用链路 (按服务角色定义，因 trace 为空)：planner(入口) → direction(路由) → 解析类 → 生成类 → summarizer(出口)。以 planner QPS 为入口基准信号，测各下游服务 QPS 的峰值互相关滞后。

每阶段中位滞后 (s)：

| 负载 | 入口 entry | 路由 route | 解析 parse | 生成 generate | 出口 exit |
|---|---|---|---|---|---|
| 1 user | 0 | 20 | 40 | 30 | 30 |
| 3 users | 0 | 20 | 42 | 60 | 0 |
| 5 users | 0 | 35 | 50 | 40 | 45 |

要点：入口 planner 滞后≈0s (即驱动信号本身)；一旦进入下游，滞后立即跳升到 20–50s，且随链路加深而增大/更分散 (1、5 users 下出口 summarizer 滞后 30–45s)。同一逻辑请求在链路不同阶段的响应散布在 0→50s 的宽区间，链路越长、动态性越强，响应窗口越晚越宽 → 无法为整条链划定统一时间窗口。

注：5 users 的 summarizer QPS 采集偏稀疏 (非零率仅 13%)，其出口点为采集伪影，不作为主证据；解析/生成阶段仍显著滞后于入口。

图: `c1_chain_lag_stage.png`, `c1_chain_overlay_5user.png`；表: `chain_lag.csv`


## C2 稀疏性 + 极大方差 → 故障特征难识别

### 稀疏性 (有效信息占比)

| load    | metric                |   null_% |   zero_% |   informative_% |
|:--------|:----------------------|---------:|---------:|----------------:|
| 1 user  | latency (p50/p90/p99) |     76   |     14   |             9.9 |
| 1 user  | success_rate          |     80.5 |      0   |            19.5 |
| 1 user  | qps                   |      0   |     90.2 |             9.8 |
| 1 user  | call (p50/p90/p99)    |     76   |     14   |             9.9 |
| 3 users | latency (p50/p90/p99) |     80.9 |      4.9 |            14.2 |
| 3 users | success_rate          |     75   |      0.8 |            24.2 |
| 3 users | qps                   |      0   |     85.6 |            14.4 |
| 3 users | call (p50/p90/p99)    |     80.9 |      4.9 |            14.2 |
| 5 users | latency (p50/p90/p99) |     79.4 |      0   |            20.6 |
| 5 users | success_rate          |     62.6 |      0.8 |            36.6 |
| 5 users | qps                   |      0   |     78.8 |            21.2 |
| 5 users | call (p50/p90/p99)    |     79.4 |      0   |            20.6 |


要点：时延/调用指标即便在 5 users 下仍有 ~79% 为空、QPS ~79% 为零，真正携带信号的单元格不足 1/4；1 user 下更严重。故障特征淹没在缺失里。

图: `c2_sparsity_bars.png`, `c2_latency_availability.png`


### 方差 (正常态变异系数 CV)

| load    | metric      |   median_CV |   p90_CV |   max_CV |   frac_CV>0.5 |   frac_CV>1 |
|:--------|:------------|------------:|---------:|---------:|--------------:|------------:|
| 1 user  | CPU         |        0.24 |     0.7  |     1.09 |          0.23 |        0.08 |
| 1 user  | Memory      |        0.02 |     0.09 |     0.09 |          0    |        0    |
| 1 user  | Net recv    |        0.28 |     1.18 |     3.75 |          0.23 |        0.15 |
| 1 user  | Latency p99 |        0.17 |     1.04 |     4.12 |          0.3  |        0.1  |
| 1 user  | Latency p50 |        0.07 |     0.79 |     4.12 |          0.1  |        0.1  |
| 3 users | CPU         |        0.63 |     1.54 |     1.59 |          0.62 |        0.38 |
| 3 users | Memory      |        0.01 |     0.04 |     0.47 |          0    |        0    |
| 3 users | Net recv    |        0.33 |     1.18 |     1.55 |          0.31 |        0.15 |
| 3 users | Latency p99 |        0.79 |     1.69 |     2.14 |          0.75 |        0.25 |
| 3 users | Latency p50 |        0.67 |     1.55 |     2.18 |          0.58 |        0.25 |
| 5 users | CPU         |        0.25 |     1.02 |     1.53 |          0.31 |        0.15 |
| 5 users | Memory      |        0.04 |     0.17 |     0.19 |          0    |        0    |
| 5 users | Net recv    |        0.35 |     0.78 |     1.32 |          0.15 |        0.08 |
| 5 users | Latency p99 |        0.45 |     0.93 |     1.05 |          0.45 |        0.09 |
| 5 users | Latency p50 |        0.39 |     0.93 |     0.96 |          0.36 |        0    |


要点：正常运行下网络/时延指标的 CV 已可达 1–4，相当比例的指标列 CV>0.5。基线波动本身巨大，导致基于均值±kσ 的阈值要么漏报要么频繁误报，故障特征难以与噪声区分。

图: `c2_variance_cv_box.png`
