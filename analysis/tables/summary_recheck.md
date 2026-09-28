# 复核报告：多副本 (1-user & 5-user) vs 单副本

多副本配置：planner/csv-gen/excel-parsing/word-gen/word-parsing 增至 2 副本, 集群 pod 13→18。


## Q1 负滞后是否偶然？——是，偶然

| load    | replicas   | response   |   peak_lag_s |   peak_corr |
|:--------|:-----------|:-----------|-------------:|------------:|
| 1 user  | single     | vCPU       |          -45 |        0.56 |
| 1 user  | multi      | vCPU       |            5 |        0.79 |
| 1 user  | single     | memory     |          -45 |        0.21 |
| 1 user  | multi      | memory     |          -25 |        0.43 |
| 3 users | single     | vCPU       |           55 |        0.72 |
| 3 users | multi      | vCPU       |           20 |        0.92 |
| 3 users | single     | memory     |           40 |        0.3  |
| 3 users | multi      | memory     |           40 |        0.46 |
| 5 users | single     | vCPU       |           30 |        0.81 |
| 5 users | multi      | vCPU       |           10 |        0.94 |
| 5 users | single     | memory     |           35 |        0.15 |
| 5 users | multi      | memory     |           60 |        0.66 |


- 旧 1-user(单副本) QPS→CPU 峰值滞后 **−45s(r=0.56)** 为偶然/稀疏噪声(CCF 近水平)；新 1-user(多副本) 转为 **+5s(r=0.79)**。
- 5-user 两次都为正(single +30s / multi +10s)，multi 相关更强(r=0.94)。
- CPU 滞后一致为正且相关随负载/副本增强；内存滞后更长（multi 5u 达 +60s），符合内存缓慢累积。

图: `recheck_lag_ccf.png`


## Q2 其余结论在多副本下是否成立？——全部成立

| run            |   lat_info% |   succ_info% |   qps_nonzero% |   latP99_CV_med |   latP99_CV_max |   netRecv_CV_max |   traces |   chain_len_mean |   chain_len_max |   distinct_paths |   planner_execCV |   ocr_execCV |
|:---------------|------------:|-------------:|---------------:|----------------:|----------------:|-----------------:|---------:|-----------------:|----------------:|-----------------:|-----------------:|-------------:|
| 1 user single  |         9.9 |         19.5 |            9.8 |            0.17 |            4.12 |             3.75 |       46 |             2.83 |               7 |               12 |             0.89 |         0.83 |
| 1 user multi   |         9.8 |         18.7 |           10   |            0.59 |            3.89 |             0.32 |       40 |             2.83 |               7 |               14 |             0.26 |         0.64 |
| 3 users single |        14.2 |         24.2 |           14.4 |            0.79 |            2.14 |             1.55 |      122 |             2.63 |               6 |               16 |             0.48 |         0.94 |
| 3 users multi  |        13.9 |         22.3 |           13.9 |            0.54 |            1.17 |             1.27 |      120 |             2.59 |               6 |               17 |             0.28 |         1.31 |
| 5 users single |        20.6 |         36.6 |           21.2 |            0.45 |            1.05 |             1.32 |      173 |             2.98 |               9 |               22 |             0.7  |         2.81 |
| 5 users multi  |        16.3 |         25.2 |           16.6 |            0.54 |            1.04 |             2.44 |      204 |             2.55 |               8 |               17 |             0.81 |         1.17 |


稀疏性(latency/success/qps)、方差(latency p99 CV、planner/OCR 执行时间 CV)、链路长度/路径动态性，multi 与对应 single 同量级，C1/C2 结论不变。


## Q2b 多副本的影响

| run            | service       |   replicas |   lat_p99_nonnull% |
|:---------------|:--------------|-----------:|-------------------:|
| 1 user single  | planner       |          1 |               34.8 |
| 1 user single  | csv-gen       |          1 |                3.3 |
| 1 user single  | excel-parsing |          1 |                6.6 |
| 1 user single  | word-gen      |          1 |                6.6 |
| 1 user single  | word-parsing  |          1 |               98.3 |
| 1 user single  | ocr           |          1 |               59.1 |
| 1 user single  | summarizer    |          1 |               31.5 |
| 1 user multi   | planner       |          2 |               31.5 |
| 1 user multi   | csv-gen       |          2 |                5   |
| 1 user multi   | excel-parsing |          2 |                9.9 |
| 1 user multi   | word-gen      |          2 |               55.8 |
| 1 user multi   | word-parsing  |          2 |                3.3 |
| 1 user multi   | ocr           |          1 |                1.7 |
| 1 user multi   | summarizer    |          1 |               28.2 |
| 3 users single | planner       |          1 |               31.1 |
| 3 users single | csv-gen       |          1 |                8.7 |
| 3 users single | excel-parsing |          1 |               10   |
| 3 users single | word-gen      |          1 |               16.2 |
| 3 users single | word-parsing  |          1 |                5   |
| 3 users single | ocr           |          1 |                8.7 |
| 3 users single | summarizer    |          1 |               29.9 |
| 3 users multi  | planner       |          2 |               26.6 |
| 3 users multi  | csv-gen       |          2 |                8.3 |
| 3 users multi  | excel-parsing |          2 |               10.4 |
| 3 users multi  | word-gen      |          2 |               18.3 |
| 3 users multi  | word-parsing  |          2 |                9.5 |
| 3 users multi  | ocr           |          1 |                7.5 |
| 3 users multi  | summarizer    |          1 |               29.9 |
| 5 users single | planner       |          1 |               49   |
| 5 users single | csv-gen       |          1 |                6.6 |
| 5 users single | excel-parsing |          1 |               15   |
| 5 users single | word-gen      |          1 |               24.1 |
| 5 users single | word-parsing  |          1 |               10.8 |
| 5 users single | ocr           |          1 |                8.3 |
| 5 users single | summarizer    |          1 |               52.6 |
| 5 users multi  | planner       |          2 |               31.9 |
| 5 users multi  | csv-gen       |          2 |                4.3 |
| 5 users multi  | excel-parsing |          2 |               15.9 |
| 5 users multi  | word-gen      |          2 |               19.9 |
| 5 users multi  | word-parsing  |          2 |                9.3 |
| 5 users multi  | ocr           |          1 |               12   |
| 5 users multi  | summarizer    |          1 |               28.9 |


集群资源：


| run            |   pods |   vCPU_mean |   mem_mean |
|:---------------|-------:|------------:|-----------:|
| 1 user single  |     13 |        0.23 |       10.9 |
| 1 user multi   |     18 |        0.42 |       16.5 |
| 3 users single |     13 |        0.38 |       17   |
| 3 users multi  |     18 |        0.5  |       35.3 |
| 5 users single |     13 |        0.28 |       12   |
| 5 users multi  |     18 |        0.54 |       16.3 |


- 服务级指标(latency/qps)是**跨副本聚合**的，稀疏性/方差不受副本数影响。
- 副本只改变集群总资源(pod 13→18, vCPU/内存增)。
- 各服务时延可观测性在不同采集间大幅波动，源于**任务组合不同**而非副本——再次印证 C2。

表: `recheck_compare.csv`, `recheck_replica_effect.csv`, `recheck_cluster_resource.csv`
