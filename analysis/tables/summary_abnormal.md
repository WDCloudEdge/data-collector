# 异常实验补充：动机 2 的图表与证据

## 来源与口径

- 异常数据：`data/thingo/abnormal/load-3/agent-network-pdf-parsing_{fault_type}_1/agent-network/metrics/`（时间线图另用 `load-5/`）。
- 故障时刻与负载：各 load 目录下的 `agent-network-pdf-parsing_label.txt`。五类故障各一次；load-3 为 3 用户、load-5 为 5 用户，均约 181 秒。
- 重叠图（`c2c_...`）按“故障服务”分四个条件：pdf-parsing（load-3 与 load-5）、planner（load-5）、summarizer（load-5），共二十次实验（五类故障 × 四条件）；时间线图 (c) 使用 pdf-parsing 的 load-3 与 load-5。load-5 每次实验丢弃前 60 秒冷启动数据。
- 重叠图基线改为“同一次实验自身的注入前工作负载区间” `[load_start, fault_start)`（负载已开、故障未注入，同机同条件），不再跨采集对照另一次 normal 运行。
- 主对照：`data/thingo/normal/20260913-14.40-14.45-normal-thingo-3user-5min-14.50/agent-network/metrics/`，与原分析 `common.DATASETS["3 users"]` 一致。
- 正常使用完整已采集序列；异常仅使用标签区间 `[fault_start, fault_end)`。每个故障区间有 37 个采样点；正常资源指标有 241 个采样点，p99 有 178 个有效点。样本数是监控采样点数，不是独立实验次数。
- CPU / 内存从 `instance.csv` 逐时刻汇总该服务全部 Pod，包括 pod_kill 重建后的新 Pod；保留采集器已记录的零值。时延使用 `latency.csv` 的 p99；成功率使用 `success_rate.csv`，缺失值不补 1，也不连线跨越缺失区段。
- CV = 总体标准差 / 均值。`standardized_mean_shift` = 两状态均值差绝对值 / 正常标准差，只表示均值偏移，不等同于分布重叠；重叠证据另用“故障期样本落在正常 p10--p90 范围内的比例”。
- 成功率下降定义为有效值 < 0.999；同时记录采集窗口内首次下降、注入后首次下降及是否已有注入前下降。时差均为采样时间相对标签时刻的观测偏移，不是逐请求因果传播时间。

## 图如何合并

1. `figures/c2c_abn_vs_normal_overlap.pdf`：(a) 复用 `motivation_analysis.variance(ax=...)` 的正常态 CV 分析；(b)(c) 为四个故障服务条件（pdf-parsing 3u/5u、planner 5u、summarizer 5u）各五类故障的横向条形图：(b) 故障期 CPU CV（条）对照同一次实验注入前 CPU CV（竖线刻度），(c) 故障期 CPU 落在自身注入前 p10--p90 范围内的百分比。以 CPU 为主，内存留在 CSV。覆盖比例是单指标的注入前范围覆盖率，不是分类准确率或联合分布重叠系数。原 `c2_variance_cv_box.pdf` 及其数据保留。
2. `figures/c1_abn_window_timeline.pdf`：单独绘制异常时间线，包括 CPU-stress 的资源与成功率轨迹以及五类故障的首次信号时刻。事件相对时间与原 QPS 互相关的偏移轴含义不同，因此不混入原 CCF 图。

## 正常与异常的波动和重叠

以每次实验自身注入前工作负载为基线：注入前 CPU CV 本身就大（约 0.34--1.05），故障期 CPU CV 与之量级相当（0.14--5.91），无法据 CV 稳定区分。故障期 CPU 落在自身注入前 p10--p90 的比例，在四个故障服务（pdf-parsing 3u/5u、planner 5u、summarizer 5u）与五类故障间从约 0% 跨到约 70%（如网络延迟：pdf 3u 67.6%、planner 51.4%、summarizer 70.3%；pod_kill：多数 51%--69% 高度重叠；CPU/内存压力普遍 <25%；pod_failure 因服务不可用 CPU 反而更低）。结论是缺乏跨故障类型、跨负载、跨故障服务一致可靠的指标边界，而非异常完全不可检测。

| service     | load    | metric           | fault       |   prefault_n |   infault_n |   prefault_cv |   infault_cv |   infault_within_prefault_p10_p90_pct |
|:------------|:--------|:-----------------|:------------|-------------:|------------:|--------------:|-------------:|--------------------------------------:|
| pdf-parsing | 3 users | CPU (mC)         | cpu_load    |           24 |          37 |         0.803 |        0.463 |                                18.919 |
| pdf-parsing | 3 users | Memory (MB)      | cpu_load    |           24 |          37 |         0.107 |        0.038 |                                 2.703 |
| pdf-parsing | 3 users | Latency p99 (ms) | cpu_load    |            0 |           0 |       nan     |      nan     |                               nan     |
| pdf-parsing | 3 users | Success rate     | cpu_load    |            0 |           0 |       nan     |      nan     |                               nan     |
| pdf-parsing | 3 users | CPU (mC)         | mem_load    |           24 |          37 |         0.379 |        0.625 |                                24.324 |
| pdf-parsing | 3 users | Memory (MB)      | mem_load    |           24 |          37 |         0.001 |        0.515 |                                 0     |
| pdf-parsing | 3 users | Latency p99 (ms) | mem_load    |            0 |           0 |       nan     |      nan     |                               nan     |
| pdf-parsing | 3 users | Success rate     | mem_load    |            0 |           0 |       nan     |      nan     |                               nan     |
| pdf-parsing | 3 users | CPU (mC)         | net_latency |           24 |          37 |         0.55  |        0.529 |                                67.568 |
| pdf-parsing | 3 users | Memory (MB)      | net_latency |           24 |          37 |         0     |        0.001 |                                 8.108 |
| pdf-parsing | 3 users | Latency p99 (ms) | net_latency |            6 |          21 |         0     |        0.331 |                                14.286 |
| pdf-parsing | 3 users | Success rate     | net_latency |           12 |          32 |         0     |        0     |                               100     |
| pdf-parsing | 3 users | CPU (mC)         | pod_failure |           24 |          37 |         0.344 |        3.355 |                                 8.108 |
| pdf-parsing | 3 users | Memory (MB)      | pod_failure |           24 |          37 |         0     |        3.124 |                                 8.108 |
| pdf-parsing | 3 users | Latency p99 (ms) | pod_failure |            9 |           6 |         0.565 |        0.499 |                               100     |
| pdf-parsing | 3 users | Success rate     | pod_failure |           15 |          18 |         0     |        0     |                               100     |
| pdf-parsing | 3 users | CPU (mC)         | pod_kill    |           24 |          37 |         0.476 |        0.979 |                                59.459 |
| pdf-parsing | 3 users | Memory (MB)      | pod_kill    |           24 |          37 |         0.017 |        0.446 |                                 0     |
| pdf-parsing | 3 users | Latency p99 (ms) | pod_kill    |            6 |           6 |         0.497 |        0.499 |                                50     |
| pdf-parsing | 3 users | Success rate     | pod_kill    |           13 |          17 |         0     |        0     |                               100     |
| pdf-parsing | 5 users | CPU (mC)         | cpu_load    |           24 |          37 |         1.05  |        0.442 |                                21.622 |
| pdf-parsing | 5 users | Memory (MB)      | cpu_load    |           24 |          37 |         0.187 |        0.013 |                                 5.405 |
| pdf-parsing | 5 users | Latency p99 (ms) | cpu_load    |            0 |          35 |       nan     |        0.283 |                               nan     |
| pdf-parsing | 5 users | Success rate     | cpu_load    |            0 |          35 |       nan     |        0     |                               nan     |
| pdf-parsing | 5 users | CPU (mC)         | mem_load    |           24 |          37 |         0.814 |        0.481 |                                 0     |
| pdf-parsing | 5 users | Memory (MB)      | mem_load    |           24 |          37 |         0.143 |        0.578 |                                 0     |
| pdf-parsing | 5 users | Latency p99 (ms) | mem_load    |            1 |          34 |       nan     |        0.005 |                                 5.882 |
| pdf-parsing | 5 users | Success rate     | mem_load    |            1 |          37 |       nan     |        0     |                               100     |
| pdf-parsing | 5 users | CPU (mC)         | net_latency |           24 |          36 |         0.796 |        1.529 |                                27.778 |
| pdf-parsing | 5 users | Memory (MB)      | net_latency |           24 |          36 |         0.176 |        0.004 |                                 0     |
| pdf-parsing | 5 users | Latency p99 (ms) | net_latency |            8 |          16 |         0.004 |        1.308 |                                81.25  |
| pdf-parsing | 5 users | Success rate     | net_latency |            8 |          22 |         0     |        0.075 |                                59.091 |
| pdf-parsing | 5 users | CPU (mC)         | pod_failure |           27 |          36 |         0.695 |        5.912 |                                 2.778 |
| pdf-parsing | 5 users | Memory (MB)      | pod_failure |           27 |          36 |         0.168 |        4.845 |                                 2.778 |
| pdf-parsing | 5 users | Latency p99 (ms) | pod_failure |           11 |          24 |         0.002 |        0.513 |                                16.667 |
| pdf-parsing | 5 users | Success rate     | pod_failure |           11 |          16 |         0     |        0     |                               100     |
| pdf-parsing | 5 users | CPU (mC)         | pod_kill    |           26 |          36 |         0.634 |        1.022 |                                66.667 |
| pdf-parsing | 5 users | Memory (MB)      | pod_kill    |           26 |          36 |         0.166 |        0.451 |                                72.222 |
| pdf-parsing | 5 users | Latency p99 (ms) | pod_kill    |            2 |          36 |         0     |        0.158 |                                 2.778 |
| pdf-parsing | 5 users | Success rate     | pod_kill    |            2 |          36 |         0     |        0     |                               100     |
| planner     | 5 users | CPU (mC)         | cpu_load    |           24 |          36 |         0.702 |        0.332 |                                 0     |
| planner     | 5 users | Memory (MB)      | cpu_load    |           24 |          36 |         0.174 |        0.083 |                                 0     |
| planner     | 5 users | Latency p99 (ms) | cpu_load    |           10 |          36 |         0.001 |        0.001 |                                69.444 |
| planner     | 5 users | Success rate     | cpu_load    |           10 |          36 |         0     |        0     |                               100     |
| planner     | 5 users | CPU (mC)         | mem_load    |           24 |          37 |         0.529 |        0.143 |                                 0     |
| planner     | 5 users | Memory (MB)      | mem_load    |           24 |          37 |         0.171 |        0.44  |                                 0     |
| planner     | 5 users | Latency p99 (ms) | mem_load    |           11 |          22 |         0.32  |        1.205 |                                59.091 |
| planner     | 5 users | Success rate     | mem_load    |           11 |          31 |         0     |        0     |                               100     |
| planner     | 5 users | CPU (mC)         | net_latency |           24 |          37 |         0.661 |        1.358 |                                51.351 |
| planner     | 5 users | Memory (MB)      | net_latency |           24 |          37 |         0.2   |        0.024 |                                 0     |
| planner     | 5 users | Latency p99 (ms) | net_latency |           11 |          13 |         0     |        0     |                                76.923 |
| planner     | 5 users | Success rate     | net_latency |           11 |          13 |         0     |        0     |                               100     |
| planner     | 5 users | CPU (mC)         | pod_failure |           26 |          36 |         0.591 |        2.828 |                                 0     |
| planner     | 5 users | Memory (MB)      | pod_failure |           26 |          36 |         0.19  |        2.659 |                                 0     |
| planner     | 5 users | Latency p99 (ms) | pod_failure |           13 |          36 |         0     |        1.254 |                                38.889 |
| planner     | 5 users | Success rate     | pod_failure |           13 |          14 |         0     |        0     |                               100     |
| planner     | 5 users | CPU (mC)         | pod_kill    |           25 |          36 |         0.624 |        0.734 |                                69.444 |
| planner     | 5 users | Memory (MB)      | pod_kill    |           25 |          36 |         0.222 |        0.361 |                                50     |
| planner     | 5 users | Latency p99 (ms) | pod_kill    |            0 |          33 |       nan     |        0     |                               nan     |
| planner     | 5 users | Success rate     | pod_kill    |            0 |          33 |       nan     |        0     |                               nan     |
| summarizer  | 5 users | CPU (mC)         | cpu_load    |           24 |          37 |         0.866 |        0.46  |                                10.811 |
| summarizer  | 5 users | Memory (MB)      | cpu_load    |           24 |          37 |         0.174 |        0.079 |                                 0     |
| summarizer  | 5 users | Latency p99 (ms) | cpu_load    |            5 |          37 |         0.431 |        0.804 |                                59.459 |
| summarizer  | 5 users | Success rate     | cpu_load    |            5 |          37 |         0     |        0     |                               100     |
| summarizer  | 5 users | CPU (mC)         | mem_load    |           24 |          37 |         0.771 |        0.431 |                                13.514 |
| summarizer  | 5 users | Memory (MB)      | mem_load    |           24 |          37 |         0.173 |        0.415 |                                 0     |
| summarizer  | 5 users | Latency p99 (ms) | mem_load    |           11 |          37 |         0.513 |        0.529 |                               100     |
| summarizer  | 5 users | Success rate     | mem_load    |           11 |          37 |         0     |        0.04  |                                75.676 |
| summarizer  | 5 users | CPU (mC)         | net_latency |           24 |          37 |         0.661 |        0.89  |                                70.27  |
| summarizer  | 5 users | Memory (MB)      | net_latency |           24 |          37 |         0.155 |        0.057 |                                 5.405 |
| summarizer  | 5 users | Latency p99 (ms) | net_latency |            6 |          18 |         0.012 |        1.208 |                                16.667 |
| summarizer  | 5 users | Success rate     | net_latency |            6 |          24 |         0     |        0     |                               100     |
| summarizer  | 5 users | CPU (mC)         | pod_failure |           25 |          36 |         0.909 |        2.49  |                                13.889 |
| summarizer  | 5 users | Memory (MB)      | pod_failure |           25 |          36 |         0.187 |        2.49  |                                 0     |
| summarizer  | 5 users | Latency p99 (ms) | pod_failure |           13 |          14 |         0.468 |        0.438 |                               100     |
| summarizer  | 5 users | Success rate     | pod_failure |           13 |          20 |         0     |        0     |                               100     |
| summarizer  | 5 users | CPU (mC)         | pod_kill    |           25 |          37 |         0.83  |        1.178 |                                51.351 |
| summarizer  | 5 users | Memory (MB)      | pod_kill    |           25 |          37 |         0.169 |        0.445 |                                16.216 |
| summarizer  | 5 users | Latency p99 (ms) | pod_kill    |            0 |          37 |       nan     |        0.505 |                               nan     |
| summarizer  | 5 users | Success rate     | pod_kill    |            0 |          37 |       nan     |        0.052 |                               nan     |

## 信号位置与时间错位

所有实验中故障服务的有效成功率观测均为 1.0，但 CPU / 内存压力期间是 0/37 个有效值；不能据此声称全程成功。其余三类故障区间分别为 32/37、18/37、17/37 个有效值。

CPU 压力注入期峰值 568 mC；planner 和 summarizer 的首次下降在故障开始后 400 秒，即结束后 219 秒，届时资源峰值已经过去。CPU、内存压力、pod failure、pod kill 的首次链路服务下降分别晚于故障结束 219、429、254、194 秒。planner 是入口，summarizer 是下游出口，不能把二者都称为下游服务。

网络延迟实验在注入前 75 秒已有下降；注入后首次下降在 +120 秒，仍在故障期内。保留这组观测，但不将它计入“故障结束后才首次下降”的四组。该例支持异常起点归属不明确，不能用于证明干净的故障后传播时延。

| fault       |   faulted_svc_infault_valid_samples |   faulted_svc_infault_total_samples |   first_drop_from_fault_start_s |   first_drop_lag_after_fault_end_s |   first_post_start_drop_from_fault_start_s | preexisting_drop   |
|:------------|------------------------------------:|------------------------------------:|--------------------------------:|-----------------------------------:|-------------------------------------------:|:-------------------|
| cpu_load    |                                   0 |                                  37 |                             400 |                                219 |                                        400 | False              |
| mem_load    |                                   0 |                                  37 |                             610 |                                429 |                                        610 | False              |
| net_latency |                                  32 |                                  37 |                             -75 |                               -256 |                                        120 | True               |
| pod_failure |                                  18 |                                  37 |                             435 |                                254 |                                        435 | False              |
| pod_kill    |                                  17 |                                  37 |                             375 |                                194 |                                        375 | False              |

## 论文衔接与证据边界

动机 2 保留正常数据的 metric lag、链路完成时间与动态路径论证，再接“部分故障指标被正常波动覆盖，局部起点难定”，然后接“链路其他服务的症状延迟或提前存在”，最终落到异常窗口需要结合请求执行时序。`motivation.tex` 保留原有数值与主体论证，统一正文、图注和图内的专业词汇为 metric lag、failure、normal workload、anomaly time windows 和 metrics。

- `c2c_abn_baseline_sensitivity.csv` 另列 1、3、5 用户和合并基线。1/5 用户的 PDF-parsing CPU 波动更小，且无该服务 p99 观测；跨负载混合会改变标准化偏移。因此正文限定为同负载对照，不沿用旧 4.5--5.2 sigma，也不声称基线无关。
- 正常与异常采集时长、任务组合不完全相同，五类故障各仅一次；图表为描述性证据，不给出分类准确率或显著性检验。
- `MetricCollector.py` 中 CPU 和成功率采用 1 分钟 rate 窗口，采样步长 5 秒。标签中停止负载与故障结束重合。不能仅据这些聚合曲线，把所有时差归因于故障传播或断言资源峰值仅持续于注入区间。
- 没有运行一个具体检测器；“从首次症状开始的窗口会错过故障”是由观测时间位置推出的结果。没有证明任意长度回溯窗口都会失败。
- 旧 `tables/c2_abn_vs_normal.csv` 是历史口径，不用于本次正文。当前定量依据以 `c2c_abn_vs_normal_overlap.csv`、`c2c_abn_baseline_sensitivity.csv`、`c1_abn_downstream_lag.csv` 和 `c1_abn_service_drop_times.csv` 为准。`downstream_drops` 是沿用的 CSV 列名，其中包含入口 planner，解读为其他链路服务。

## 复现

在仓库根目录运行 `.venv/bin/python analysis/abnormal_analysis.py`，生成两张 PDF / PNG、四张 CSV 和本报告。`analysis/run_all.py` 已将异常分析作为最后一步。正常图的独立生成逻辑保持不变。
