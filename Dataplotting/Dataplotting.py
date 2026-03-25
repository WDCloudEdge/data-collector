import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ==========================================
# 1. 核心算法：数据对齐与特征提取
# ==========================================
def process_locust_data(csv_file, steady_start_sec=120, max_sec=300):
    # 读取数据
    df = pd.read_csv(csv_file)

    # 清洗数据：Locust在请求量为0时，百分位延迟会写成 'N/A'，我们需要替换为 0
    df['95%'] = pd.to_numeric(df['95%'], errors='coerce').fillna(0)
    df['Requests/s'] = pd.to_numeric(df['Requests/s'], errors='coerce').fillna(0)

    # 找 T=0 (有数据的上一个0)
    # 找到第一个 User Count > 0 的索引
    active_mask = df['User Count'] > 0
    if not active_mask.any():
        raise ValueError(f"文件 {csv_file} 中没有有效的并发用户数据")

    first_active_idx = active_mask.idxmax()
    # T=0 定义为活跃的前一个点；如果一开始就活跃，就取第0个点
    start_idx = max(0, first_active_idx - 1)
    start_time = df.loc[start_idx, 'Timestamp']

    # 过滤 5分钟 (300秒) 内的数据
    df_5min = df[(df['Timestamp'] >= start_time) & (df['Timestamp'] <= start_time + max_sec)].copy()
    df_5min['Relative_Time'] = df_5min['Timestamp'] - start_time

    # 提取稳态数据 (比如 120秒 到 300秒)，去掉 Ramp-up 的抖动
    df_steady = df_5min[(df_5min['Relative_Time'] >= steady_start_sec)]

    if df_steady.empty:
        # 如果数据不够120秒，就拿全部5分钟数据硬算
        df_steady = df_5min

    # 计算均值和标准差
    metrics = {
        'rps_mean': df_steady['Requests/s'].mean(),
        'rps_std': df_steady['Requests/s'].std(),
        'lat_95_mean': df_steady['95%'].mean(),
        'lat_95_std': df_steady['95%'].std()
    }
    return metrics


try:
    cloud_30 = process_locust_data('cloud_30_stats_history.csv')
    cloud_90 = process_locust_data('cloud_90_stats_history.csv')
    edge_30  = process_locust_data('edge_30_stats_history.csv')
    edge_90  = process_locust_data('edge_90_stats_history.csv')
except FileNotFoundError as e:
    print(f"找不到文件: {e}。\n(这里使用模拟数据进行演示画图)")
    # 为了保证代码能直接跑，这里我放了模拟数据。
    # 当你放好真实的CSV后，把下面这几行删掉即可。
    cloud_30 = {'rps_mean': 120.5, 'rps_std': 5.2, 'lat_95_mean': 45.2, 'lat_95_std': 2.1}
    cloud_90 = {'rps_mean': 250.3, 'rps_std': 12.1, 'lat_95_mean': 110.5, 'lat_95_std': 8.5}
    edge_30  = {'rps_mean': 180.2, 'rps_std': 4.8, 'lat_95_mean': 25.4, 'lat_95_std': 1.5}
    edge_90  = {'rps_mean': 410.6, 'rps_std': 15.3, 'lat_95_mean': 60.2, 'lat_95_std': 4.2}

# 将处理好的数据打包给画图工具
group1_rps = [cloud_30['rps_mean'], cloud_90['rps_mean']]
group1_rps_err = [cloud_30['rps_std'], cloud_90['rps_std']]
group1_lat = [cloud_30['lat_95_mean'], cloud_90['lat_95_mean']]
group1_lat_err = [cloud_30['lat_95_std'], cloud_90['lat_95_std']]

group2_rps = [edge_30['rps_mean'], edge_90['rps_mean']]
group2_rps_err = [edge_30['rps_std'], edge_90['rps_std']]
group2_lat = [edge_30['lat_95_mean'], edge_90['lat_95_mean']]
group2_lat_err = [edge_30['lat_95_std'], edge_90['lat_95_std']]

# ==========================================
# 3. 科研级别画图设置
# ==========================================
labels = ['30 Users', '90 Users']

plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.size'] = 14
plt.rcParams['axes.linewidth'] = 1.5
plt.rcParams['xtick.major.width'] = 1.5
plt.rcParams['ytick.major.width'] = 1.5

color1, color2 = '#4A7EBB', '#C0504D'
hatch1, hatch2 = '//', 'xx'

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))

x = np.arange(len(labels))
width = 0.35

# --- 图 1: 吞吐量 (Throughput) ---
rects1 = ax1.bar(x - width/2, group1_rps, width, yerr=group1_rps_err,
                 label='Baseline (Cloud)', color=color1, edgecolor='black',
                 linewidth=1.2, hatch=hatch1, capsize=5)
rects2 = ax1.bar(x + width/2, group2_rps, width, yerr=group2_rps_err,
                 label='Proposed (Edge)', color=color2, edgecolor='black',
                 linewidth=1.2, hatch=hatch2, capsize=5)

ax1.set_ylabel('Throughput (Requests/sec)', fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(labels, fontweight='bold')
ax1.set_title('(a) System Throughput', y=-0.25, fontweight='bold')
ax1.grid(axis='y', linestyle='--', alpha=0.7)

# --- 图 2: 尾延迟 (95% Latency) ---
rects3 = ax2.bar(x - width/2, group1_lat, width, yerr=group1_lat_err,
                 color=color1, edgecolor='black', linewidth=1.2, hatch=hatch1, capsize=5)
rects4 = ax2.bar(x + width/2, group2_lat, width, yerr=group2_lat_err,
                 color=color2, edgecolor='black', linewidth=1.2, hatch=hatch2, capsize=5)

ax2.set_ylabel('95% Response Time (ms)', fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(labels, fontweight='bold')
ax2.set_title('(b) Tail Latency (95th Percentile)', y=-0.25, fontweight='bold')
ax2.grid(axis='y', linestyle='--', alpha=0.7)

# 图例设置
fig.legend([rects1, rects2], ['Baseline (Cloud)', 'Proposed (Edge)'],
           loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=2, frameon=False, prop={'weight':'bold'})

plt.tight_layout()

# 保存图片
plt.savefig("performance_results.pdf", bbox_inches='tight', dpi=300)
plt.savefig("performance_results.png", bbox_inches='tight', dpi=300)

plt.show()