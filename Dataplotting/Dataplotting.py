import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ==========================================
# 1. 核心算法：数据对齐与特征提取
# ==========================================
def process_locust_data(csv_file, steady_start_sec=120, max_sec=300):
    # 读取数据
    df = pd.read_csv(csv_file)

    # 【修复致命错误】：必须只提取 Name 为 'Aggregated' 的汇总行，否则画图和均值全是错的！
    df = df[df['Name'] == 'Aggregated'].copy()
    df.reset_index(drop=True, inplace=True)

    # 清洗数据：Locust在请求量为0时，百分位延迟会写成 'N/A'，我们需要替换为 0
    df['95%'] = pd.to_numeric(df['95%'], errors='coerce').fillna(0)
    df['Requests/s'] = pd.to_numeric(df['Requests/s'], errors='coerce').fillna(0)
    df['User Count'] = pd.to_numeric(df['User Count'], errors='coerce').fillna(0)

    # 找 T=0 (有数据的上一个0)
    active_mask = df['User Count'] > 0
    if not active_mask.any():
        raise ValueError(f"文件 {csv_file} 中没有有效的并发用户数据")

    first_active_idx = active_mask.idxmax()
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

    # 修改：返回 metrics 的同时，把对齐好的 df_5min 也返回去，用来画时序图
    return metrics, df_5min


# ==========================================
# 2. 读取实验数据
# ==========================================
try:
    cloud_30_metrics, cloud_30_df = process_locust_data('bookinfo_30_stats_history.csv')
    cloud_90_metrics, cloud_90_df = process_locust_data('bookinfo_90_stats_history.csv')
    edge_30_metrics, edge_30_df  = process_locust_data('bookinfo_agent_30_stats_history.csv')
    edge_90_metrics, edge_90_df  = process_locust_data('bookinfo_agent_90_stats_history.csv')
except FileNotFoundError as e:
    print(f"找不到文件: {e}。\n(请确保CSV文件在当前目录下)")
    exit()

# 将处理好的稳态数据打包给柱状图
group1_rps = [cloud_30_metrics['rps_mean'], cloud_90_metrics['rps_mean']]
group1_rps_err = [cloud_30_metrics['rps_std'], cloud_90_metrics['rps_std']]
group1_lat = [cloud_30_metrics['lat_95_mean'], cloud_90_metrics['lat_95_mean']]
group1_lat_err = [cloud_30_metrics['lat_95_std'], cloud_90_metrics['lat_95_std']]

group2_rps = [edge_30_metrics['rps_mean'], edge_90_metrics['rps_mean']]
group2_rps_err = [edge_30_metrics['rps_std'], edge_90_metrics['rps_std']]
group2_lat = [edge_30_metrics['lat_95_mean'], edge_90_metrics['lat_95_mean']]
group2_lat_err = [edge_30_metrics['lat_95_std'], edge_90_metrics['lat_95_std']]


# ==========================================
# 3. 全局科研级别画图设置
# ==========================================
# 兼容 WSL 环境，如果没有 Times New Roman 会自动使用 DejaVu Serif
plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif']
plt.rcParams['font.size'] = 14
plt.rcParams['axes.linewidth'] = 1.5
plt.rcParams['xtick.major.width'] = 1.5
plt.rcParams['ytick.major.width'] = 1.5

color1, color2 = '#4A7EBB', '#C0504D'
hatch1, hatch2 = '//', 'xx'
labels = ['30 Users', '90 Users']


# ==========================================
# 4. 绘制第一张图：稳态期对比 (柱状图) - 保留原有代码
# ==========================================
fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))
x = np.arange(len(labels))
width = 0.35

# 图 1a: 吞吐量 (Throughput)
rects1 = ax1.bar(x - width/2, group1_rps, width, yerr=group1_rps_err,
                 label='Bookinfo (only)', color=color1, edgecolor='black',
                 linewidth=1.2, hatch=hatch1, capsize=5)
rects2 = ax1.bar(x + width/2, group2_rps, width, yerr=group2_rps_err,
                 label='Bookinfo (with Agent Service)', color=color2, edgecolor='black',
                 linewidth=1.2, hatch=hatch2, capsize=5)
ax1.set_ylabel('Throughput (Requests/sec)', fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(labels, fontweight='bold')
ax1.set_title('(a) System Throughput', y=-0.25, fontweight='bold')
ax1.grid(axis='y', linestyle='--', alpha=0.7)

# 图 1b: 尾延迟 (95% Latency)
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
fig1.legend([rects1, rects2], ['Bookinfo (only)', 'Bookinfo (with Agent Service)'],
            loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=2, frameon=False, prop={'weight':'bold'})
fig1.tight_layout()

# 保存柱状图
fig1.savefig("performance_bar_results.pdf", bbox_inches='tight', dpi=300)
fig1.savefig("performance_bar_results.png", bbox_inches='tight', dpi=300)


# ==========================================
# 5. 绘制第二张图：5分钟生命周期 (时序图) - 新增代码
# ==========================================
fig2, (ax3, ax4) = plt.subplots(1, 2, figsize=(12, 5))

# 定义平滑窗口(解决Locust每秒采样带来的锯齿毛刺，让科研图表看起来更专业)
smooth_window = 3

# 准备画线的数据配置
plot_configs = [
    {'df': cloud_30_df, 'label': 'Bookinfo(only) - 30U',   'color': color1, 'ls': '-'},
    {'df': cloud_90_df, 'label': 'Bookinfo(only) - 90U',   'color': color1, 'ls': '--'},
    {'df': edge_30_df,  'label': 'Bookinfo(Agent) - 30U',  'color': color2, 'ls': '-'},
    {'df': edge_90_df,  'label': 'Bookinfo(Agent) - 90U',  'color': color2, 'ls': '--'}
]

for config in plot_configs:
    df = config['df']
    x_time = df['Relative_Time']
    # 使用 rolling().mean() 让曲线变得稍微平滑
    y_rps = df['Requests/s'].rolling(window=smooth_window, min_periods=1).mean()
    y_lat = df['95%'].rolling(window=smooth_window, min_periods=1).mean()

    # 图 2a: 吞吐量时序图
    ax3.plot(x_time, y_rps, label=config['label'], color=config['color'],
             linestyle=config['ls'], linewidth=2, alpha=0.85)

    # 图 2b: 延迟时序图
    ax4.plot(x_time, y_lat, label=config['label'], color=config['color'],
             linestyle=config['ls'], linewidth=2, alpha=0.85)

# --- 时序图 ax3 细节 ---
ax3.set_xlabel('Time (seconds)', fontweight='bold')
ax3.set_ylabel('Throughput (Requests/sec)', fontweight='bold')
ax3.set_title('(a) Throughput over Time', y=-0.25, fontweight='bold')
ax3.set_xlim(0, 300)
ax3.grid(True, linestyle='--', alpha=0.6)

# --- 时序图 ax4 细节 ---
ax4.set_xlabel('Time (seconds)', fontweight='bold')
ax4.set_ylabel('95% Response Time (ms)', fontweight='bold')
ax4.set_title('(b) 95th Percentile Latency over Time', y=-0.25, fontweight='bold')
ax4.set_xlim(0, 300)
ax4.grid(True, linestyle='--', alpha=0.6)

# 图例设置 (放在图表上方)
handles2, labels2 = ax3.get_legend_handles_labels()
fig2.legend(handles2, labels2, loc='upper center', bbox_to_anchor=(0.5, 1.12),
            ncol=4, frameon=False, prop={'weight':'bold', 'size': 12})
fig2.tight_layout()

# 保存时序图
fig2.savefig("performance_timeseries_results.pdf", bbox_inches='tight', dpi=300)
fig2.savefig("performance_timeseries_results.png", bbox_inches='tight', dpi=300)


# 在Windows/Mac下弹窗显示，在WSL下会忽略
try:
    plt.show()
except Exception:
    pass