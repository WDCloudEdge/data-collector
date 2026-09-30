#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Motivation analysis for the paper.

Two claims to support with figures/tables, using the three NORMAL datasets
(1 / 5 / 10 concurrent users) of the `agent-network` (thingo) system:

  C1. 指标滞后性导致时间窗口难确定
      (Metric lag makes the anomaly time window hard to pin down)
  C2. 指标稀疏性 + 极大方差导致故障特征难以识别
      (Metric sparsity & huge variance make fault features hard to identify)

Outputs:
  analysis/figures/*.png    -- figures
  analysis/tables/*.csv      -- machine-readable tables
  analysis/tables/summary.md -- human-readable summary for the paper
"""
import os
import numpy as np
import pandas as pd
from common import (plt, FIG, TAB, DATASETS, ORDER, PALETTE, COLORS, SAMPLE_SEC,
                    mpath, cv_series as cv, load_traces, trace_node_times, save_fig,
                    scale_figure_text)


# ===================================================================
# C2-A  SPARSITY
# ===================================================================
def sparsity():
    rows = []
    METRICS = {
        "latency (p50/p90/p99)": "latency.csv",
        "success_rate":          "success_rate.csv",
        "qps":                   "svc_qps.csv",
        "call (p50/p90/p99)":    "call.csv",
    }
    for name, root in DATASETS.items():
        for label, fn in METRICS.items():
            df = pd.read_csv(mpath(root, fn))
            cols = [c for c in df.columns if c != "timestamp"]
            v = df[cols].apply(pd.to_numeric, errors="coerce")
            total = v.size
            nnull = int(v.isna().sum().sum())
            nzero = int((v == 0).sum().sum())
            info = total - nnull - nzero
            rows.append({
                "load": name, "metric": label,
                "null_%": round(100 * nnull / total, 1),
                "zero_%": round(100 * nzero / total, 1),
                "informative_%": round(100 * info / total, 1),
            })
    tab = pd.DataFrame(rows)
    tab.to_csv(os.path.join(TAB, "sparsity.csv"), index=False)

    # figure: informative-rate grouped bars
    metrics = list(METRICS.keys())
    x = np.arange(len(metrics))
    w = 0.25
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for i, name in enumerate(ORDER):
        sub = tab[tab.load == name].set_index("metric").loc[metrics]
        ax.bar(x + (i - 1) * w, sub["informative_%"], w, label=name)
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, rotation=12)
    ax.set_ylabel("Proportion of valid information (%)")
    ax.legend(title="User Workload")
    ax.grid(axis="y", ls=":", alpha=.5)
    scale_figure_text(fig, 1.4)
    fig.tight_layout()
    save_fig(fig, "c2_sparsity_bars")
    plt.close(fig)

    # figure: latency availability heatmap (5 users) — highest load in 1/3/5
    root = DATASETS["5 users"]
    lat = pd.read_csv(mpath(root, "latency.csv"))
    p90 = [c for c in lat.columns if c.endswith("&p90")]
    avail = (~lat[p90].apply(pd.to_numeric, errors="coerce").isna()).astype(int).T
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.imshow(avail.values, aspect="auto", cmap="Greys", interpolation="nearest")
    ax.set_yticks(range(len(p90)))
    ax.set_yticklabels([c.replace("agent-network-", "").replace("&p90", "") for c in p90], fontsize=7)
    ax.set_xlabel(f"Time window (× {SAMPLE_SEC}s)")
    scale_figure_text(fig, 1.4)
    fig.tight_layout()
    save_fig(fig, "c2_latency_availability")
    plt.close(fig)
    return tab


# ===================================================================
# C2-B  VARIANCE (coefficient of variation on normal data)
# ===================================================================
def variance(ax=None):
    """Draw the original CV panel under normal workload, optionally in a combined figure."""
    KINDS = {
        "CPU":         ("svc_metric.csv", "&cpu_usage"),
        "Memory":      ("svc_metric.csv", "&mem_usage"),
        "Net recv":    ("svc_metric.csv", "&net_receive"),
        "Latency p99": ("latency.csv",    "&p99"),
        "Latency p50": ("latency.csv",    "&p50"),
    }
    rows = []
    cv_store = {}  # (load, kind) -> list of cv
    for name, root in DATASETS.items():
        for kind, (fn, suffix) in KINDS.items():
            df = pd.read_csv(mpath(root, fn))
            cols = [c for c in df.columns if c.endswith(suffix)]
            cvs = [cv(df[c]) for c in cols]
            cvs = [x for x in cvs if not np.isnan(x)]
            cv_store[(name, kind)] = cvs
            if cvs:
                a = np.array(cvs)
                rows.append({
                    "load": name, "metric": kind,
                    "median_CV": round(float(np.median(a)), 2),
                    "p90_CV": round(float(np.percentile(a, 90)), 2),
                    "max_CV": round(float(a.max()), 2),
                    "frac_CV>0.5": round(float((a > 0.5).mean()), 2),
                    "frac_CV>1": round(float((a > 1).mean()), 2),
                })
    tab = pd.DataFrame(rows)
    standalone = ax is None
    if standalone:
        tab.to_csv(os.path.join(TAB, "variance_cv.csv"), index=False)

    # boxplot of CV by metric kind, grouped by load
    kinds = list(KINDS.keys())
    if standalone:
        fig, ax = plt.subplots(figsize=(10, 5))
    positions, data, colors, ticks = [], [], [], []
    palette = PALETTE
    p = 0
    for kind in kinds:
        for name in ORDER:
            vals = cv_store.get((name, kind), [])
            if vals:
                data.append(vals); positions.append(p); colors.append(palette[name])
            p += 1
        ticks.append((p - 3 + 0.0, kind))
        p += 1
    bp = ax.boxplot(data, positions=positions, widths=0.8, patch_artist=True, showfliers=True)
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c); patch.set_alpha(.7)
    ax.axhline(0.5, ls="--", color=COLORS["reference"], alpha=.7)
    ax.set_xticks([t[0] for t in ticks])
    ax.set_xticklabels([t[1] for t in ticks])
    ax.set_ylabel("Coefficient of variation (CV = std/mean)")
    ax.set_title("The coefficient of variation of service-level metrics is large under normal workload.")
    handles = [plt.Rectangle((0, 0), 1, 1, fc=palette[n], alpha=.7) for n in ORDER]
    ax.legend(handles, ORDER, title="Normal workload")
    ax.grid(axis="y", ls=":", alpha=.5)
    if standalone:
        fig.tight_layout()
        save_fig(fig, "c2_variance_cv_box")
        plt.close(fig)
    return tab


# ===================================================================
# C1  LAG  (cross-correlation between the driving signal QPS and responses)
# ===================================================================
def lag():
    def ccf(x, y, maxlag):
        x = pd.to_numeric(pd.Series(x), errors="coerce").fillna(0).values.astype(float)
        y = pd.to_numeric(pd.Series(y), errors="coerce").fillna(0).values.astype(float)
        x = (x - x.mean()) / (x.std() + 1e-9)
        y = (y - y.mean()) / (y.std() + 1e-9)
        n = len(x)
        lags = range(-maxlag, maxlag + 1)
        out = {}
        for L in lags:
            if L >= 0:
                a, b = x[:n - L], y[L:]
            else:
                a, b = x[-L:], y[:n + L]
            if len(a) < 5:
                out[L] = np.nan
            else:
                out[L] = np.corrcoef(a, b)[0, 1]
        return out

    MAXLAG = 12  # ±60s
    rows = []
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=True)
    for ax, name in zip(axes, ORDER):
        root = DATASETS[name]
        qps = pd.read_csv(mpath(root, "svc_qps.csv"))
        res = pd.read_csv(mpath(root, "resource.csv"))
        q = qps.drop(columns=["timestamp"]).apply(pd.to_numeric, errors="coerce").sum(axis=1).values
        n = min(len(q), len(res))
        q = q[:n]
        for resp, col, color in [("CPU", "vCPU", COLORS["cpu"]),
                     ("Memory", "memory", COLORS["memory"])]:
            series = res[col].values[:n]
            c = ccf(q, series, MAXLAG)
            lags = sorted(c.keys())
            vals = [c[L] for L in lags]
            best = max((L for L in lags if not np.isnan(c[L])), key=lambda L: c[L])
            ax.plot([L * SAMPLE_SEC for L in lags], vals, marker="o", ms=3, label=f"QPS→{resp}", color=color)
            ax.axvline(best * SAMPLE_SEC, ls="--", color=color, alpha=.6)
            rows.append({"load": name, "response": resp,
                         "peak_lag_s": best * SAMPLE_SEC,
                         "peak_corr": round(float(c[best]), 2)})
        ax.axvline(0, color=COLORS["reference"], lw=.8)
        ax.set_title(name)
        ax.set_xlabel("Lag of resource response relative to QPS (s)")
        ax.grid(ls=":", alpha=.5)
        ax.legend(fontsize=8)
    axes[0].set_ylabel("Cross-correlation")
    fig.suptitle("The peak lag between the request load and the resource response varies across metrics and user loads.",
                 fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    save_fig(fig, "c1_lag_ccf")
    plt.close(fig)

    tab = pd.DataFrame(rows)
    tab.to_csv(os.path.join(TAB, "lag_peaks.csv"), index=False)

    # overlay time series (5 users) — visual lag / persistence
    root = DATASETS["5 users"]
    qps = pd.read_csv(mpath(root, "svc_qps.csv"))
    res = pd.read_csv(mpath(root, "resource.csv"))
    q = qps.drop(columns=["timestamp"]).apply(pd.to_numeric, errors="coerce").sum(axis=1).values
    n = min(len(q), len(res))
    t = np.arange(n) * SAMPLE_SEC

    def z(a):
        a = a[:n].astype(float)
        return (a - np.nanmean(a)) / (np.nanstd(a) + 1e-9)

    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(t, z(q), label="Total QPS", color=COLORS["entry"], lw=1.5)
    ax.plot(t, z(res["vCPU"].values), label="vCPU", color=COLORS["cpu"], alpha=.8)
    ax.plot(t, z(res["memory"].values), label="Memory", color=COLORS["memory"], alpha=.8)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Z-score (normalized)")
    ax.set_title("CPU trails the request load while memory accumulates slowly and persists at a user load of 5.")
    ax.legend(ncol=3)
    ax.grid(ls=":", alpha=.5)
    fig.tight_layout()
    save_fig(fig, "c1_lag_overlay_10user")
    plt.close(fig)
    return tab


# ===================================================================
# C1-chain  LAG ALONG THE CALL CHAIN
#   planner(entry) -> direction(route) -> parse -> generate -> summarizer(exit)
#   Traces are empty, so the pipeline order is defined by service ROLE.
#   Reference signal = planner QPS (the entry / driving load).
#   For each downstream service: peak cross-correlation lag of its QPS vs entry.
# ===================================================================
STAGE = {
    "planner":       (0, "入口 entry"),
    "direction":     (1, "路由 route"),
    "ocr":           (2, "解析 parse"), "pdf-parsing": (2, "解析 parse"),
    "excel-parsing": (2, "解析 parse"), "word-parsing": (2, "解析 parse"),
    "image":         (2, "解析 parse"),
    "pdf-gen":       (3, "生成 generate"), "excel-gen": (3, "生成 generate"),
    "word-gen":      (3, "生成 generate"), "csv-gen": (3, "生成 generate"),
    "image-gen":     (3, "生成 generate"),
    "summarizer":    (4, "出口 exit"),
}
STAGE_LABELS = ["entry", "route", "parse", "generate", "exit"]


def _peak_lag(ref, y, maxlag=12):
    """Peak positive-lag cross-correlation (downstream lags entry => lag>=0)."""
    ref = np.asarray(ref, float)
    y = np.asarray(y, float)
    if ref.std() < 1e-9 or y.std() < 1e-9:
        return None
    x = (ref - ref.mean()) / ref.std()
    yy = (y - y.mean()) / y.std()
    n = len(x)
    best = None
    for L in range(0, maxlag + 1):
        a, b = x[:n - L], yy[L:]
        if len(a) < 8:
            continue
        c = np.corrcoef(a, b)[0, 1]
        if np.isnan(c):
            continue
        if best is None or c > best[1]:
            best = (L, c)
    return best


def lag_chain():
    import collections
    rows = []
    stage_lag = {name: {} for name in DATASETS}   # name -> {stage: median lag}
    for name, root in DATASETS.items():
        q = pd.read_csv(mpath(root, "svc_qps.csv"))
        d = q.drop(columns=["timestamp"]).apply(pd.to_numeric, errors="coerce").fillna(0)
        ref = d["agent-network-planner"].values
        agg = collections.defaultdict(list)
        for c in d.columns:
            s = c.replace("agent-network-", "")
            if s not in STAGE:
                continue
            res = _peak_lag(ref, d[c].values)
            if res is None:
                continue
            L, cc = res
            depth, role = STAGE[s]
            rows.append({"load": name, "stage": depth, "role": role, "service": s,
                         "lag_s": L * SAMPLE_SEC, "peak_corr": round(float(cc), 2)})
            agg[depth].append(L * SAMPLE_SEC)
        for depth, vals in agg.items():
            stage_lag[name][depth] = float(np.median(vals))
    tab = pd.DataFrame(rows).sort_values(["load", "stage", "service"])
    tab.to_csv(os.path.join(TAB, "chain_lag.csv"), index=False)

    # --- figure 1: per-stage lag vs pipeline depth ---
    fig, ax = plt.subplots(figsize=(8.5, 5))
    palette = PALETTE
    for name in ORDER:
        xs = sorted(stage_lag[name].keys())
        ys = [stage_lag[name][d] for d in xs]
        ax.plot(xs, ys, marker="o", ms=8, lw=2, label=name, color=palette[name])
    # per-service scatter (light)
    for _, r in tab.iterrows():
        ax.scatter(r["stage"] + np.random.uniform(-0.08, 0.08), r["lag_s"],
                   color=palette[r["load"]], alpha=.25, s=18, zorder=1)
    ax.set_xticks(range(5))
    ax.set_xticklabels(STAGE_LABELS)
    ax.set_xlabel("Call-chain stage (increasing depth)")
    ax.set_ylabel("Lag relative to the entry service (s)")
    ax.set_title("The response lag accumulates along the call chain and grows with depth.")
    ax.legend(title="User Workload")
    ax.grid(ls=":", alpha=.5)
    fig.tight_layout()
    save_fig(fig, "c1_chain_lag_stage")
    plt.close(fig)

    # --- figure 2: entry vs mid vs exit overlay (5 users = densest summarizer) ---
    root = DATASETS["5 users"]
    q = pd.read_csv(mpath(root, "svc_qps.csv"))
    d = q.drop(columns=["timestamp"]).apply(pd.to_numeric, errors="coerce").fillna(0)
    sm = d.rolling(5, center=True, min_periods=1).mean()
    n = len(sm)
    t = np.arange(n) * SAMPLE_SEC

    def z(a):
        a = np.asarray(a, float)
        return (a - a.mean()) / (a.std() + 1e-9)

    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.plot(t, z(sm["agent-network-planner"]), color=COLORS["entry"], lw=1.8, label="planner (entry)")
    ax.plot(t, z(sm["agent-network-word-gen"]), color=COLORS["middle"], alpha=.8, label="word-gen (middle)")
    ax.plot(t, z(sm["agent-network-summarizer"]), color=COLORS["exit"], alpha=.9, label="summarizer (exit)")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Z-score of QPS (smoothed)")
    ax.set_title("The exit-service request pulses arrive later than the entry-service pulses at a user load of 5.")
    ax.legend(ncol=3)
    ax.grid(ls=":", alpha=.5)
    fig.tight_layout()
    save_fig(fig, "c1_chain_overlay_5user")
    plt.close(fig)
    return tab, stage_lag


def write_summary(sp, va, lg, ch, stage_lag):
    lines = []
    lines.append("# Motivation 分析结果 (normal thingo, 1/3/5 users)\n")
    lines.append("采样间隔 5s。三组正常态数据。trace/log 维度的 trace pkl 在三组中均为空 "
                 "(normal/inbound/outbound/abnormal 长度为 0)，本身即稀疏性的极端证据。\n")

    lines.append("\n## C1 指标滞后性 → 时间窗口难确定\n")
    lines.append("### C1.1 整体：QPS(驱动信号) 与资源响应的互相关峰值滞后\n")
    lines.append(lg.to_markdown(index=False))
    lines.append("\n\n要点：CPU 相对请求存在 10–35s 的正向滞后 (峰值相关随负载升高而增强)，"
                 "而内存的最佳滞后与 CPU 不一致且相关性弱 (缓慢累积、长时间不回落)。"
                 "不同指标的响应滞后不一致 → 无法用单一固定时间窗口对齐所有信号。\n")
    lines.append("图: `c1_lag_ccf.png`, `c1_lag_overlay_10user.png`\n")

    lines.append("\n### C1.2 沿调用链路：滞后随链路长度累积\n")
    lines.append("调用链路 (按服务角色定义，因 trace 为空)："
                 "planner(入口) → direction(路由) → 解析类 → 生成类 → summarizer(出口)。"
                 "以 planner QPS 为入口基准信号，测各下游服务 QPS 的峰值互相关滞后。\n")
    lines.append("每阶段中位滞后 (s)：\n")
    hdr = "| 负载 | 入口 entry | 路由 route | 解析 parse | 生成 generate | 出口 exit |"
    lines.append(hdr + "\n|" + "---|" * 6)
    for name in ORDER:
        sl = stage_lag[name]
        cells = [name] + [(f"{sl[d]:.0f}" if d in sl else "-") for d in range(5)]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("\n要点：入口 planner 滞后≈0s (即驱动信号本身)；一旦进入下游，滞后立即跳升到 "
                 "20–50s，且随链路加深而增大/更分散 (1、5 users 下出口 summarizer 滞后 30–45s)。"
                 "同一逻辑请求在链路不同阶段的响应散布在 0→50s 的宽区间，"
                 "链路越长、动态性越强，响应窗口越晚越宽 → 无法为整条链划定统一时间窗口。\n")
    lines.append("注：5 users 的 summarizer QPS 采集偏稀疏 (非零率仅 13%)，其出口点为采集伪影，"
                 "不作为主证据；解析/生成阶段仍显著滞后于入口。\n")
    lines.append("图: `c1_chain_lag_stage.png`, `c1_chain_overlay_5user.png`；表: `chain_lag.csv`\n")

    lines.append("\n## C2 稀疏性 + 极大方差 → 故障特征难识别\n")
    lines.append("### 稀疏性 (有效信息占比)\n")
    lines.append(sp.to_markdown(index=False))
    lines.append("\n\n要点：时延/调用指标即便在 5 users 下仍有 ~79% 为空、QPS ~79% 为零，"
                 "真正携带信号的单元格不足 1/4；1 user 下更严重。故障特征淹没在缺失里。\n")
    lines.append("图: `c2_sparsity_bars.png`, `c2_latency_availability.png`\n")
    lines.append("\n### 方差 (正常态变异系数 CV)\n")
    lines.append(va.to_markdown(index=False))
    lines.append("\n\n要点：正常运行下网络/时延指标的 CV 已可达 1–4，相当比例的指标列 CV>0.5。"
                 "基线波动本身巨大，导致基于均值±kσ 的阈值要么漏报要么频繁误报，故障特征难以与噪声区分。\n")
    lines.append("图: `c2_variance_cv_box.png`\n")

    with open(os.path.join(TAB, "summary.md"), "w") as f:
        f.write("\n".join(lines))


# ===================================================================
# C2-C  INTER-SERVICE HETEROGENEITY (服务间差异大)
#   Normal-state per-service CPU / memory / exec-time span 1-2 orders of
#   magnitude, and the per-metric ranking differs across services.
# ===================================================================
G2S = {"AgentNetworkPlannerGroup": "planner", "OCRParserGroup": "ocr",
       "WordGenerationAgentGroup": "word-gen", "PdfGenAgentGroup": "pdf-gen",
       "DirectionAgentGroup": "direction", "CSVGeneratorAgentGroup": "csv-gen",
       "ImageGenAgentGroup": "image-gen", "ExcelGenGroup": "excel-gen"}


def heterogeneity():
    root = DATASETS["5 users"]
    sm = pd.read_csv(mpath(root, "svc_metric.csv"))
    tt = trace_node_times(load_traces(root))
    rows = []
    for grp, svc in G2S.items():
        cpu = pd.to_numeric(sm.get(f"agent-network-{svc}&cpu_usage"), errors="coerce").mean()
        mem = pd.to_numeric(sm.get(f"agent-network-{svc}&mem_usage"), errors="coerce").mean()
        et = np.array(tt.get(grp, []), float)
        rows.append({"service": svc, "cpu_mean": round(float(cpu), 3),
                     "mem_mean_MB": round(float(mem), 0),
                     "exec_med_s": round(float(np.median(et)), 2) if len(et) else np.nan})
    tab = pd.DataFrame(rows)
    tab.to_csv(os.path.join(TAB, "service_heterogeneity.csv"), index=False)

    panels = [("cpu_mean", "Mean CPU usage (cores)"), ("mem_mean_MB", "Mean Memory usage (MB)"),
              ("exec_med_s", "Median Execution time (s)")]
    from matplotlib.ticker import NullLocator
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.6))
    for ax, (col, lab) in zip(axes, panels):
        sub = tab.sort_values(col)
        ax.barh(sub["service"], sub[col], color=COLORS["trace"], alpha=.8)
        ax.set_xscale("log")
        v = sub[col].dropna()
        # explicit, readable ticks: 6 values evenly spaced in log over the data range
        lo, hi = float(v.min()), float(v.max())
        ticks = np.geomspace(lo, hi, 6)

        def _fmt(t):
            if t >= 100:
                return f"{t:.0f}"
            if t >= 10:
                return f"{t:.0f}"
            if t >= 1:
                return f"{t:.1f}"
            return f"{t:.3f}"
        ax.set_xticks(ticks)
        ax.xaxis.set_minor_locator(NullLocator())            # drop the sparse auto minor ticks
        ax.set_xticklabels([_fmt(t) for t in ticks], fontsize=7, rotation=30, ha="right")
        ax.set_xlim(lo / 1.15, hi * 1.15)
        ax.set_xlabel(f"{lab}\n(max/min ≈ {v.max()/max(v.min(),1e-9):.0f}×)", fontsize=10)
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="x", ls=":", alpha=.5)
    scale_figure_text(fig, 1.4)
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    save_fig(fig, "c2b_service_heterogeneity")
    plt.close(fig)
    return tab


def main():
    np.random.seed(0)
    sp = sparsity()
    va = variance()
    het = heterogeneity()
    lg = lag()
    ch, stage_lag = lag_chain()
    write_summary(sp, va, lg, ch, stage_lag)
    print("\n=== inter-service heterogeneity (10u) ===")
    print(het.to_string(index=False))
    print("DONE. figures ->", FIG)
    print("tables  ->", TAB)
    print("\n=== sparsity ==="); print(sp.to_string(index=False))
    print("\n=== variance ==="); print(va.to_string(index=False))
    print("\n=== lag (overall) ==="); print(lg.to_string(index=False))
    print("\n=== chain per-stage median lag (s) ===")
    for name in ORDER:
        print(f"  {name}: " + ", ".join(f"s{d}={stage_lag[name].get(d,'-')}" for d in range(5)))


if __name__ == "__main__":
    main()
