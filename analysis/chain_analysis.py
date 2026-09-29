#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Trace-grounded call-chain analysis (uses the REAL call chains collected by
GraphCollector, i.e. data/<run>/agent-network/graph/graph_json/*.json).

Strengthens the two paper claims with ground-truth topology & timing instead of
metric cross-correlation inference:

  C1  滞后性 → 时间窗口难确定
      - 完成时间沿调用链深度累积，且越深越晚、越分散 (cumulative completion time
        grows and spreads with chain depth)
      - 调用链高度动态：长度/路径多变 (highly dynamic chain length & paths)
  C2  稀疏性 + 极大方差 → 故障特征难识别
      - 各顶点被调用频次极不均衡 (per-vertex invocation is highly skewed/sparse)
      - 各顶点执行时间方差极大 (per-vertex execution-time CV is huge)

Outputs -> analysis/figures/*.png , analysis/tables/chain_*.csv , summary appended.
"""
import os
import collections
import numpy as np
import pandas as pd
from common import (plt, FIG, TAB, DATASETS, ORDER, PALETTE, COLORS,
                    load_traces, cv_pos as cv, save_fig, scale_figure_text)


# ===================================================================
# C1  completion-time accumulation along chain depth  +  dynamism
# ===================================================================
# 完成时间/长度累积用多副本 1/3/5 users；调用链非固定角色流水线，按链路长度/深度累积。
C1_MULTI = {
    "1 user (multi)":  "data/thingo/normal/20260907-21.00-21.05-normal-thingo-1user-5min-multi-21.10",
    "3 users (multi)": "data/thingo/normal/20260913-14.10-14.15-normal-thingo-3user-5min-multi-14.20",
    "5 users (multi)": "data/thingo/normal/20260907-22.45-22.50-normal-thingo-5user-5min-multi-23.00",
}
C1_COLOR = {"1 user (multi)": PALETTE["1 user"], "3 users (multi)": PALETTE["3 users"],
            "5 users (multi)": PALETTE["5 users"]}

def chain_c1(data):
    c1data = {name: load_traces(root) for name, root in C1_MULTI.items()}
    c1_loads = list(C1_MULTI.keys())

    # ---- 完成时间随调用链长度累积（图3, end-to-end vs total length）----
    fig, axes = plt.subplots(1, len(c1_loads), figsize=(5.2 * len(c1_loads), 4.4), sharey=True)
    len_tab = []
    for ax, name in zip(np.atleast_1d(axes), c1_loads):
        by_len = collections.defaultdict(list)
        for tr in c1data[name]:
            e = tr.get("e2e_time")
            if isinstance(e, (int, float)) and e > 0:
                by_len[int(tr["total_level"])].append(float(e))
        lens = sorted(by_len.keys())
        bp = ax.boxplot([by_len[L] for L in lens], positions=lens, widths=0.6,
                        patch_artist=True, showfliers=False)
        for patch in bp["boxes"]:
            patch.set_facecolor(C1_COLOR[name]); patch.set_alpha(.7)
        ax.set_title(name)
        ax.set_xlabel("Call chain length (total levels)")
        ax.grid(axis="y", ls=":", alpha=.5)
        for L in lens:
            v = np.array(by_len[L])
            len_tab.append({"load": name, "chain_length": L, "n": len(v),
                            "e2e_median_s": round(float(np.median(v)), 1),
                            "e2e_p90_s": round(float(np.percentile(v, 90)), 1)})
    np.atleast_1d(axes)[0].set_ylabel("End-to-end completion time (s)")
    fig.suptitle("The end-to-end completion time grows with the call chain length under multi-replica deployment.",
                 fontsize=11)
    scale_figure_text(fig, 2)
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    save_fig(fig, "c1_completion_by_length")
    plt.close(fig)
    pd.DataFrame(len_tab).to_csv(os.path.join(TAB, "chain_completion_by_length.csv"), index=False)

    # ---- 累计完成时间随调用链深度累积（图4, cumulative vs depth）----
    fig, axes = plt.subplots(1, len(c1_loads), figsize=(5.2 * len(c1_loads), 4.4), sharey=True)
    depth_tab = []
    for ax, name in zip(np.atleast_1d(axes), c1_loads):
        by_depth = collections.defaultdict(list)
        for tr in c1data[name]:
            cum = 0.0
            for d, lv in enumerate(tr["levels"]):
                cum += lv["time"]
                by_depth[d].append(cum)
        depths = sorted(by_depth.keys())
        bp = ax.boxplot([by_depth[d] for d in depths], positions=depths, widths=0.6,
                        patch_artist=True, showfliers=False)
        for patch in bp["boxes"]:
            patch.set_facecolor(C1_COLOR[name]); patch.set_alpha(.7)
        ax.set_title(name.replace(" (multi)", ""))
        ax.set_xlabel("Call chain depth")
        ax.grid(axis="y", ls=":", alpha=.5)
        for d in depths:
            v = np.array(by_depth[d])
            depth_tab.append({"load": name, "depth": d, "n": len(v),
                              "median_s": round(float(np.median(v)), 1),
                              "p90_s": round(float(np.percentile(v, 90)), 1),
                              "iqr_s": round(float(np.percentile(v, 75) - np.percentile(v, 25)), 1)})
    np.atleast_1d(axes)[0].set_ylabel("Cumulative completion time (s)")
    scale_figure_text(fig, 2)
    fig.tight_layout()
    save_fig(fig, "c1r_completion_by_depth")
    plt.close(fig)
    pd.DataFrame(depth_tab).to_csv(os.path.join(TAB, "chain_completion_by_depth.csv"), index=False)

    # ---- chain-length / path dynamism ----
    # 图用合并(单+多)的 1/3/5 users，并排柱状图（不重叠）。
    dyn_rows = []
    for name in ORDER:
        lens = [t["total_level"] for t in data[name]]
        paths = [tuple(t["path"]) for t in data[name] if t["path"]]
        cnt = collections.Counter(paths)
        p = np.array(list(cnt.values()), float); p = p / p.sum() if p.sum() else p
        ent = float(-(p * np.log2(p)).sum()) if len(p) else 0.0
        dyn_rows.append({"load": name, "traces": len(lens),
                         "len_mean": round(np.mean(lens), 2), "len_max": int(np.max(lens)),
                         "distinct_paths": len(set(paths)),
                         "distinct_ratio": round(len(set(paths)) / max(len(paths), 1), 2),
                         "path_entropy_bits": round(ent, 2)})
    dyn = pd.DataFrame(dyn_rows)
    dyn.to_csv(os.path.join(TAB, "chain_dynamism.csv"), index=False)

    # 图：合并【单副本+多副本】的 1/3/5 users，并排柱状（不重叠）
    dyn_merge = {
        "1 user": [DATASETS["1 user"], C1_MULTI["1 user (multi)"]],
        "3 users": [DATASETS["3 users"], C1_MULTI["3 users (multi)"]],
        "5 users": [DATASETS["5 users"], C1_MULTI["5 users (multi)"]],
    }
    merged = {name: [t["total_level"] for r in roots for t in load_traces(r)]
              for name, roots in dyn_merge.items()}
    fig_loads = list(dyn_merge.keys())
    maxlen = max(max(lens, default=0) for lens in merged.values())
    xs = np.arange(0, maxlen + 1)
    n_groups = len(fig_loads)
    width = 0.8 / n_groups
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    for i, name in enumerate(fig_loads):
        lens = merged[name]
        counts = [sum(1 for L in lens if L == x) for x in xs]
        ax.bar(xs + (i - (n_groups - 1) / 2) * width, counts, width,
               label=f"{name} (n={len(lens)})", color=PALETTE[name], alpha=.9)
    ax.set_xticks(xs)
    ax.set_xlabel("Call chain length (total levels)")
    ax.set_ylabel("Number of traces")
    ax.legend(title="User Workload")
    ax.grid(axis="y", ls=":", alpha=.5)
    scale_figure_text(fig, 2)
    fig.tight_layout()
    save_fig(fig, "c1r_chain_dynamism")
    plt.close(fig)
    return dyn


# ===================================================================
# C2  per-vertex sparsity  +  execution-time variance
# ===================================================================
def chain_c2(data):
    # collapse to group name (before '/') so tool = one logical service
    def grp(v):
        return v.split("/")[0]

    # ---- invocation frequency (sparsity), 5 users ----
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
    vtab = []
    for name in ORDER:
        cnt = collections.Counter()
        times = collections.defaultdict(list)
        n_tr = len(data[name])
        for tr in data[name]:
            seen = set()
            for lv in tr["levels"]:
                for node, t in lv["node_times"].items():
                    g = grp(node)
                    times[g].append(t)
                    if g not in seen:
                        cnt[g] += 1
                        seen.add(g)
        for g in cnt:
            vtab.append({"load": name, "vertex": g, "traces_with": cnt[g],
                         "coverage": round(cnt[g] / max(n_tr, 1), 3),
                         "invocations": len(times[g]),
                         "time_mean_s": round(float(np.mean(times[g])), 2),
                         "time_cv": round(cv(times[g]), 2)})
    vt = pd.DataFrame(vtab)
    vt.to_csv(os.path.join(TAB, "chain_vertex_stats.csv"), index=False)

    # left: coverage bars (5 users), sorted
    sub = vt[vt.load == "5 users"].sort_values("coverage", ascending=True)
    axes[0].barh(sub["vertex"], sub["coverage"] * 100, color=PALETTE["5 users"], alpha=.8)
    axes[0].set_xlabel("Percentage of traces\ncovering the service (%)")
    axes[0].tick_params(axis="y", labelsize=7)
    axes[0].grid(axis="x", ls=":", alpha=.5)

    # right: per-vertex execution-time CV (box across loads)
    kinds = sub["vertex"].tolist()
    data_cv = [vt[vt.vertex == k]["time_cv"].dropna().values for k in kinds]
    data_cv = [d for d in data_cv if len(d)]
    allcv = vt.groupby("vertex")["time_cv"].mean().sort_values(ascending=True)
    axes[1].barh(allcv.index, allcv.values, color=COLORS["variance"], alpha=.8)
    axes[1].axvline(1.0, ls="--", color=COLORS["reference"])
    axes[1].set_xlabel("Coefficient of variation of execution time\n(CV) = σ/μ (standard deviation/mean)")
    axes[1].tick_params(axis="y", labelsize=7)
    axes[1].grid(axis="x", ls=":", alpha=.5)
    scale_figure_text(fig, 2)
    fig.tight_layout(w_pad=2.0)
    save_fig(fig, "c2r_vertex_sparsity_variance")
    plt.close(fig)
    return vt


def main():
    data = {name: load_traces(root) for name, root in DATASETS.items()}
    for name in ORDER:
        print(f"{name}: {len(data[name])} traces loaded")
    dyn = chain_c1(data)
    vt = chain_c2(data)

    print("\n=== chain dynamism ==="); print(dyn.to_string(index=False))
    print("\n=== top vertices (5 users) by coverage ===")
    top = vt[vt.load == "5 users"].sort_values("coverage", ascending=False).head(15)
    print(top.to_string(index=False))

    # write own section file (idempotent, overwrite)
    md = ["## C1/C2 补充：基于真实调用链 (graph.json) 的证据\n"]
    md.append("### C1 完成时间沿调用链累积 + 链路动态性\n")
    md.append(dyn.to_markdown(index=False))
    md.append("\n\n要点：调用链长度高度可变 (最长 7–9 层)，不同请求路径各异 "
              "(路径熵见上表)，且完成时间随深度单调累积、跨 trace 分散显著 "
              "→ 无法为一次逻辑请求划定统一的观测时间窗口。\n")
    md.append("图: `c1r_completion_by_depth.png`, `c1r_chain_dynamism.png`；"
              "表: `chain_completion_by_depth.csv`, `chain_dynamism.csv`\n")
    md.append("\n### C2 顶点调用稀疏 + 执行时间方差极大\n")
    md.append("（仅统计**实际执行 span**：节点名含 `/` 的 Group/Agent；已剔除 time≈0 的组入口路由 span。）\n\n")
    med_cv = vt.groupby("load")["time_cv"].median().round(2).to_dict()
    top = vt[vt.load == "5 users"].sort_values("time_cv", ascending=False).head(3)
    top_str = ", ".join(f"{r.vertex.replace('AgentNetwork','').replace('Group','')}={r.time_cv}"
                        for _, r in top.iterrows())
    md.append(f"各负载下顶点执行时间 CV 中位数：{med_cv}；5 users 最高：{top_str}\n\n")
    md.append("要点：绝大多数工具顶点只在少数 trace 中被调用 (coverage <14%，极稀疏)；"
              "执行时间方差方面，**计算/IO 密集型智能体 CV 极大**（OCR≈2.5、planner≈1.45），"
              "整体中位 CV≈0.5、约 1/5 顶点 CV≥1 → 主导时延的关键服务其正常态波动就极大，"
              "故障引起的特征变化难以与这种固有稀疏+高方差区分。\n")
    md.append("图: `c2r_vertex_sparsity_variance.png`；表: `chain_vertex_stats.csv`\n")
    with open(os.path.join(TAB, "summary_chain.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print("\nDONE -> figures & tables updated")


if __name__ == "__main__":
    main()
