#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Joint analysis: real completion time (from traces / graph.json) vs the
service-level latency metrics (latency.csv, call.csv).

Findings this quantifies:
  * call.csv is byte-identical to latency.csv (same p50/p90/p99, only prefixed
    "unknown_") -> the "call" view adds no resolution over "latency".
  * latency.csv / call.csv are Prometheus-histogram-bucket QUANTIZED: each
    service's p50/p90/p99 collapses onto a handful of discrete bucket values.
  * The metric latency grossly MIS-estimates the real per-service completion
    time obtained from traces (magnitude + variance), and is far sparser.

=> Neither latency.csv nor call.csv can faithfully locate the anomaly time
   window (C1) or expose fault features (C2); the ground-truth signal lives in
   the trace completion time.

Outputs -> analysis/figures/joint_*.png , analysis/tables/joint_*.csv
"""
import os
import numpy as np
import pandas as pd
from common import (plt, FIG, TAB, DATASETS, mpath, load_traces,
                    trace_node_times, cv_pos as cv, save_fig)

# trace vertex group  ->  metric service column stem (agent-network-<svc>)
GROUP2SVC = {
    "AgentNetworkPlannerGroup": "planner",
    "WordGenerationAgentGroup": "word-gen",
    "PdfGenAgentGroup":         "pdf-gen",
    "PdfAgentGroup":            "pdf-parsing",
    "DirectionAgentGroup":      "direction",
    "OCRParserGroup":           "ocr",
    "CSVGeneratorAgentGroup":   "csv-gen",
    "ImageGenAgentGroup":       "image-gen",
    "ExcelGenGroup":            "excel-gen",
    "ExcelGroup":               "excel-parsing",   # Excel 读取/解析
    "WordAgentGroup":           "word-parsing",    # Word 读取/解析
}


def metric_latency(root, svc, quant="p99"):
    """Return metric latency series in SECONDS for a service (ms/1000)."""
    lat = pd.read_csv(mpath(root, "latency.csv"))
    col = f"agent-network-{svc}&{quant}"
    if col not in lat.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(lat[col], errors="coerce").dropna() / 1000.0


def build_table():
    rows = []
    for name, root in DATASETS.items():
        tt = trace_node_times(load_traces(root))
        for grp, svc in GROUP2SVC.items():
            real = np.array(tt.get(grp, []), float)
            m99 = metric_latency(root, svc, "p99")
            m50 = metric_latency(root, svc, "p50")
            if len(real) == 0 and len(m99) == 0:
                continue
            rows.append({
                "load": name, "service": svc, "trace_group": grp,
                "trace_invocations": len(real),
                "trace_median_s": round(float(np.median(real)), 2) if len(real) else np.nan,
                "trace_p99_s": round(float(np.percentile(real, 99)), 2) if len(real) else np.nan,
                "trace_cv": round(cv(real), 2),
                "metric_nonnull_windows": int(len(m99)),
                "metric_distinct_vals": int(m99.nunique()),
                "metric_p99_median_s": round(float(m99.median()), 2) if len(m99) else np.nan,
                "metric_p50_median_s": round(float(m50.median()), 2) if len(m50) else np.nan,
                "metric_cv": round(cv(m99.values), 2),
                "coverage_ratio": round(len(m99) / max(len(real), 1), 2) if len(real) else np.nan,
                "mag_ratio_metricP99_over_traceP99": (
                    round(float(m99.median()) / float(np.percentile(real, 99)), 1)
                    if len(m99) and len(real) and np.percentile(real, 99) > 0 else np.nan),
            })
    tab = pd.DataFrame(rows)
    tab.to_csv(os.path.join(TAB, "joint_latency_completion.csv"), index=False)
    return tab


def fig_real_vs_metric(tab):
    """5 users: per service, real trace time vs metric latency (log-y)."""
    sub = tab[tab.load == "5 users"].copy()
    sub = sub[sub.trace_invocations > 0].sort_values("trace_median_s")
    svcs = sub.service.tolist()
    x = np.arange(len(svcs))
    fig, ax = plt.subplots(figsize=(11, 5.2))
    ax.set_yscale("log")
    # real trace: median with p50..p99 whisker
    ax.vlines(x - 0.12, sub.trace_median_s, sub.trace_p99_s, color="#4C72B0", lw=6, alpha=.35)
    ax.plot(x - 0.12, sub.trace_median_s, "o", color="#4C72B0", label="Trace completion time, median (s)")
    ax.plot(x - 0.12, sub.trace_p99_s, "_", color="#2A4B7C", ms=12, label="Trace completion time, p99 (s)")
    # metric latency: p50 & p99 medians
    ax.plot(x + 0.12, sub.metric_p50_median_s, "s", color="#DD8452", label="Metric latency p50, median (s)")
    ax.plot(x + 0.12, sub.metric_p99_median_s, "D", color="#C44E52", label="Metric latency p99, median (s)")
    ax.set_xticks(x)
    ax.set_xticklabels(svcs, rotation=25, ha="right")
    ax.set_ylabel("Time (s, log scale)")
    ax.set_title("The histogram-bucketed latency metric grossly distorts the real completion time at a user load of 5.")
    ax.legend(fontsize=8, ncol=2)
    ax.grid(axis="y", ls=":", alpha=.5)
    fig.tight_layout()
    save_fig(fig, "joint_real_vs_metric_latency")
    plt.close(fig)


def fig_quant_coverage(tab):
    sub = tab[tab.load == "5 users"].copy()
    sub = sub[sub.trace_invocations > 0].sort_values("service")
    svcs = sub.service.tolist()
    y = np.arange(len(svcs))
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    # left: quantization — distinct metric values vs distinct-ish real (invocations)
    axes[0].barh(y - 0.2, sub.metric_distinct_vals, 0.4, color="#C44E52", label="Distinct metric latency values")
    axes[0].barh(y + 0.2, sub.trace_invocations, 0.4, color="#4C72B0", label="Trace invocations")
    axes[0].set_yticks(y); axes[0].set_yticklabels(svcs, fontsize=8)
    axes[0].set_xscale("log")
    axes[0].set_xlabel("Count (log scale)")
    axes[0].set_title("The latency metric collapses onto only a few discrete bucket values per service.")
    axes[0].legend(fontsize=8); axes[0].grid(axis="x", ls=":", alpha=.5)
    # right: coverage ratio
    axes[1].barh(y, sub.coverage_ratio, color="#55A868", alpha=.85)
    axes[1].axvline(1.0, ls="--", color="grey")
    axes[1].set_yticks(y); axes[1].set_yticklabels(svcs, fontsize=8)
    axes[1].set_xlabel("Metric non-null windows / trace invocations")
    axes[1].set_title("The number of metric windows is not proportional to the real invocation count.")
    axes[1].grid(axis="x", ls=":", alpha=.5)
    fig.tight_layout()
    save_fig(fig, "joint_quantization_coverage")
    plt.close(fig)


def main():
    tab = build_table()
    fig_real_vs_metric(tab)
    fig_quant_coverage(tab)
    print("=== joint table (5 users) ===")
    show = tab[tab.load == "5 users"][
        ["service", "trace_invocations", "trace_median_s", "trace_p99_s", "trace_cv",
         "metric_nonnull_windows", "metric_distinct_vals", "metric_p99_median_s",
         "metric_cv", "coverage_ratio", "mag_ratio_metricP99_over_traceP99"]]
    print(show.to_string(index=False))
    print("\nDONE -> joint figures & table")


if __name__ == "__main__":
    main()
