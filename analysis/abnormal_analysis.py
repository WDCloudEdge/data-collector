#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Add labelled 3-user failure experiments to the analysis under normal workload.

Run: .venv/bin/python analysis/abnormal_analysis.py

The comparison figure reuses motivation_analysis.variance for the normal panel.
Metric lag is descriptive, not a causal propagation estimate or a detector test.
Missing success-rate metrics stay missing; pre-injection drops are retained.
"""
import os
import re
from functools import lru_cache

import numpy as np
import pandas as pd
from common import plt, ROOT, FIG, TAB, DATASETS, mpath, save_fig
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from motivation_analysis import variance

ABN_ROOT = os.path.join(ROOT, "data", "thingo", "abnormal")
SVC = "agent-network-pdf-parsing"
# Each load has its own directory. The first `startup_trim_s` seconds of every load-5
# run are dropped because the program has just started (cold-start artifact).
LOADS = {"3 users": ("load-3", 0), "5 users": ("load-5", 60)}


def label_path(load):
    return os.path.join(ABN_ROOT, LOADS[load][0], SVC + "_label.txt")


def metrics_dir(load, fault_type):
    return os.path.join(ABN_ROOT, LOADS[load][0], f"{SVC}_{fault_type}_1", "agent-network", "metrics")


ABN_LABEL = label_path("3 users")  # overlap figure and main baseline stay 3-user
FAULT_ORDER = ["cpu_load", "mem_load", "net_latency", "pod_failure", "pod_kill"]
FAULT_LABEL = dict(zip(FAULT_ORDER, ["CPU", "MEM", "NET", "PodF", "PodK"]))
FAULT_NAMES = dict(zip(FAULT_ORDER, ["CPU stress", "Memory stress", "Network latency", "Pod failure", "Pod kill"]))
FAULT_COLOR = dict(zip(FAULT_ORDER, ["#C44E52", "#DD8452", "#8172B3", "#937860", "#DA8BC3"]))
KINDS = ["CPU (mC)", "Memory (MB)", "Latency p99 (ms)", "Success rate"]
CHAIN_SERVICES = {"agent-network-planner": ("Planner (entry)", "#4C72B0", "o"),
                  "agent-network-summarizer": ("Summarizer (downstream)", "#55A868", "^")}
SR_THRESHOLD = 0.999
# Dark = 3 users, light = 5 users; blue = planner, green = summarizer.
SHADE = {("agent-network-planner", "3 users"): "#20456F",
         ("agent-network-planner", "5 users"): "#9DBEE0",
         ("agent-network-summarizer", "3 users"): "#2F6B3C",
         ("agent-network-summarizer", "5 users"): "#9BD0A5"}


def to_epoch(series):
    return pd.to_datetime(series, utc=True).astype("int64") // 10**9


def parse_label(path):
    out, cur = {}, None
    with open(path, encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if line.startswith("===") and line.endswith("==="):
                cur = line.strip("= ")
                out[cur] = {"name": cur}
            elif cur and ":" in line:
                k, _, v = line.partition(":")
                k, v = k.strip(), v.strip()
                out[cur][k] = v
                match = re.search(r"\((\d+)\)", v)
                if match:
                    out[cur][k + "_ts"] = int(match.group(1))
    return {meta["fault_type"]: meta for meta in out.values()}


def abn_metrics_dir(fault_type):
    return metrics_dir("3 users", fault_type)  # 3-user path kept for the overlap figure


@lru_cache(maxsize=None)
def read_metric(path):
    df = pd.read_csv(path)
    df["t"] = to_epoch(df["timestamp"])
    return df.set_index("t").drop(columns="timestamp").apply(pd.to_numeric, errors="coerce").sort_index()


def resource_series(inst, suffix, svc=SVC):
    # Sum both the terminated pod and its replacement at each timestamp.
    cols = [c for c in inst if c.startswith(svc + "-") and c.endswith(suffix)]
    if not cols:
        raise ValueError(f"No {svc} resource columns ending in {suffix}")
    return inst[cols].sum(axis=1, min_count=1)


def service_metrics(directory, svc=SVC):
    inst = read_metric(os.path.join(directory, "instance.csv"))
    lat = read_metric(os.path.join(directory, "latency.csv"))
    sr = read_metric(os.path.join(directory, "success_rate.csv"))
    return {KINDS[0]: resource_series(inst, "_cpu", svc),
            KINDS[1]: resource_series(inst, "_memory", svc),
            KINDS[2]: lat.get(svc + "&p99", pd.Series(np.nan, index=lat.index)),
            KINDS[3]: sr.get(svc, pd.Series(np.nan, index=sr.index))}


def normal_baseline(loads=("3 users",)):
    values = {kind: [] for kind in KINDS}
    for load in loads:
        directory = os.path.dirname(mpath(DATASETS[load], "instance.csv"))
        for kind, series in service_metrics(directory).items():
            values[kind].append(series.dropna().to_numpy())
    return {kind: np.concatenate(arrays) for kind, arrays in values.items()}


def in_fault_values(fault_type, meta):
    fs, fe = meta["fault_start_ts"], meta["fault_end_ts"]
    return {kind: s[(s.index >= fs) & (s.index < fe)].dropna().to_numpy()
            for kind, s in service_metrics(abn_metrics_dir(fault_type)).items()}


def in_fault_values_for(load, fault_type, meta):
    fs, fe = meta["fault_start_ts"], meta["fault_end_ts"]
    return {kind: s[(s.index >= fs) & (s.index < fe)].dropna().to_numpy()
            for kind, s in service_metrics(metrics_dir(load, fault_type)).items()}


def prefault_values_for(load, fault_type, meta):
    """Same-run normal-workload baseline: [load_start, fault_start) — load on, no fault."""
    ls, fs, ws = meta["load_start_ts"], meta["fault_start_ts"], meta["window_start_ts"]
    start = max(ls, ws + LOADS[load][1])
    return {kind: s[(s.index >= start) & (s.index < fs)].dropna().to_numpy()
            for kind, s in service_metrics(metrics_dir(load, fault_type)).items()}


# Faulted-service conditions shown in the overlap figure. pdf-parsing keeps both loads
# (the original evidence); planner and summarizer add the newly collected 5-user runs.
# (service, load, label, color)
CONDITIONS = [("pdf-parsing", "3 users", "pdf-parsing 3u", "#9CCF9A"),
              ("pdf-parsing", "5 users", "pdf-parsing 5u", "#55A868"),
              ("planner", "5 users", "planner 5u", "#4C72B0"),
              ("summarizer", "5 users", "summarizer 5u", "#8172B3")]


def own_baseline_overlap():
    """Each failure's in-fault metrics vs its own pre-failure workload (same run).

    Iterates the faulted-service conditions in CONDITIONS so the newly collected
    planner/summarizer failures are added alongside the original pdf-parsing runs.
    """
    rows = []
    for service, load, _, _ in CONDITIONS:
        svc_full = f"agent-network-{service}"
        labels = parse_label(os.path.join(ABN_ROOT, LOADS[load][0], svc_full + "_label.txt"))
        trim = LOADS[load][1]
        for ft in FAULT_ORDER:
            meta = labels[ft]
            ls, fs, fe, ws = (meta["load_start_ts"], meta["fault_start_ts"],
                              meta["fault_end_ts"], meta["window_start_ts"])
            mdir = os.path.join(ABN_ROOT, LOADS[load][0], f"{svc_full}_{ft}_1", "agent-network", "metrics")
            sm = service_metrics(mdir, svc_full)
            start = max(ls, ws + trim)
            base = {k: s[(s.index >= start) & (s.index < fs)].dropna().to_numpy() for k, s in sm.items()}
            inf = {k: s[(s.index >= fs) & (s.index < fe)].dropna().to_numpy() for k, s in sm.items()}
            for kind in KINDS:
                nb, fv = base[kind], inf[kind]
                lo, hi = np.percentile(nb, [10, 90]) if len(nb) else (np.nan, np.nan)
                rows.append({"service": service, "load": load, "condition": f"{service} {load[0]}u",
                             "fault": ft, "metric": kind, "prefault_n": len(nb), "infault_n": len(fv),
                             "prefault_mean": np.mean(nb) if len(nb) else np.nan,
                             "prefault_cv": cv(nb), "prefault_p10": lo, "prefault_p90": hi,
                             "infault_mean": np.mean(fv) if len(fv) else np.nan, "infault_cv": cv(fv),
                             "infault_within_prefault_p10_p90_pct":
                                 100 * np.mean((fv >= lo) & (fv <= hi)) if len(fv) and len(nb) else np.nan})
    return pd.DataFrame(rows)


def cv(values):
    return float(np.std(values) / abs(np.mean(values))) if len(values) > 1 and np.mean(values) != 0 else np.nan


def overlap_table(labels, baseline, baseline_name):
    rows = []
    for ft in FAULT_ORDER:
        values = in_fault_values(ft, labels[ft])
        series = service_metrics(abn_metrics_dir(ft))
        fs, fe = labels[ft]["fault_start_ts"], labels[ft]["fault_end_ts"]
        for kind in KINDS:
            nb, fv = baseline[kind], values[kind]
            lo, hi = np.percentile(nb, [10, 90]) if len(nb) else (np.nan, np.nan)
            std = np.std(nb) if len(nb) else np.nan
            delta = abs(np.mean(fv) - np.mean(nb)) if len(fv) and len(nb) else np.nan
            # A standardized mean difference is NOT a distribution overlap score.
            shift = delta / std if std > 1e-9 else (0.0 if delta == 0 else np.nan)
            total = int(((series[kind].index >= fs) & (series[kind].index < fe)).sum())
            rows.append({"baseline": baseline_name, "metric": kind, "fault": ft,
                         "normal_n": len(nb), "infault_n": len(fv), "infault_total_samples": total,
                         "normal_mean": np.mean(nb) if len(nb) else np.nan,
                         "normal_std": std, "normal_cv": cv(nb), "normal_p10": lo, "normal_p90": hi,
                         "infault_mean": np.mean(fv) if len(fv) else np.nan,
                         "infault_cv": cv(fv), "standardized_mean_shift": shift,
                         "infault_within_normal_p10_p90_pct":
                             100 * np.mean((fv >= lo) & (fv <= hi)) if len(fv) else np.nan})
    return pd.DataFrame(rows)


def fig_overlap(labels):
    tab = own_baseline_overlap()  # each failure vs its own pre-failure workload (3 and 5 users)
    tab.to_csv(os.path.join(TAB, "c2c_abn_vs_normal_overlap.csv"), index=False, float_format="%.6f")
    # secondary: cross-run sensitivity against the separately collected normal runs
    sensitivity = pd.concat([overlap_table(labels, normal_baseline((load,)), load) for load in DATASETS] +
                            [overlap_table(labels, normal_baseline(tuple(DATASETS)), "pooled 1/3/5 users")])
    sensitivity.to_csv(os.path.join(TAB, "c2c_abn_baseline_sensitivity.csv"), index=False, float_format="%.6f")

    cpu = tab[tab.metric == KINDS[0]]
    y = np.arange(len(FAULT_ORDER))[::-1]
    offsets = [.28, .10, -.10, -.28]  # one lane per faulted-service condition

    with plt.rc_context({"font.size": 8, "axes.titlesize": 9, "axes.labelsize": 8,
                         "xtick.labelsize": 8, "ytick.labelsize": 8}):
        fig = plt.figure(figsize=(7.6, 5.2))
        grid = fig.add_gridspec(2, 2, height_ratios=[1, 1.18], hspace=.73, wspace=.26)
        top = fig.add_subplot(grid[0, :])
        variance(ax=top)  # Preserve the original analysis under normal workload.
        top.set_title("")
        top.set_title("(a) Variability of metrics under normal workload", loc="left")
        top.set_ylabel("Coefficient of variation")
        top.legend(handles=[Patch(facecolor=c, alpha=.7) for c in ["#4C72B0", "#55A868", "#DD8452"]],
                   labels=list(DATASETS), ncol=3, fontsize=7, loc="upper right", frameon=False)
        top.set_xticks(np.arange(5) * 4 + 1)
        top.set_xticklabels(["CPU", "Memory", "Net recv", "Latency p99", "Latency p50"])

        left = fig.add_subplot(grid[1, 0])
        right = fig.add_subplot(grid[1, 1], sharey=left)
        # (b) in-failure CPU CV (bars) vs each run's own pre-failure CV (ticks);
        # (c) % of in-failure CPU within the same run's pre-failure p10-p90.
        for (service, load, clabel, color), off in zip(CONDITIONS, offsets):
            sub = cpu[(cpu.service == service) & (cpu.load == load)].set_index("fault").loc[FAULT_ORDER]
            yy = y + off
            left.barh(yy, sub.infault_cv, height=.16, color=color, alpha=.9, zorder=3, label=clabel)
            left.scatter(sub.prefault_cv, yy, marker="|", s=60, color="#222222", lw=1.1, zorder=4)
            right.barh(yy, sub.infault_within_prefault_p10_p90_pct, height=.16, color=color, alpha=.9, zorder=3)
            for row_y, value in zip(yy, sub.infault_within_prefault_p10_p90_pct):
                right.text(value+1.8, row_y, f"{value:.0f}", ha="left", va="center", fontsize=5.6)
        left.set_xlim(0, 6.6)
        left.set_xticks([0, 2, 4, 6])
        left.set_yticks(y)
        left.set_yticklabels([FAULT_NAMES[ft] for ft in FAULT_ORDER])
        left.set_title("(b) CPU CV: failure (bar) vs pre-failure (│)", loc="left", pad=30)
        left.set_xlabel("Coefficient of variation")
        right.set_xlim(0, 112)
        right.set_xticks([0, 25, 50, 75, 100])
        right.tick_params(axis="y", left=False, labelleft=False)
        right.set_xlabel("CPU within pre-failure p10-p90 (%)")
        right.set_title("(c) CPU overlap with pre-failure", loc="left", pad=30)
        for ax in (left, right):
            ax.set_ylim(-.65, 5.0)
            ax.grid(axis="x", ls=":", alpha=.4, zorder=0)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
        fig.legend(handles=[Patch(facecolor=c, alpha=.9, label=l) for _, _, l, c in CONDITIONS],
                   loc="center", ncol=4, fontsize=6.8, frameon=False, bbox_to_anchor=(.56, .565))
        fig.text(.56, .028,
                 "Faulted service per row group; same-run baseline (~24 pre-failure, ~36 in-failure values each).",
                 ha="center", fontsize=6.6)
        fig.subplots_adjust(left=.165, right=.985, top=.94, bottom=.13)
        save_fig(fig, "c2c_abn_vs_normal_overlap")
        plt.close(fig)
    return tab


def table_downstream_lag(labels):
    rows, service_rows = [], []
    for ft in FAULT_ORDER:
        fs, fe = labels[ft]["fault_start_ts"], labels[ft]["fault_end_ts"]
        sr = read_metric(os.path.join(abn_metrics_dir(ft), "success_rate.csv"))
        own = sr[SVC]
        in_fault = own[(own.index >= fs) & (own.index < fe)]
        first_any, first_post, drop_services = [], [], []
        for service in sr:
            if not service.startswith("agent-network-") or service == SVC:
                continue
            bad = sr[service][sr[service] < SR_THRESHOLD]
            post = bad[bad.index >= fs]
            if len(bad):
                first_any.append(int(bad.index[0]))
                drop_services.append(service.removeprefix("agent-network-"))
            if len(post):
                first_post.append(int(post.index[0]))
            service_rows.append({"fault": ft, "service": service,
                                 "first_drop_from_fault_start_s": int(bad.index[0] - fs) if len(bad) else np.nan,
                                 "first_post_start_drop_from_fault_start_s": int(post.index[0] - fs) if len(post) else np.nan,
                                 "preexisting_drop": bool(len(bad) and bad.index[0] < fs),
                                 "success_min": sr[service].min()})
        any_t = min(first_any) if first_any else None
        post_t = min(first_post) if first_post else None
        rows.append({"fault": ft, "fault_duration_s": fe - fs,
                     "faulted_svc_success_min": own.min(), "faulted_svc_valid_samples": own.count(),
                     "faulted_svc_infault_valid_samples": in_fault.count(),
                     "faulted_svc_infault_total_samples": len(in_fault),
                     "downstream_drops": ",".join(drop_services),
                     "first_drop_from_fault_start_s": any_t - fs if any_t is not None else np.nan,
                     "first_drop_lag_after_fault_end_s": any_t - fe if any_t is not None else np.nan,
                     "first_post_start_drop_from_fault_start_s": post_t - fs if post_t is not None else np.nan,
                     "preexisting_drop": bool(any_t is not None and any_t < fs),
                     "clean_post_fault_onset": bool(any_t is not None and any_t > fe)})
    tab, detail = pd.DataFrame(rows), pd.DataFrame(service_rows)
    tab.to_csv(os.path.join(TAB, "c1_abn_downstream_lag.csv"), index=False)
    detail.to_csv(os.path.join(TAB, "c1_abn_service_drop_times.csv"), index=False)
    return tab, detail


def chain_drop_timing(load):
    """First success-rate reduction at planner/summarizer per fault, for one load.

    Times are given relative to failure onset and to the end of injection. The first
    `startup_trim_s` seconds of the run are dropped before scanning (load-5 cold start).
    """
    labels = parse_label(label_path(load))
    trim = LOADS[load][1]
    rows = []
    for ft in FAULT_ORDER:
        meta = labels[ft]
        ws, fs, fe = meta["window_start_ts"], meta["fault_start_ts"], meta["fault_end_ts"]
        sr = read_metric(os.path.join(metrics_dir(load, ft), "success_rate.csv"))
        if trim:
            sr = sr[sr.index >= ws + trim]
        for svc in CHAIN_SERVICES:
            first = None
            if svc in sr:
                bad = sr[svc][sr[svc] < SR_THRESHOLD]
                if len(bad):
                    first = int(bad.index[0])
            rows.append({"load": load, "fault": ft, "service": svc.removeprefix("agent-network-"),
                         "svc_key": svc,
                         "rel_onset": (first - fs) if first is not None else np.nan,
                         "rel_end": (first - fe) if first is not None else np.nan,
                         "preexisting": bool(first is not None and first < fs)})
    return pd.DataFrame(rows)


def fig_window_timeline(labels, lagtab, detail, fault_type="cpu_load"):
    # Panels (a)-(b): load-5 CPU stress — the faulted service keeps success-rate metrics
    # through the fault (no gap), so all three chain lines can be drawn end to end.
    LOAD_A = "5 users"
    meta = parse_label(label_path(LOAD_A))[fault_type]
    ws, ls, fs, fe = (meta["window_start_ts"], meta["load_start_ts"],
                      meta["fault_start_ts"], meta["fault_end_ts"])
    trim = LOADS[LOAD_A][1]
    mdir = metrics_dir(LOAD_A, fault_type)
    cpu = service_metrics(mdir)[KINDS[0]]
    cpu = cpu[cpu.index >= ws + trim]
    sr = read_metric(os.path.join(mdir, "success_rate.csv"))
    sr = sr[sr.index >= ws + trim]
    pre = cpu[(cpu.index >= ls) & (cpu.index < fs)].dropna()          # pre-failure workload band
    lo, hi = np.percentile(pre, [10, 90])
    drop = np.nan                                                     # first downstream drop after injection
    for svc in CHAIN_SERVICES:
        if svc in sr:
            bad = sr[svc][(sr[svc] < SR_THRESHOLD) & (sr[svc].index > fe)]
            if len(bad):
                drop = np.nanmin([drop, int(bad.index[0]) - fs])
    with plt.rc_context({"font.size": 8, "axes.titlesize": 9, "axes.labelsize": 8,
                         "xtick.labelsize": 8, "ytick.labelsize": 8}):
        fig, axes = plt.subplots(3, 1, figsize=(7.2, 5.6), sharex=True,
                                 gridspec_kw={"height_ratios": [1, 1, 1.35], "hspace": .36})
        for ax in axes:
            ax.axvspan(0, fe-fs, color="#C44E52", alpha=.12, zorder=0)
            ax.grid(axis="x", ls=":", alpha=.3)
        a, b, c = axes
        a.plot(cpu.index-fs, cpu, color="#C44E52", lw=1.5)
        a.axhspan(lo, hi, color="#999999", alpha=.18, zorder=0)
        a.set_title("(a) CPU metrics at pdf-parsing during CPU stress (5 users)", loc="left")
        a.set_ylabel("CPU (mC)")
        a.set_ylim(0, 740)
        a.text(90, 690, "Failure injection", ha="center", color="#A13A3E", fontsize=8)
        if not np.isnan(drop):
            a.annotate(f"{int(drop-(fe-fs))} s after failure injection ends", xy=(fe-fs, 625), xytext=(drop, 625),
                       arrowprops={"arrowstyle": "<->", "color": "#555555"}, ha="left", va="center", fontsize=8)
        a.text(600, hi+55, "Pre-fault workload\np10-p90", color="#666666", fontsize=7)
        b.plot(sr.index-fs, sr[SVC], color="black", marker=".", ms=2, lw=1.4, label="pdf-parsing (with failure)")
        for svc, (label, color, marker) in CHAIN_SERVICES.items():
            if svc in sr:
                b.plot(sr.index-fs, sr[svc], color=color, ls="--", lw=1.2, label=label)
        b.set_ylim(-.03, 1.06)
        b.set_yticks([0, .5, 1.0])
        b.set_ylabel("Success rate")
        b.set_title("(b) Success-rate metrics during CPU stress (5 users)", loc="left")
        b.legend(loc="lower right", fontsize=7, frameon=False)
        for ax in (a, b):
            if not np.isnan(drop):
                ax.axvline(drop, color="#555555", ls=":", lw=1.2)
        c.set_title("(c) First success-rate reduction relative to failure injection (3 and 5 users)", loc="left")
        timing = pd.concat([chain_drop_timing(load) for load in LOADS], ignore_index=True)
        timing.to_csv(os.path.join(TAB, "c1_abn_chain_drop_timing.csv"), index=False)
        yoff = {"agent-network-planner": .16, "agent-network-summarizer": -.16}
        for y, ft in enumerate(FAULT_ORDER[::-1]):
            for load in LOADS:
                for svc, (_, _, marker) in CHAIN_SERVICES.items():
                    r = timing[(timing.load == load) & (timing.fault == ft) & (timing.svc_key == svc)]
                    if not len(r) or pd.isna(r.rel_onset.iloc[0]):
                        continue
                    color = SHADE[(svc, load)]
                    c.scatter(r.rel_onset.iloc[0], y + yoff[svc], marker=marker, s=34, lw=1.1, zorder=4,
                              edgecolor=color, facecolor="white" if bool(r.preexisting.iloc[0]) else color)
            for load, dy in (("3 users", .30), ("5 users", -.30)):
                sub = timing[(timing.load == load) & (timing.fault == ft)]
                if sub.rel_end.notna().any():
                    e = int(sub.rel_end.min())
                    c.text(930, y + dy, (f"+{e}" if e >= 0 else f"{e}") + " s", ha="right", va="center",
                           fontsize=6.6, color=SHADE[("agent-network-planner", load)])
        c.axvline(0, color="#C44E52", lw=.8)
        c.axvline(181, color="#C44E52", lw=.8, ls=":")
        c.set_yticks(range(5))
        c.set_yticklabels([FAULT_NAMES[ft] for ft in FAULT_ORDER[::-1]])
        c.set_ylim(-.6, 4.7)
        c.set_xlim(-330, 960)
        c.set_xticks([-300, -150, 0, 181, 400, 600])
        c.set_xlabel("Time relative to failure onset (s)  ·  right labels = first drop vs injection end")
        fig.legend(handles=[Patch(facecolor="#C44E52", alpha=.15, label="Failure injection (~181 s)"),
                            Line2D([], [], marker="o", color="#20456F", ls="", label="Planner"),
                            Line2D([], [], marker="^", color="#2F6B3C", ls="", label="Summarizer"),
                            Line2D([], [], marker="s", color="#20456F", ls="", label="3 users (dark)"),
                            Line2D([], [], marker="s", color="#9DBEE0", ls="", label="5 users (light)"),
                            Line2D([], [], marker="o", markerfacecolor="white", color="#777777", ls="", label="Drop precedes onset")],
                   loc="lower center", ncol=6, frameon=False, fontsize=6.6, bbox_to_anchor=(.54, .002))
        fig.subplots_adjust(left=.15, right=.985, bottom=.13, top=.95)
        save_fig(fig, "c1_abn_window_timeline")
        plt.close(fig)


def write_summary(overlap, lagtab):
    """Keep the paper's numerical evidence and its interpretation together."""
    distribution = overlap[["service", "load", "metric", "fault", "prefault_n", "infault_n", "prefault_cv",
                            "infault_cv", "infault_within_prefault_p10_p90_pct"]].round(3)
    timing = lagtab[["fault", "faulted_svc_infault_valid_samples", "faulted_svc_infault_total_samples",
                     "first_drop_from_fault_start_s", "first_drop_lag_after_fault_end_s",
                     "first_post_start_drop_from_fault_start_s", "preexisting_drop"]]
    text = """# 异常实验补充：动机 2 的图表与证据

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

""" + distribution.to_markdown(index=False) + """

## 信号位置与时间错位

所有实验中故障服务的有效成功率观测均为 1.0，但 CPU / 内存压力期间是 0/37 个有效值；不能据此声称全程成功。其余三类故障区间分别为 32/37、18/37、17/37 个有效值。

CPU 压力注入期峰值 568 mC；planner 和 summarizer 的首次下降在故障开始后 400 秒，即结束后 219 秒，届时资源峰值已经过去。CPU、内存压力、pod failure、pod kill 的首次链路服务下降分别晚于故障结束 219、429、254、194 秒。planner 是入口，summarizer 是下游出口，不能把二者都称为下游服务。

网络延迟实验在注入前 75 秒已有下降；注入后首次下降在 +120 秒，仍在故障期内。保留这组观测，但不将它计入“故障结束后才首次下降”的四组。该例支持异常起点归属不明确，不能用于证明干净的故障后传播时延。

""" + timing.to_markdown(index=False) + """

## 论文衔接与证据边界

动机 2 保留正常数据的 metric lag、链路完成时间与动态路径论证，再接“部分故障指标被正常波动覆盖，局部起点难定”，然后接“链路其他服务的症状延迟或提前存在”，最终落到异常窗口需要结合请求执行时序。`motivation.tex` 保留原有数值与主体论证，统一正文、图注和图内的专业词汇为 metric lag、failure、normal workload、anomaly time windows 和 metrics。

- `c2c_abn_baseline_sensitivity.csv` 另列 1、3、5 用户和合并基线。1/5 用户的 PDF-parsing CPU 波动更小，且无该服务 p99 观测；跨负载混合会改变标准化偏移。因此正文限定为同负载对照，不沿用旧 4.5--5.2 sigma，也不声称基线无关。
- 正常与异常采集时长、任务组合不完全相同，五类故障各仅一次；图表为描述性证据，不给出分类准确率或显著性检验。
- `MetricCollector.py` 中 CPU 和成功率采用 1 分钟 rate 窗口，采样步长 5 秒。标签中停止负载与故障结束重合。不能仅据这些聚合曲线，把所有时差归因于故障传播或断言资源峰值仅持续于注入区间。
- 没有运行一个具体检测器；“从首次症状开始的窗口会错过故障”是由观测时间位置推出的结果。没有证明任意长度回溯窗口都会失败。
- 旧 `tables/c2_abn_vs_normal.csv` 是历史口径，不用于本次正文。当前定量依据以 `c2c_abn_vs_normal_overlap.csv`、`c2c_abn_baseline_sensitivity.csv`、`c1_abn_downstream_lag.csv` 和 `c1_abn_service_drop_times.csv` 为准。`downstream_drops` 是沿用的 CSV 列名，其中包含入口 planner，解读为其他链路服务。

## 复现

在仓库根目录运行 `.venv/bin/python analysis/abnormal_analysis.py`，生成两张 PDF / PNG、四张 CSV 和本报告。`analysis/run_all.py` 已将异常分析作为最后一步。正常图的独立生成逻辑保持不变。
"""
    with open(os.path.join(TAB, "summary_abnormal.md"), "w", encoding="utf-8") as stream:
        stream.write(text)


def main():
    labels = parse_label(ABN_LABEL)
    if set(labels) != set(FAULT_ORDER) or any(int(m["load_users"]) != 3 for m in labels.values()):
        raise ValueError("Expected exactly the five labelled 3-user failure experiments")
    overlap = fig_overlap(labels)
    lagtab, detail = table_downstream_lag(labels)
    fig_window_timeline(labels, lagtab, detail)
    write_summary(overlap, lagtab)
    print(overlap[overlap.metric == KINDS[0]][["service", "load", "fault", "prefault_cv", "infault_cv",
          "infault_within_prefault_p10_p90_pct"]].round(2).to_string(index=False))
    print(lagtab.to_string(index=False))
    print("Figures:", FIG, "\nTables:", TAB)


if __name__ == "__main__":
    main()
