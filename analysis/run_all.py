#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the full motivation analysis pipeline in order.

    .venv/bin/python analysis/run_all.py

Produces every figure (analysis/figures/) and table (analysis/tables/).
See analysis/分析汇总.md for the consolidated narrative.
"""
import motivation_analysis
import chain_analysis
import joint_analysis

if __name__ == "__main__":
    print("=" * 60, "\n[1/5] motivation_analysis (指标稀疏/方差 + 整体/角色链路滞后)")
    motivation_analysis.main()
    print("=" * 60, "\n[2/5] chain_analysis (真实调用链: 完成时间/动态性/顶点稀疏方差)")
    chain_analysis.main()
    print("=" * 60, "\n[3/5] joint_analysis (完成时间 vs latency.csv/call.csv)")
    joint_analysis.main()
    print("=" * 60, "\n[4/5] multi_replica_recheck (新1-user多副本: 负滞后复核 + 全结论复跑)")
    import multi_replica_recheck
    multi_replica_recheck.main()
    # print("=" * 60, "\n[5/5] abnormal_analysis (正常/异常对照 + 异常窗口错位)")
    # import abnormal_analysis
    # abnormal_analysis.main()
    print("=" * 60, "\nALL DONE -> analysis/figures, analysis/tables")
