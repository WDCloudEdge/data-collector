# data-collector
统一从Prometheus，jaeger收集数据并整理的工具

## OpenClaw OTEL采集入口

新增入口：`Main-Openclaw.py`，用于同时抓取：

- `otel-collector` 日志（OTEL）
- `ollama-proxy` 原始 jsonl 日志

并导出原始 JSON。

### 运行示例（仅需监测时间）

```bash
python Main-Openclaw.py 180
```

### 输出目录

默认输出到：`./data/openclaw/`

输出文件：

- `raw/openclaw_otel_raw.json` — 完整原始遥测数据（含解析后的 spans / metrics / logs 及原始日志行）
- `raw/openclaw_ollama_raw.json` — `ollama-proxy` 原始数据（records + invalid_lines + raw_lines）
- `result/openclaw_trace_groups.json` — 融合后的按 `trace_id` 分组结果（含 spans 列表与 attributes）
- `result/openclaw_trace_groups_flat.csv` — 融合结果的平铺表，便于快速筛选/统计

其余参数已内置默认值：

- otel-collector: `openclaw/otel-collector-verify`
- ollama-proxy: `openclaw/ollama-proxy`
- proxy 日志路径: `/var/log/ollama-proxy/openclaw_proxy_raw.jsonl`
