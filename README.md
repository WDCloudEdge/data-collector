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
- `tidy/openclaw_otel_tidy.json` — 精简后的 span 级别数据（按秒时间、token、duration 等）
- `tidy/openclaw_ollama_tidy.json` — 精简后的请求/响应级别数据（按秒时间、token、message 结构）
- `result/result.json` — 按规则串联后的 webchat span 与对应 ollama record 链路结果

其余参数已内置默认值：

- otel-collector: `openclaw/otel-collector-verify`
- ollama-proxy: `openclaw/ollama-proxy`
- proxy 日志路径: `/var/log/ollama-proxy/openclaw_proxy_raw.jsonl`
