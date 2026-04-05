# data-collector
统一从Prometheus，jaeger收集数据并整理的工具

## OpenClaw 采集入口

新增入口：`Main-Openclaw.py`，用于抓取：

- `ollama-proxy` 原始 jsonl 日志

并导出原始 JSON。

### 运行示例（仅需监测时间）

```bash
python Main-Openclaw.py 180
```

### 输出目录

默认输出到：`./data/openclaw/`

输出文件：

- `raw/openclaw_ollama_raw.json` 或 `raw/openclaw_dashscope_proxy.json` — 代理原始数据（records + invalid_lines + raw_lines）
- `tidy/openclaw_ollama_tidy.json` 或 `tidy/openclaw_dashscope_tidy.json` — 精简后的请求/响应级别数据（按秒时间、token、message 结构）

其余参数已内置默认值：

- 方式1(ollama) proxy: `openclaw/ollama-proxy`，日志路径：`/var/log/ollama-proxy/openclaw_proxy_raw.jsonl`
- 方式2(apikey) proxy 默认: `openclaw/dashscope-proxy`，日志路径：`/var/log/dashscope-proxy/openclaw_proxy_raw.jsonl`
- 可通过环境变量覆盖：
  - `OPENCLAW_OLLAMA_PROXY_DEPLOYMENT` / `OPENCLAW_OLLAMA_PROXY_LOG_PATH`
  - `OPENCLAW_APIKEY_PROXY_DEPLOYMENT` / `OPENCLAW_APIKEY_PROXY_LOG_PATH`
