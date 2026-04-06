# -*- coding: utf-8 -*-
"""
OpenClaw 代理日志采集 + 按 Jaeger span 对齐，将 LLM 字段写入 trace 的 pkl。

逻辑与 Main-Openclaw.py 一致（通过动态加载复用 OllamaProxyRawCollector / build_openclaw_ollama_tidy），
不修改 Main-Openclaw.py 本体。
"""
import importlib.util
import json
import os
import pickle
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

# 与 handler.TraceIstio 写入 pkl 的 timestamp 一致：startTime + clock_skew_adjust(span)
from handler.TraceIstio import clock_skew_adjust


def _load_openclaw_script():
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "Main-Openclaw.py")
    if not os.path.isfile(path):
        raise FileNotFoundError("Main-Openclaw.py not found beside openclaw_trace_enrich.py: %s" % path)
    spec = importlib.util.spec_from_file_location("openclaw_main_script", path)
    mod = importlib.util.module_from_spec(spec)
    # 避免被当作脚本顶层执行
    mod.__name__ = "openclaw_main_script"
    spec.loader.exec_module(mod)
    return mod


_oc_mod = None


def _oc():
    global _oc_mod
    if _oc_mod is None:
        _oc_mod = _load_openclaw_script()
    return _oc_mod


def jaeger_api_root(jaeger_url: str) -> str:
    """从 Config.jaeger_url 得到 http://host:port"""
    u = (jaeger_url or "").split("?")[0].rstrip("/")
    if u.endswith("/api/traces"):
        return u[: -len("/api/traces")]
    if u.endswith("/traces"):
        return u[: -len("/traces")]
    return u


def _record_in_time_window(record: Dict[str, Any], start_unix: int, end_unix: int) -> bool:
    oc = _oc()
    dt = oc._parse_iso_datetime(record.get("timestamp"))
    if dt is None:
        return False
    ts = int(dt.timestamp())
    return start_unix <= ts <= end_unix


def _kubectl_cat_log(
    namespace: str, deployment: str, container: str, log_path: str
) -> str:
    cmd = [
        "kubectl",
        "-n",
        namespace,
        "exec",
        "deploy/%s" % deployment,
    ]
    if container and container.strip():
        cmd.extend(["-c", container.strip()])
    cmd.extend(["--", "sh", "-c", "if [ -f '%s' ]; then cat '%s'; fi" % (log_path, log_path)])
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if proc.returncode != 0:
        sys.stderr.write(
            "[openclaw] kubectl exec failed for deploy/%s: %s\n" % (deployment, proc.stderr.strip())
        )
        return ""
    return proc.stdout or ""


def _write_filtered_raw_and_tidy(
    oc,
    records: List[Dict[str, Any]],
    raw_dir: str,
    tidy_dir: str,
    raw_filename: str,
    tidy_filename: str,
) -> str:
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(tidy_dir, exist_ok=True)
    raw_path = os.path.join(raw_dir, raw_filename)
    payload = {
        "meta": {"source": "openclaw_trace_enrich", "filtered": True},
        "counts": {"records": len(records)},
        "records": records,
        "invalid_lines": [],
        "raw_lines": [],
    }
    with open(raw_path, "w", encoding="utf-8") as fw:
        json.dump(payload, fw, ensure_ascii=False, indent=2)
    return oc.build_openclaw_ollama_tidy(raw_path, tidy_dir, output_filename=tidy_filename)


def _load_tidy_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as fr:
        data = json.load(fr)
    return data if isinstance(data, list) else []


def collect_openclaw_proxy_for_window(config) -> Tuple[List[Dict[str, Any]], str]:
    """
    拉取 openclaw 命名空间下两个代理的 jsonl（整文件），按 [config.start, config.end] 过滤记录，
    生成 raw + tidy 到 data/{user}/openclaw/，并返回 (合并后的 tidy 记录列表, openclaw 根目录)。
    """
    oc = _oc()
    user = str(config.user)
    root = os.path.join(".", "data", user, "openclaw")
    raw_dir = os.path.join(root, "raw")
    tidy_dir = os.path.join(root, "tidy")
    start_u = int(config.start)
    end_u = int(config.end)
    ns = getattr(config, "openclaw_proxy_namespace", "openclaw")
    container = getattr(config, "openclaw_proxy_container", "") or ""

    merged_tidy: List[Dict[str, Any]] = []

    specs = [
        (
            getattr(config, "openclaw_dashscope_deployment", "dashscope-proxy"),
            getattr(
                config,
                "openclaw_dashscope_log_path",
                "/var/log/dashscope-proxy/openclaw_proxy_raw.jsonl",
            ),
            "openclaw_dashscope_proxy.json",
            "openclaw_dashscope_tidy.json",
        ),
        (
            getattr(config, "openclaw_ollama_deployment", "ollama-proxy"),
            getattr(
                config,
                "openclaw_ollama_log_path",
                "/var/log/ollama-proxy/openclaw_proxy_raw.jsonl",
            ),
            "openclaw_ollama_raw.json",
            "openclaw_ollama_tidy.json",
        ),
    ]

    for deployment, log_path, raw_name, tidy_name in specs:
        if not oc._k8s_deployment_exists(ns, deployment):
            print("[openclaw] skip proxy deploy/%s (not found in ns %s)" % (deployment, ns))
            continue
        raw_text = _kubectl_cat_log(ns, deployment, container, log_path)
        payload = oc.OllamaProxyRawCollector._parse_jsonl(raw_text)
        records = [r for r in payload.get("records", []) if isinstance(r, dict)]
        records = [r for r in records if _record_in_time_window(r, start_u, end_u)]
        if not records:
            print("[openclaw] no records in time window for deploy/%s" % deployment)
            continue
        tidy_path = _write_filtered_raw_and_tidy(oc, records, raw_dir, tidy_dir, raw_name, tidy_name)
        merged_tidy.extend(_load_tidy_json(tidy_path))
        print("[openclaw] deploy/%s -> %s records, tidy -> %s" % (deployment, len(records), tidy_path))

    return merged_tidy, root


def _span_tags(span: Dict[str, Any]) -> Dict[str, str]:
    out = {}
    for t in span.get("tags") or []:
        if isinstance(t, dict) and isinstance(t.get("key"), str):
            v = t.get("value")
            out[t["key"]] = v if isinstance(v, str) else str(v)
    return out


def _is_openclaw_llm_client_span(span: Dict[str, Any]) -> bool:
    tags = _span_tags(span)
    if tags.get("span.kind") != "client":
        return False
    if tags.get("istio.canonical_service") != "openclaw-gateway":
        return False
    up = tags.get("upstream_cluster") or ""
    op = span.get("operationName") or ""
    if "dashscope-proxy" in up or "ollama-proxy" in up:
        return True
    if "dashscope-proxy" in op or "ollama-proxy" in op:
        return True
    if "11435" in up or "11434" in up:
        return True
    if ":11435" in op or ":11434" in op:
        return True
    return False


def _fetch_jaeger_trace(api_root: str, trace_id: str, timeout: float = 60.0) -> Optional[Dict[str, Any]]:
    url = "%s/api/traces/%s" % (api_root.rstrip("/"), trace_id)
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json().get("data")
        if isinstance(data, list) and data:
            return data[0]
    except Exception as e:
        sys.stderr.write("[openclaw] jaeger fetch failed %s: %s\n" % (url, e))
    return None


def _skew_adj(span: Dict[str, Any]) -> int:
    a = clock_skew_adjust(span)
    if a is None:
        return 0
    try:
        return int(a)
    except (TypeError, ValueError):
        return 0


def _client_llm_adjusted_maps(
    jaeger_trace: Dict[str, Any],
) -> Tuple[Dict[int, str], Dict[int, Dict[str, Any]]]:
    """键为与 pkl 一致的 client 开始时间（微秒）= Jaeger startTime + clock_skew_adjust。"""
    start_to_sid: Dict[int, str] = {}
    start_to_span: Dict[int, Dict[str, Any]] = {}
    for span in jaeger_trace.get("spans") or []:
        if not isinstance(span, dict):
            continue
        if not _is_openclaw_llm_client_span(span):
            continue
        st = span.get("startTime")
        sid = span.get("spanID")
        if not isinstance(st, int) or not isinstance(sid, str):
            continue
        key = st + _skew_adj(span)
        start_to_sid[key] = sid
        start_to_span[key] = span
    return start_to_sid, start_to_span


def _request_id_from_span(span: Dict[str, Any]) -> Optional[str]:
    tags = _span_tags(span)
    for k in ("guid:x-request-id", "x-request-id"):
        v = tags.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _build_tidy_index(tidy_records: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx = {}
    for rec in tidy_records:
        if not isinstance(rec, dict):
            continue
        tid = (rec.get("istio_trace_id") or "").strip().lower()
        sid = (rec.get("istio_span_id") or "").strip().lower()
        if tid and sid:
            idx[(tid, sid)] = rec
    return idx


def _build_tidy_request_index(tidy_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """x-request-id -> tidy 记录（代理与 Jaeger 常用同一 request id，比 B3 span 更稳）。"""
    out: Dict[str, Dict[str, Any]] = {}
    for rec in tidy_records:
        if not isinstance(rec, dict):
            continue
        rid = rec.get("request_id")
        if isinstance(rid, str) and rid.strip():
            out[rid.strip().lower()] = rec
    return out


def _tidy_rows_for_trace_sorted(tidy_records: List[Dict[str, Any]], tid_l: str) -> List[Dict[str, Any]]:
    rows = [
        r
        for r in tidy_records
        if isinstance(r, dict) and (r.get("istio_trace_id") or "").strip().lower() == tid_l
    ]
    rows.sort(key=lambda r: (r.get("timestamp") or "", r.get("end_time") or ""))
    return rows


def _enrichment_from_tidy(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "latest_message": rec.get("latest_message"),
        "response_message": rec.get("response_message"),
        "token_input": rec.get("token_input"),
        "token_output": rec.get("token_output"),
    }


def _hop_needs_openclaw_llm(caller: str, callee: str) -> bool:
    if caller != "openclaw-gateway":
        return False
    if callee in ("dashscope-proxy", "ollama-proxy"):
        return True
    return False


def enrich_trace_dict(
    trace: Dict[str, Any],
    tidy_index: Dict[Tuple[str, str], Dict[str, Any]],
    tidy_by_request: Dict[str, Dict[str, Any]],
    tidy_records: List[Dict[str, Any]],
    jaeger_api: str,
    jaeger_cache: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    """为单条 trace 字典增加 openclaw_llm_enrichment（与 call 等长）。"""
    calls = trace.get("call")
    if not isinstance(calls, list) or not calls:
        trace["openclaw_llm_enrichment"] = []
        return trace
    n = len(calls)
    trace_id = trace.get("traceId")
    if not isinstance(trace_id, str) or not trace_id:
        trace["openclaw_llm_enrichment"] = [None] * n
        return trace

    tid_l = trace_id.lower()
    if trace_id not in jaeger_cache:
        jaeger_cache[trace_id] = _fetch_jaeger_trace(jaeger_api, trace_id)
    jt = jaeger_cache[trace_id]
    start_to_sid: Dict[int, str] = {}
    start_to_span: Dict[int, Dict[str, Any]] = {}
    if isinstance(jt, dict):
        start_to_sid, start_to_span = _client_llm_adjusted_maps(jt)

    ts = trace.get("timestamp")
    if not isinstance(ts, list):
        ts = []

    out: List[Optional[Dict[str, Any]]] = [None] * n
    for i in range(n):
        c = calls[i]
        if not isinstance(c, tuple) or len(c) < 2:
            continue
        caller, callee = c[0], c[1]
        if not _hop_needs_openclaw_llm(str(caller), str(callee)):
            continue
        if 2 * i >= len(ts):
            continue
        client_start = ts[2 * i]
        if not isinstance(client_start, int):
            continue
        span_id = start_to_sid.get(client_start)
        rec = None
        if span_id:
            rec = tidy_index.get((tid_l, span_id.lower()))
        if rec is None:
            span = start_to_span.get(client_start)
            if span:
                rid = _request_id_from_span(span)
                if rid:
                    rec = tidy_by_request.get(rid.lower())
        if rec:
            out[i] = _enrichment_from_tidy(rec)

    # 顺序回退：同一 trace 的 tidy 按时间排序，与 LLM hop 顺序一一对应（条数一致时）
    llm_idx = []
    for i in range(n):
        c = calls[i]
        if not isinstance(c, tuple) or len(c) < 2:
            continue
        if _hop_needs_openclaw_llm(str(c[0]), str(c[1])):
            llm_idx.append(i)
    per_trace = _tidy_rows_for_trace_sorted(tidy_records, tid_l)
    if per_trace and len(per_trace) == len(llm_idx):
        for j, i in enumerate(llm_idx):
            if out[i] is None and j < len(per_trace):
                out[i] = _enrichment_from_tidy(per_trace[j])

    trace["openclaw_llm_enrichment"] = out
    return trace


def _rewrite_trace_pickle(
    path: str,
    tidy_index: Dict[Tuple[str, str], Dict[str, Any]],
    tidy_by_request: Dict[str, Dict[str, Any]],
    tidy_records: List[Dict[str, Any]],
    config,
) -> None:
    if not os.path.isfile(path):
        return
    api = jaeger_api_root(getattr(config, "jaeger_url", "") or "")
    jaeger_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    with open(path, "rb") as f:
        objs = []
        while True:
            try:
                objs.append(pickle.load(f))
            except EOFError:
                break
    new_objs = []
    for obj in objs:
        if isinstance(obj, list):
            new_objs.append(
                [
                    enrich_trace_dict(t, tidy_index, tidy_by_request, tidy_records, api, jaeger_cache)
                    if isinstance(t, dict)
                    else t
                    for t in obj
                ]
            )
        else:
            new_objs.append(obj)
    with open(path, "wb") as f:
        for o in new_objs:
            pickle.dump(o, f)


def enrich_trace_pickles_under_dir(trace_dir: str, tidy_records: List[Dict[str, Any]], config) -> None:
    if not tidy_records:
        print("[openclaw] skip pkl enrich: no tidy records")
        return
    if not os.path.isdir(trace_dir):
        return
    idx = _build_tidy_index(tidy_records)
    idx_req = _build_tidy_request_index(tidy_records)
    names = [
        "normal.pkl",
        "inbound.pkl",
        "outbound.pkl",
        "abnormal.pkl",
        "inbound_half.pkl",
        "outbound_half.pkl",
        "abnormal_half.pkl",
    ]
    for name in names:
        p = os.path.join(trace_dir, name)
        if os.path.isfile(p):
            _rewrite_trace_pickle(p, idx, idx_req, tidy_records, config)
            print("[openclaw] enriched %s" % p)


def run_openclaw_collect_and_enrich(config, namespaces: List[str]) -> None:
    if not getattr(config, "openclaw_enrich_enabled", True):
        return
    try:
        tidy_list, _root = collect_openclaw_proxy_for_window(config)
    except Exception as e:
        print("[openclaw] collect failed: %s" % e)
        return
    base = os.path.join(".", "data", str(config.user))
    for ns in namespaces:
        td = os.path.join(base, ns, "trace")
        enrich_trace_pickles_under_dir(td, tidy_list, config)
