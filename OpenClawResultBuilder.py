import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        if isinstance(value, str) and value.strip() == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str) and value.strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _parse_ts(value: Any) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    return pd.to_datetime(value, errors="coerce", utc=True)


def _to_iso(value: Any) -> Optional[str]:
    ts = _parse_ts(value)
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_prompt(request_body: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(request_body, dict):
        return None, None
    messages = request_body.get("messages")
    if isinstance(messages, list) and messages:
        first = messages[0] if isinstance(messages[0], dict) else {}
        return first.get("role"), first.get("content")
    prompt = request_body.get("prompt")
    if isinstance(prompt, str):
        return "user", prompt
    return None, None


def _extract_completion(
    response_body: Any,
    response_completion_merged: Any,
) -> Tuple[Optional[str], Optional[str]]:
    if isinstance(response_body, dict):
        content = (
            (response_body.get("message") or {}).get("content")
            or response_body.get("response")
            or response_completion_merged
        )
        finish_reason = response_body.get("done_reason") or response_body.get("finish_reason")
        return content, finish_reason
    if isinstance(response_completion_merged, str) and response_completion_merged:
        return response_completion_merged, None
    return None, None


def _extract_tokens(response_body: Any) -> Tuple[int, int]:
    if not isinstance(response_body, dict):
        return 0, 0
    prompt_tokens = response_body.get("prompt_eval_count")
    completion_tokens = response_body.get("eval_count")
    if prompt_tokens is None and isinstance(response_body.get("usage"), dict):
        prompt_tokens = response_body["usage"].get("prompt_tokens")
    if completion_tokens is None and isinstance(response_body.get("usage"), dict):
        completion_tokens = response_body["usage"].get("completion_tokens")
    return _safe_int(prompt_tokens, 0), _safe_int(completion_tokens, 0)


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fr:
        return json.load(fr)


def _dump_json(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent)
    with open(path, "w", encoding="utf-8") as fw:
        json.dump(payload, fw, ensure_ascii=False, indent=2)


def _normalize_proxy_record(record: Dict[str, Any], idx: int) -> Dict[str, Any]:
    ts = _parse_ts(record.get("timestamp"))
    request_body = record.get("request_body")
    response_body = record.get("response_body")
    prompt_role, prompt_content = _extract_prompt(request_body)
    completion_content, finish_reason = _extract_completion(
        response_body=response_body,
        response_completion_merged=record.get("response_completion_merged"),
    )
    prompt_tokens, completion_tokens = _extract_tokens(response_body)
    request_model = request_body.get("model") if isinstance(request_body, dict) else None
    response_model = response_body.get("model") if isinstance(response_body, dict) else None
    temperature = None
    if isinstance(request_body, dict):
        options = request_body.get("options")
        if isinstance(options, dict):
            temperature = options.get("temperature")
        if temperature is None:
            temperature = request_body.get("temperature")

    return {
        "_proxy_index": idx,
        "_proxy_ts": ts,
        "timestamp": _to_iso(ts),
        "request_model": request_model,
        "response_model": response_model or request_model,
        "temperature": temperature,
        "prompt_role": prompt_role,
        "prompt_content": prompt_content,
        "completion_content": completion_content,
        "finish_reason": finish_reason,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "method": record.get("method"),
        "path": record.get("path"),
        "response_status": record.get("response_status"),
    }


def _match_proxy_for_usage_span(
    span: Dict[str, Any],
    proxy_rows: List[Dict[str, Any]],
    used_proxy_idx: set,
    max_time_diff_sec: float,
) -> Tuple[Optional[Dict[str, Any]], float]:
    span_ts = _parse_ts(span.get("timestamp"))
    span_model = (span.get("openclaw.model") or "").strip()
    input_tokens = _safe_int(span.get("openclaw.tokens.input"), 0)
    output_tokens = _safe_int(span.get("openclaw.tokens.output"), 0)
    if input_tokens <= 0 and output_tokens <= 0:
        return None, 0.0

    best = None
    best_score = None
    best_conf = 0.0

    for row in proxy_rows:
        pidx = row["_proxy_index"]
        if pidx in used_proxy_idx:
            continue

        if row["prompt_tokens"] != input_tokens or row["completion_tokens"] != output_tokens:
            continue

        row_model = (row.get("request_model") or "").strip()
        if span_model and row_model and span_model != row_model:
            continue

        row_ts = row["_proxy_ts"]
        if pd.isna(span_ts) or pd.isna(row_ts):
            continue
        delta = abs((span_ts - row_ts).total_seconds())
        if delta > max_time_diff_sec:
            continue

        score = delta
        if row.get("path") != "/api/chat":
            score += 0.5
        if row.get("response_status") != 200:
            score += 0.5

        if best_score is None or score < best_score:
            best = row
            best_score = score
            conf = max(0.0, 1.0 - (delta / max_time_diff_sec))
            if span_model and row_model and span_model == row_model:
                conf = min(1.0, conf + 0.05)
            best_conf = conf

    return best, best_conf


def _build_enriched_span(
    span: Dict[str, Any],
    proxy: Optional[Dict[str, Any]],
    confidence: float,
) -> Dict[str, Any]:
    attrs = dict(span.get("attributes") or {})
    provider = span.get("openclaw.provider") or attrs.get("openclaw.provider")
    model = span.get("openclaw.model") or attrs.get("openclaw.model")
    input_tokens = _safe_int(span.get("openclaw.tokens.input"), 0)
    output_tokens = _safe_int(span.get("openclaw.tokens.output"), 0)
    total_tokens = _safe_int(span.get("openclaw.tokens.total"), input_tokens + output_tokens)

    out_attrs = {
        "gen_ai.request.type": "chat",
        "gen_ai.system": provider or None,
        "gen_ai.request.model": (proxy or {}).get("request_model") or model or None,
        "gen_ai.response.model": (proxy or {}).get("response_model") or model or None,
        "gen_ai.request.temperature": (proxy or {}).get("temperature"),
        "gen_ai.prompt.0.role": (proxy or {}).get("prompt_role"),
        "gen_ai.prompt.0.content": (proxy or {}).get("prompt_content"),
        "gen_ai.completion.0.role": "assistant" if (proxy or {}).get("completion_content") else None,
        "gen_ai.completion.0.content": (proxy or {}).get("completion_content"),
        "gen_ai.completion.0.finish_reason": (proxy or {}).get("finish_reason"),
        "gen_ai.usage.prompt_tokens": input_tokens,
        "gen_ai.usage.completion_tokens": output_tokens,
        "gen_ai.usage.total_tokens": total_tokens,
        "openclaw.channel": span.get("openclaw.channel") or attrs.get("openclaw.channel"),
        "openclaw.provider": provider,
        "openclaw.model": model,
        "openclaw.session_key": span.get("openclaw.sessionKey") or attrs.get("openclaw.sessionKey"),
        "openclaw.session_id": span.get("openclaw.sessionId") or attrs.get("openclaw.sessionId"),
        "openclaw.message_id": span.get("openclaw.messageId") or attrs.get("openclaw.messageId"),
        "openclaw.outcome": span.get("openclaw.outcome") or attrs.get("openclaw.outcome"),
        "http.method": (proxy or {}).get("method"),
        "http.path": (proxy or {}).get("path"),
        "http.status_code": (proxy or {}).get("response_status"),
        "proxy.timestamp": (proxy or {}).get("timestamp"),
        "join.confidence": round(confidence, 4) if proxy else 0.0,
    }
    out_attrs.update({k: v for k, v in attrs.items() if k not in out_attrs})

    return {
        "rollout_id": None,
        "attempt_id": None,
        "sequence_id": None,
        "trace_id": span.get("trace_id") or None,
        "span_id": span.get("span_id") or None,
        "parent_span_id": span.get("parent_id") or None,
        "name": span.get("span_name") or None,
        "kind": span.get("kind") or None,
        "start_time": span.get("start_time") or span.get("timestamp"),
        "end_time": span.get("end_time") or span.get("timestamp"),
        "status_code": span.get("status_code") or None,
        "duration_ms": _safe_float(span.get("duration_ms"), 0.0),
        "attributes": out_attrs,
    }


def build_openclaw_result(
    otel_raw_path: str,
    proxy_raw_path: str,
    out_dir: str,
    max_time_diff_sec: float = 8.0,
) -> Dict[str, Any]:
    otel_payload = _load_json(otel_raw_path)
    proxy_payload = _load_json(proxy_raw_path)
    spans = list(otel_payload.get("spans") or [])
    proxy_records = list(proxy_payload.get("records") or [])
    normalized_proxy = [_normalize_proxy_record(r, i) for i, r in enumerate(proxy_records)]
    used_proxy_idx = set()

    # 1) First pass: match model usage spans with proxy records.
    direct_match: Dict[str, Tuple[Dict[str, Any], float]] = {}
    for span in spans:
        if (span.get("span_name") or "") != "openclaw.model.usage":
            continue
        key = span.get("span_id") or f"__idx_{len(direct_match)}"
        matched, confidence = _match_proxy_for_usage_span(
            span=span,
            proxy_rows=normalized_proxy,
            used_proxy_idx=used_proxy_idx,
            max_time_diff_sec=max_time_diff_sec,
        )
        if matched is not None:
            direct_match[key] = (matched, confidence)
            used_proxy_idx.add(matched["_proxy_index"])

    # 2) Build span list.
    enriched_spans: List[Dict[str, Any]] = []
    for i, span in enumerate(spans):
        skey = span.get("span_id") or f"__idx_{i}"
        matched = direct_match.get(skey)
        proxy = matched[0] if matched else None
        conf = matched[1] if matched else 0.0
        enriched_spans.append(_build_enriched_span(span=span, proxy=proxy, confidence=conf))

    # 3) Group by trace_id and assign sequence/internal index.
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for sp in enriched_spans:
        trace_id = sp.get("trace_id") or "no-trace"
        grouped.setdefault(trace_id, []).append(sp)

    traces = []
    for trace_id, items in grouped.items():
        items = sorted(items, key=lambda x: str(x.get("start_time") or ""))
        for idx, item in enumerate(items, start=1):
            item["sequence_id"] = idx
            item["attributes"]["openclaw.internal_call_index"] = idx
        traces.append(
            {
                "trace_id": None if trace_id == "no-trace" else trace_id,
                "span_count": len(items),
                "start_time": items[0].get("start_time") if items else None,
                "end_time": items[-1].get("end_time") if items else None,
                "spans": items,
            }
        )

    traces.sort(key=lambda x: str(x.get("start_time") or ""))

    unmatched_proxy = [
        {
            "timestamp": p.get("timestamp"),
            "request_model": p.get("request_model"),
            "prompt_tokens": p.get("prompt_tokens"),
            "completion_tokens": p.get("completion_tokens"),
            "path": p.get("path"),
            "response_status": p.get("response_status"),
        }
        for p in normalized_proxy
        if p["_proxy_index"] not in used_proxy_idx
    ]

    result_payload = {
        "meta": {
            "otel_raw_path": otel_raw_path,
            "proxy_raw_path": proxy_raw_path,
            "match_strategy": {
                "required": [
                    "timestamp_window",
                    "prompt_tokens_equal",
                    "completion_tokens_equal",
                ],
                "preferred": ["model_equal", "path=/api/chat", "response_status=200"],
                "max_time_diff_sec": max_time_diff_sec,
            },
        },
        "counts": {
            "otel_spans": len(spans),
            "proxy_records": len(proxy_records),
            "matched_usage_spans": len(direct_match),
            "unmatched_proxy_records": len(unmatched_proxy),
            "trace_groups": len(traces),
        },
        "traces": traces,
        "unmatched_proxy_records": unmatched_proxy,
    }

    os.makedirs(out_dir, exist_ok=True)
    result_json_path = os.path.join(out_dir, "openclaw_trace_groups.json")
    _dump_json(result_json_path, result_payload)

    flat_rows = []
    for trace in traces:
        for sp in trace["spans"]:
            flat_rows.append(
                {
                    "trace_id": sp.get("trace_id"),
                    "span_id": sp.get("span_id"),
                    "parent_span_id": sp.get("parent_span_id"),
                    "sequence_id": sp.get("sequence_id"),
                    "name": sp.get("name"),
                    "start_time": sp.get("start_time"),
                    "end_time": sp.get("end_time"),
                    "gen_ai.request.model": sp["attributes"].get("gen_ai.request.model"),
                    "gen_ai.response.model": sp["attributes"].get("gen_ai.response.model"),
                    "gen_ai.usage.prompt_tokens": sp["attributes"].get("gen_ai.usage.prompt_tokens"),
                    "gen_ai.usage.completion_tokens": sp["attributes"].get("gen_ai.usage.completion_tokens"),
                    "openclaw.internal_call_index": sp["attributes"].get("openclaw.internal_call_index"),
                    "join.confidence": sp["attributes"].get("join.confidence"),
                }
            )
    flat_df = pd.DataFrame(flat_rows)
    flat_csv_path = os.path.join(out_dir, "openclaw_trace_groups_flat.csv")
    flat_df.to_csv(flat_csv_path, index=False, encoding="utf-8-sig")

    return {
        "result_json": result_json_path,
        "result_csv": flat_csv_path,
        "counts": result_payload["counts"],
    }
