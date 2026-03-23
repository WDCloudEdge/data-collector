import argparse
import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from OpenClawCollector import OpenClawOtelCollector

DEFAULT_COLLECTOR_NAMESPACE = "openclaw"
DEFAULT_COLLECTOR_DEPLOYMENT = "otel-collector-verify"
DEFAULT_COLLECTOR_CONTAINER = ""
DEFAULT_SINCE = "0"
DEFAULT_USER = "openclaw"
DEFAULT_OUTPUT_ROOT = "./data"
DEFAULT_PROXY_NAMESPACE = "openclaw"
DEFAULT_PROXY_DEPLOYMENT = "ollama-proxy"
DEFAULT_PROXY_CONTAINER = ""
DEFAULT_PROXY_LOG_PATH = "/var/log/ollama-proxy/openclaw_proxy_raw.jsonl"


def _parse_iso_datetime(value: Any):
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    # Robustly parse second-level datetime first, ignoring fractional/timezone tail.
    # Examples supported:
    # - 2026-03-23T19:46:34.500Z
    # - 2026-03-23T19:46:34Z
    # - 2026-03-23 19:46:34+00:00
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})", text)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # Fallback: accept common UTC form like "2026-03-23T18:09:34.816Z".
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso_seconds(dt):
    if dt is None:
        return None
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _duration_seconds(start_dt, end_dt):
    if start_dt is None or end_dt is None:
        return None
    return int(round((end_dt - start_dt).total_seconds()))


def _to_single_line(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.replace("\r", " ").replace("\n", " ").split())


def _write_json(path: str, data: Any):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fw:
        json.dump(data, fw, ensure_ascii=False, indent=2)


def _pick_span_value(span: Dict[str, Any], attrs: Dict[str, Any], key: str):
    val = span.get(key)
    if val is not None and val != "":
        return val
    return attrs.get(key)


def build_openclaw_otel_tidy(otel_raw_path: str, tidy_dir: str) -> str:
    with open(otel_raw_path, "r", encoding="utf-8") as fr:
        raw_obj = json.load(fr)

    spans = raw_obj.get("spans", [])
    tidy_spans = []
    for span in spans:
        if not isinstance(span, dict):
            continue
        attrs = span.get("attributes")
        if not isinstance(attrs, dict):
            attrs = {}

        start_dt = _parse_iso_datetime(span.get("timestamp")) or _parse_iso_datetime(span.get("start_time"))
        end_dt = _parse_iso_datetime(span.get("end_time"))
        tidy_spans.append(
            {
                "timestamp": _to_iso_seconds(start_dt),
                "end_time": _to_iso_seconds(end_dt),
                "trace_id": span.get("trace_id", ""),
                "span_id": span.get("span_id", ""),
                "parent_id": span.get("parent_id", ""),
                "sessionKey": _pick_span_value(span, attrs, "openclaw.sessionKey"),
                "sessionId": _pick_span_value(span, attrs, "openclaw.sessionId"),
                "channel": _pick_span_value(span, attrs, "openclaw.channel"),
                "provider": _pick_span_value(span, attrs, "openclaw.provider"),
                "model": _pick_span_value(span, attrs, "openclaw.model"),
                "token_input": _pick_span_value(span, attrs, "openclaw.tokens.input"),
                "token_output": _pick_span_value(span, attrs, "openclaw.tokens.output"),
                "duration": _duration_seconds(start_dt, end_dt),
            }
        )

    out_path = os.path.join(tidy_dir, "openclaw_otel_tidy.json")
    _write_json(out_path, tidy_spans)
    return out_path


def _compact_message(msg: Any) -> Dict[str, Any]:
    if not isinstance(msg, dict):
        return {"role": "", "content": "", "tool_calls": []}
    role = msg.get("role")
    if not isinstance(role, str):
        role = ""
    tool_calls = msg.get("tool_calls")
    if not isinstance(tool_calls, list):
        tool_calls = []
    return {
        "role": role,
        "content": _to_single_line(msg.get("content")),
        "tool_calls": tool_calls,
    }


def build_openclaw_ollama_tidy(ollama_raw_path: str, tidy_dir: str) -> str:
    with open(ollama_raw_path, "r", encoding="utf-8") as fr:
        raw_obj = json.load(fr)

    records = raw_obj.get("records", [])
    tidy_records = []
    for record in records:
        if not isinstance(record, dict):
            continue

        start_dt = _parse_iso_datetime(record.get("timestamp"))
        response_body = record.get("response_body")
        if not isinstance(response_body, dict):
            response_body = {}
        end_dt = _parse_iso_datetime(response_body.get("created_at"))
        if end_dt is None:
            response_headers = record.get("response_headers")
            if isinstance(response_headers, dict):
                date_header = response_headers.get("date")
                if isinstance(date_header, str) and date_header:
                    try:
                        # Example: "Mon, 23 Mar 2026 18:10:44 GMT"
                        end_dt = datetime.strptime(date_header, "%a, %d %b %Y %H:%M:%S GMT").replace(
                            tzinfo=timezone.utc
                        )
                    except Exception:
                        end_dt = None

        request_body = record.get("request_body")
        if not isinstance(request_body, dict):
            request_body = {}
        request_messages_raw = request_body.get("messages")
        if not isinstance(request_messages_raw, list):
            request_messages_raw = []

        request_messages = [_compact_message(m) for m in request_messages_raw if isinstance(m, dict)]
        latest_message = request_messages[-1] if request_messages else None
        response_message = _compact_message(response_body.get("message"))

        tidy_records.append(
            {
                "timestamp": _to_iso_seconds(start_dt),
                "end_time": _to_iso_seconds(end_dt),
                "model": response_body.get("model") or request_body.get("model"),
                "token_input": response_body.get("prompt_eval_count"),
                "token_output": response_body.get("eval_count"),
                "duration": _duration_seconds(start_dt, end_dt),
                "latest_message": latest_message,
                "request_messages": request_messages,
                "response_message": response_message,
            }
        )

    out_path = os.path.join(tidy_dir, "openclaw_ollama_tidy.json")
    _write_json(out_path, tidy_records)
    return out_path


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _within_one_second(left_dt, right_dt) -> bool:
    if left_dt is None or right_dt is None:
        return False
    return abs((left_dt - right_dt).total_seconds()) <= 1.0


def _tokens_match(span: Dict[str, Any], token_input_sum: float, token_output_sum: float) -> bool:
    span_input = _as_float(span.get("token_input"), 0.0)
    span_output = _as_float(span.get("token_output"), 0.0)
    # Allow tiny differences due to rounding/serialization.
    return abs(token_input_sum - span_input) <= 1.0 and abs(token_output_sum - span_output) <= 1.0


def build_openclaw_result(otel_tidy_path: str, ollama_tidy_path: str, result_dir: str) -> str:
    with open(otel_tidy_path, "r", encoding="utf-8") as fr:
        otel_spans = json.load(fr)
    with open(ollama_tidy_path, "r", encoding="utf-8") as fr:
        ollama_records = json.load(fr)

    if not isinstance(otel_spans, list):
        otel_spans = []
    if not isinstance(ollama_records, list):
        ollama_records = []

    used_record_indices = set()
    merged = []

    for span in otel_spans:
        if not isinstance(span, dict):
            continue
        if str(span.get("channel", "")) != "webchat":
            continue

        span_model = span.get("model")
        span_start_dt = _parse_iso_datetime(span.get("timestamp"))
        span_end_dt = _parse_iso_datetime(span.get("end_time"))

        matched_records: List[Dict[str, Any]] = []
        matched_indices: List[int] = []

        # Find a chain:
        # 1) first record timestamp aligns with span timestamp (<=1s)
        # 2) subsequent records self-align: prev.end_time -> next.timestamp (<=1s)
        # 3) chain final end_time aligns with span end_time (<=1s)
        # 4) summed token_input/output align with span token_input/output
        for i, candidate in enumerate(ollama_records):
            if i in used_record_indices:
                continue
            if not isinstance(candidate, dict):
                continue
            if candidate.get("model") != span_model:
                continue

            first_start_dt = _parse_iso_datetime(candidate.get("timestamp"))
            if not _within_one_second(span_start_dt, first_start_dt):
                continue

            chain: List[Dict[str, Any]] = []
            chain_indices: List[int] = []
            token_input_sum = 0.0
            token_output_sum = 0.0
            prev_end_dt = None

            for j in range(i, len(ollama_records)):
                if j in used_record_indices:
                    break
                rec = ollama_records[j]
                if not isinstance(rec, dict):
                    continue
                if rec.get("model") != span_model:
                    break

                rec_start_dt = _parse_iso_datetime(rec.get("timestamp"))
                rec_end_dt = _parse_iso_datetime(rec.get("end_time"))

                if len(chain) == 0:
                    if not _within_one_second(span_start_dt, rec_start_dt):
                        break
                else:
                    if not _within_one_second(prev_end_dt, rec_start_dt):
                        break

                chain.append(rec)
                chain_indices.append(j)
                token_input_sum += _as_float(rec.get("token_input"), 0.0)
                token_output_sum += _as_float(rec.get("token_output"), 0.0)

                prev_end_dt = rec_end_dt if rec_end_dt is not None else rec_start_dt

                if (
                    _within_one_second(prev_end_dt, span_end_dt)
                    and _tokens_match(span, token_input_sum, token_output_sum)
                ):
                    matched_records = chain
                    matched_indices = chain_indices
                    break

            if matched_records:
                break

        for idx in matched_indices:
            used_record_indices.add(idx)

        merged.append(
            {
                "timestamp": span.get("timestamp"),
                "end_time": span.get("end_time"),
                "trace_id": span.get("trace_id"),
                "span_id": span.get("span_id"),
                "parent_id": span.get("parent_id"),
                "sessionKey": span.get("sessionKey"),
                "sessionId": span.get("sessionId"),
                "channel": span.get("channel"),
                "provider": span.get("provider"),
                "model": span.get("model"),
                "token_input": span.get("token_input"),
                "token_output": span.get("token_output"),
                "duration": span.get("duration"),
                "records": matched_records,
            }
        )

    out_path = os.path.join(result_dir, "result.json")
    _write_json(out_path, merged)
    return out_path


def _start_progress_timer(watch_seconds: int):
    """
    Print elapsed collection time every 30 seconds.
    Returns a stop function that should be called when collection ends.
    """
    if watch_seconds <= 0:
        return lambda: None

    stop_event = threading.Event()
    start_ts = time.monotonic()

    def _worker():
        tick = 30
        while not stop_event.wait(1):
            elapsed = int(time.monotonic() - start_ts)
            if elapsed >= tick:
                print(f"[timer] 已运行 {elapsed}s / {watch_seconds}s")
                tick += 30

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()

    def _stop():
        stop_event.set()
        thread.join(timeout=1)

    return _stop


class OllamaProxyRawCollector:
    """
    Collect raw jsonl logs from ollama-proxy pod file and export json.
    """

    def __init__(self, namespace: str, deployment: str, log_path: str, container: str = ""):
        self.namespace = namespace
        self.deployment = deployment
        self.log_path = log_path
        self.container = container.strip()

    def _run(self, cmd: List[str]) -> str:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                "Failed to run kubectl command for ollama-proxy.\n"
                f"Command: {' '.join(cmd)}\n"
                f"stderr: {proc.stderr.strip()}"
            )
        return proc.stdout

    def _exec_shell(self, shell_script: str) -> str:
        cmd = [
            "kubectl",
            "-n",
            self.namespace,
            "exec",
            f"deploy/{self.deployment}",
        ]
        if self.container:
            cmd.extend(["-c", self.container])
        cmd.extend(["--", "sh", "-c", shell_script])
        return self._run(cmd)

    def mark_start_line(self) -> int:
        # 1-based line number to read from later
        out = self._exec_shell(f"if [ -f '{self.log_path}' ]; then wc -l < '{self.log_path}'; else echo 0; fi")
        try:
            current = int((out or "0").strip())
        except Exception:
            current = 0
        return current + 1

    def fetch_since_line(self, start_line: int) -> str:
        script = (
            f"if [ -f '{self.log_path}' ]; then "
            f"sed -n '{max(1, int(start_line))},$p' '{self.log_path}'; "
            "fi"
        )
        return self._exec_shell(script)

    @staticmethod
    def _dedupe_tool_calls(tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique_calls: List[Dict[str, Any]] = []
        for item in tool_calls:
            try:
                key = json.dumps(item, ensure_ascii=False, sort_keys=True)
            except Exception:
                key = str(item)
            if key in seen:
                continue
            seen.add(key)
            unique_calls.append(item)
        return unique_calls

    @classmethod
    def _merge_response_stream(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge streamed Ollama ndjson chunks from `response_body_raw` back into
        `response_body.message`:
        - fill message.content (fallback to merged thinking when content is empty)
        - attach merged tool_calls when present
        - expose `response_completion_merged` for downstream analysis
        """
        if not isinstance(record, dict):
            return record

        raw_stream = record.get("response_body_raw")
        if not isinstance(raw_stream, str) or not raw_stream.strip():
            return record

        chunks: List[Dict[str, Any]] = []
        for line in raw_stream.splitlines():
            s = line.strip()
            if not s:
                continue
            try:
                item = json.loads(s)
            except Exception:
                continue
            if isinstance(item, dict):
                chunks.append(item)

        if not chunks:
            return record

        content_parts: List[str] = []
        thinking_parts: List[str] = []
        all_tool_calls: List[Dict[str, Any]] = []

        for chunk in chunks:
            msg = chunk.get("message")
            if not isinstance(msg, dict):
                continue

            content = msg.get("content")
            if isinstance(content, str) and content:
                content_parts.append(content)

            thinking = msg.get("thinking")
            if isinstance(thinking, str) and thinking:
                thinking_parts.append(thinking)

            tool_calls = msg.get("tool_calls")
            if isinstance(tool_calls, list):
                all_tool_calls.extend([tc for tc in tool_calls if isinstance(tc, dict)])

        merged_content = "".join(content_parts)
        merged_thinking = "".join(thinking_parts)
        merged_completion = merged_content if merged_content else merged_thinking
        merged_tool_calls = cls._dedupe_tool_calls(all_tool_calls)

        body = record.get("response_body")
        if not isinstance(body, dict):
            body = {}
            record["response_body"] = body

        message = body.get("message")
        if not isinstance(message, dict):
            message = {"role": "assistant", "content": ""}
            body["message"] = message

        current_content = message.get("content")
        if (not isinstance(current_content, str)) or (not current_content):
            if merged_completion:
                message["content"] = merged_completion

        if merged_tool_calls:
            existing_tool_calls = message.get("tool_calls")
            if isinstance(existing_tool_calls, list):
                merged = existing_tool_calls + merged_tool_calls
                message["tool_calls"] = cls._dedupe_tool_calls([tc for tc in merged if isinstance(tc, dict)])
            else:
                message["tool_calls"] = merged_tool_calls

        record["response_completion_merged"] = merged_completion if merged_completion else None
        return record

    @staticmethod
    def _parse_jsonl(raw_text: str) -> Dict:
        raw_lines = raw_text.splitlines()
        records = []
        invalid_lines = []
        for idx, line in enumerate(raw_lines, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                record = json.loads(s)
                if isinstance(record, dict):
                    record = OllamaProxyRawCollector._merge_response_stream(record)
                records.append(record)
            except Exception:
                invalid_lines.append({"line_no": idx, "line": line})
        return {
            "raw_lines": raw_lines,
            "records": records,
            "invalid_lines": invalid_lines,
        }

    def export_raw_json(self, raw_text: str, out_dir: str) -> Dict:
        payload = self._parse_jsonl(raw_text)
        output = {
            "meta": {
                "proxy_namespace": self.namespace,
                "proxy_deployment": self.deployment,
                "proxy_container": self.container,
                "proxy_log_path": self.log_path,
            },
            "counts": {
                "raw_lines": int(len(payload["raw_lines"])),
                "records": int(len(payload["records"])),
                "invalid_lines": int(len(payload["invalid_lines"])),
            },
            "records": payload["records"],
            "invalid_lines": payload["invalid_lines"],
            "raw_lines": payload["raw_lines"],
        }
        raw_dir = os.path.join(out_dir, "raw")
        os.makedirs(raw_dir, exist_ok=True)
        out_path = os.path.join(raw_dir, "openclaw_ollama_raw.json")
        with open(out_path, "w", encoding="utf-8") as fw:
            json.dump(output, fw, ensure_ascii=False, indent=2)
        return {
            "path": out_path,
            "raw_lines": output["counts"]["raw_lines"],
            "records": output["counts"]["records"],
            "invalid_lines": output["counts"]["invalid_lines"],
        }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect OpenClaw telemetry. Only one argument is required: monitoring seconds."
    )
    parser.add_argument(
        "watch_seconds",
        type=int,
        help="Monitoring duration in seconds, e.g. 180.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = os.path.join(DEFAULT_OUTPUT_ROOT, str(DEFAULT_USER))
    collector = OpenClawOtelCollector(
        namespace=DEFAULT_COLLECTOR_NAMESPACE,
        deployment=DEFAULT_COLLECTOR_DEPLOYMENT,
        since=DEFAULT_SINCE,
        container=DEFAULT_COLLECTOR_CONTAINER if DEFAULT_COLLECTOR_CONTAINER else None,
    )
    proxy_collector = OllamaProxyRawCollector(
        namespace=DEFAULT_PROXY_NAMESPACE,
        deployment=DEFAULT_PROXY_DEPLOYMENT,
        container=DEFAULT_PROXY_CONTAINER,
        log_path=DEFAULT_PROXY_LOG_PATH,
    )

    print(
        "Collecting OpenClaw telemetry from "
        f"deploy/{DEFAULT_COLLECTOR_DEPLOYMENT} in ns/{DEFAULT_COLLECTOR_NAMESPACE}, "
        f"since={DEFAULT_SINCE}, watch_seconds={args.watch_seconds}"
    )
    print(
        "Collecting Ollama proxy raw logs from "
        f"deploy/{DEFAULT_PROXY_DEPLOYMENT} in ns/{DEFAULT_PROXY_NAMESPACE}, "
        f"log_path={DEFAULT_PROXY_LOG_PATH}"
    )
    proxy_start_line = proxy_collector.mark_start_line()
    stop_timer = _start_progress_timer(args.watch_seconds)
    try:
        raw = collector.fetch_logs(watch_seconds=args.watch_seconds)
    finally:
        stop_timer()
    result = collector.export_all(raw, out_dir)
    proxy_raw = proxy_collector.fetch_since_line(proxy_start_line)
    proxy_result = proxy_collector.export_raw_json(proxy_raw, out_dir)
    otel_raw_path = os.path.join(out_dir, "raw", "openclaw_otel_raw.json")
    tidy_dir = os.path.join(out_dir, "tidy")
    otel_tidy_path = build_openclaw_otel_tidy(otel_raw_path, tidy_dir)
    ollama_tidy_path = build_openclaw_ollama_tidy(proxy_result["path"], tidy_dir)
    result_path = build_openclaw_result(
        otel_tidy_path=otel_tidy_path,
        ollama_tidy_path=ollama_tidy_path,
        result_dir=os.path.join(out_dir, "result"),
    )

    print(f"\nOpenClaw export completed: {out_dir}")
    print(f"  Raw JSON: {otel_raw_path}")
    print(f"  Proxy Raw JSON: {proxy_result['path']}")
    print(f"  Tidy OTEL JSON: {otel_tidy_path}")
    print(f"  Tidy Ollama JSON: {ollama_tidy_path}")
    print(f"  Result JSON: {result_path}")
    print(
        f"  Spans: {result['spans']}, metric points: {result['metric_points']}, "
        f"log records: {result['log_records']}"
    )
    print(
        f"  Proxy lines: {proxy_result['raw_lines']}, parsed records: {proxy_result['records']}, "
        f"invalid lines: {proxy_result['invalid_lines']}"
    )


if __name__ == "__main__":
    main()