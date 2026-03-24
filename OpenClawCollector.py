import os
import re
import subprocess
import time
import json
from typing import Dict, List, Optional, Tuple

import pandas as pd


def _safe_float(value: Optional[str], default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _parse_attr_line(line: str) -> Optional[Tuple[str, str]]:
    # Example: "     -> openclaw.channel: Str(webchat)"
    line = line.strip()
    if "->" not in line or ":" not in line:
        return None
    raw = line.split("->", 1)[1].strip()
    key, value = raw.split(":", 1)
    key = key.strip()
    value = value.strip()
    # Convert Str(x) / Int(1) / Double(1.2) to x
    m = re.match(r"^[A-Za-z]+\((.*)\)$", value)
    if m:
        value = m.group(1)
    return key, value


def _extract_field(line: str) -> Optional[str]:
    if ":" not in line:
        return None
    return line.split(":", 1)[1].strip()


def _parse_otel_time(value: Optional[str]) -> pd.Timestamp:
    """
    Parse collector time formats like:
    2026-03-22 05:10:01.514 +0000 UTC
    """
    if not value:
        return pd.NaT
    text = str(value).strip()
    # pandas may fail with duplicated timezone markers like '+0000 UTC'
    text = re.sub(r"\s+UTC$", "", text)
    return pd.to_datetime(text, errors="coerce", utc=True)


class OpenClawOtelCollector:
    """
    Collect OpenClaw telemetry from otel-collector logs and export raw JSON.
    """

    def __init__(self, namespace: str, deployment: str, since: str, container: Optional[str] = None):
        self.namespace = namespace
        self.deployment = deployment
        self.since = since
        self.container = container

    def fetch_logs(self, watch_seconds: int = 0) -> str:
        since_raw = "" if self.since is None else str(self.since).strip().lower()
        zero_since = since_raw in ("", "0", "0s", "none", "null")

        cmd = [
            "kubectl",
            "-n",
            self.namespace,
            "logs",
            f"deploy/{self.deployment}",
        ]
        if not zero_since:
            cmd.extend(["--since", self.since])
        if self.container:
            cmd.extend(["-c", self.container])
        if watch_seconds and watch_seconds > 0:
            follow_cmd = list(cmd)
            if zero_since:
                # Start following from "now" and ignore existing log backlog.
                follow_cmd.extend(["--tail=0"])
            follow_cmd.append("-f")
            proc = subprocess.Popen(
                follow_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            try:
                # Use communicate(timeout=...) to enforce a hard stop at watch_seconds.
                stdout, _ = proc.communicate(timeout=watch_seconds)
                return stdout or ""
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    stdout, _ = proc.communicate(timeout=3)
                except Exception:
                    proc.kill()
                    stdout, _ = proc.communicate()
                return stdout or ""
            finally:
                if proc.poll() is None:
                    proc.kill()
        else:
            if zero_since:
                # One-shot + zero since: return only new lines (none at call moment).
                cmd.extend(["--tail=0"])
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    "Failed to fetch otel-collector logs.\n"
                    f"Command: {' '.join(cmd)}\n"
                    f"stderr: {proc.stderr.strip()}"
                )
            return proc.stdout

    def parse_spans(self, raw: str) -> pd.DataFrame:
        lines = raw.splitlines()
        spans: List[Dict] = []
        current = None
        in_attrs = False

        for line in lines:
            s = line.strip()

            if s.startswith("Span #"):
                if current is not None:
                    spans.append(current)
                current = {
                    "trace_id": "",
                    "span_id": "",
                    "parent_id": "",
                    "name": "",
                    "kind": "",
                    "start_time": "",
                    "end_time": "",
                    "status_code": "",
                    "attributes": {},
                }
                in_attrs = False
                continue

            if current is None:
                continue

            if s.startswith("Trace ID"):
                current["trace_id"] = _extract_field(s) or ""
            elif s.startswith("Parent ID"):
                current["parent_id"] = _extract_field(s) or ""
            elif re.match(r"^ID\s+:", s):
                current["span_id"] = _extract_field(s) or ""
            elif s.startswith("Name"):
                current["name"] = _extract_field(s) or ""
            elif s.startswith("Kind"):
                current["kind"] = _extract_field(s) or ""
            elif s.startswith("Start time"):
                current["start_time"] = _extract_field(s) or ""
            elif s.startswith("End time"):
                current["end_time"] = _extract_field(s) or ""
            elif s.startswith("Status code"):
                current["status_code"] = _extract_field(s) or ""
            elif s == "Attributes:":
                in_attrs = True
            elif in_attrs and "->" in s:
                item = _parse_attr_line(line)
                if item:
                    k, v = item
                    current["attributes"][k] = v
            elif in_attrs and (s.startswith("Span #") or s.startswith("ResourceSpans")):
                in_attrs = False

        if current is not None:
            spans.append(current)

        if not spans:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "start_time",
                    "end_time",
                    "trace_id",
                    "span_id",
                    "parent_id",
                    "span_name",
                    "kind",
                    "duration_ms",
                    "status_code",
                    "attributes",
                    "openclaw.channel",
                    "openclaw.provider",
                    "openclaw.model",
                    "openclaw.sessionKey",
                    "openclaw.sessionId",
                    "openclaw.messageId",
                    "openclaw.outcome",
                    "openclaw.tokens.input",
                    "openclaw.tokens.output",
                    "openclaw.tokens.cache_read",
                    "openclaw.tokens.cache_write",
                    "openclaw.tokens.total",
                ]
            )

        rows = []
        for sp in spans:
            start = _parse_otel_time(sp.get("start_time"))
            end = _parse_otel_time(sp.get("end_time"))
            duration_ms = 0.0
            if pd.notna(start) and pd.notna(end):
                duration_ms = (end - start).total_seconds() * 1000.0
            timestamp = start if pd.notna(start) else end

            attrs = sp.get("attributes", {})
            rows.append(
                {
                    "timestamp": timestamp,
                    "start_time": start,
                    "end_time": end,
                    "trace_id": sp.get("trace_id", ""),
                    "span_id": sp.get("span_id", ""),
                    "parent_id": sp.get("parent_id", ""),
                    "span_name": sp.get("name", ""),
                    "kind": sp.get("kind", ""),
                    "duration_ms": duration_ms,
                    "status_code": sp.get("status_code", ""),
                    "attributes": attrs,
                    "openclaw.channel": attrs.get("openclaw.channel", ""),
                    "openclaw.provider": attrs.get("openclaw.provider", ""),
                    "openclaw.model": attrs.get("openclaw.model", ""),
                    "openclaw.sessionKey": attrs.get("openclaw.sessionKey", ""),
                    "openclaw.sessionId": attrs.get("openclaw.sessionId", ""),
                    "openclaw.messageId": attrs.get("openclaw.messageId", ""),
                    "openclaw.outcome": attrs.get("openclaw.outcome", ""),
                    "openclaw.tokens.input": _safe_float(attrs.get("openclaw.tokens.input"), 0.0),
                    "openclaw.tokens.output": _safe_float(attrs.get("openclaw.tokens.output"), 0.0),
                    "openclaw.tokens.cache_read": _safe_float(attrs.get("openclaw.tokens.cache_read"), 0.0),
                    "openclaw.tokens.cache_write": _safe_float(attrs.get("openclaw.tokens.cache_write"), 0.0),
                    "openclaw.tokens.total": _safe_float(attrs.get("openclaw.tokens.total"), 0.0),
                }
            )

        df = pd.DataFrame(rows)
        # NOTE:
        # 有些环境/极端情况下，pd.DataFrame(rows) 可能不是我们预期的二维表，
        # 进而导致 df.dropna 在内部计算 count(axis=1) 时触发维度错误。
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(rows)

        expected_empty_cols = [
            "timestamp",
            "start_time",
            "end_time",
            "trace_id",
            "span_id",
            "parent_id",
            "span_name",
            "kind",
            "duration_ms",
            "status_code",
            "attributes",
            "openclaw.channel",
            "openclaw.provider",
            "openclaw.model",
            "openclaw.sessionKey",
            "openclaw.sessionId",
            "openclaw.messageId",
            "openclaw.outcome",
            "openclaw.tokens.input",
            "openclaw.tokens.output",
            "openclaw.tokens.cache_read",
            "openclaw.tokens.cache_write",
            "openclaw.tokens.total",
        ]

        if df.empty:
            return df if all(c in df.columns for c in expected_empty_cols) else pd.DataFrame(columns=expected_empty_cols)

        if "timestamp" in df.columns:
            df = df[pd.notna(df["timestamp"])]

        df = df.sort_values("timestamp")
        if not df.empty:
            return df

        # Fallback: parse TracesExporter summary lines when detailed spans are unavailable.
        fallback_rows = []
        for line in lines:
            if "TracesExporter" not in line:
                continue
            ts_match = re.match(r"^(\S+)", line.strip())
            if not ts_match:
                continue
            ts = pd.to_datetime(ts_match.group(1), errors="coerce", utc=True)
            if pd.isna(ts):
                continue
            span_count_match = re.search(r'"spans":\s*(\d+)', line)
            span_count = float(span_count_match.group(1)) if span_count_match else 0.0
            fallback_rows.append(
                {
                    "timestamp": ts,
                    "start_time": ts,
                    "end_time": ts,
                    "trace_id": "",
                    "span_id": "",
                    "parent_id": "",
                    "span_name": "otel.exporter.traces.batch",
                    "kind": "Internal",
                    "duration_ms": 0.0,
                    "status_code": "",
                    "attributes": {},
                    "openclaw.channel": "",
                    "openclaw.provider": "",
                    "openclaw.model": "",
                    "openclaw.sessionKey": "",
                    "openclaw.sessionId": "",
                    "openclaw.messageId": "",
                    "openclaw.outcome": "",
                    "openclaw.tokens.input": 0.0,
                    "openclaw.tokens.output": 0.0,
                    "openclaw.tokens.cache_read": 0.0,
                    "openclaw.tokens.cache_write": 0.0,
                    "openclaw.tokens.total": span_count,
                }
            )
        if fallback_rows:
            return pd.DataFrame(fallback_rows).sort_values("timestamp")
        return df

    def parse_metrics(self, raw: str) -> pd.DataFrame:
        lines = raw.splitlines()
        points: List[Dict] = []

        current_metric = None
        current_point = None
        in_point_attrs = False

        def flush_point():
            nonlocal current_point
            if current_metric and current_point is not None:
                points.append(current_point)
            current_point = None

        for line in lines:
            s = line.strip()

            if s.startswith("Metric #"):
                flush_point()
                current_metric = None
                continue

            if s.startswith("-> Name:"):
                current_metric = _extract_field(s)
                continue

            if s.startswith("NumberDataPoints #") or s.startswith("HistogramDataPoints #"):
                flush_point()
                current_point = {
                    "metric_name": current_metric or "",
                    "timestamp": "",
                    "value": 0.0,
                    "labels": {},
                }
                in_point_attrs = False
                continue

            if current_point is None:
                continue

            if s == "Data point attributes:":
                in_point_attrs = True
                continue

            if in_point_attrs and "->" in s:
                item = _parse_attr_line(line)
                if item:
                    k, v = item
                    current_point["labels"][k] = v
                continue

            if s.startswith("Timestamp:"):
                current_point["timestamp"] = _extract_field(s) or ""
                in_point_attrs = False
                continue

            if s.startswith("Value:"):
                current_point["value"] = _safe_float(_extract_field(s), 0.0)
                continue

            if s.startswith("Sum:"):
                # Histogram datapoint fallback numeric value
                current_point["value"] = _safe_float(_extract_field(s), 0.0)
                continue

        flush_point()

        if not points:
            return pd.DataFrame(columns=["timestamp", "metric_name", "labels", "value"])

        rows = []
        for p in points:
            ts = pd.to_datetime(p.get("timestamp"), errors="coerce", utc=True)
            if pd.isna(ts):
                continue
            rows.append(
                {
                    "timestamp": ts,
                    "metric_name": p.get("metric_name", ""),
                    "labels": p.get("labels", {}),
                    "value": _safe_float(str(p.get("value", 0.0)), 0.0),
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            # Fallback: parse MetricsExporter summary lines when detailed points are unavailable.
            fallback = []
            for line in lines:
                if "MetricsExporter" not in line:
                    continue
                ts_match = re.match(r"^(\S+)", line.strip())
                if not ts_match:
                    continue
                ts = pd.to_datetime(ts_match.group(1), errors="coerce", utc=True)
                if pd.isna(ts):
                    continue
                metrics_match = re.search(r'"metrics":\s*(\d+)', line)
                points_match = re.search(r'"data points":\s*(\d+)', line)
                fallback.append(
                    {
                        "timestamp": ts,
                        "metric_name": "otel.exporter.metrics.count",
                        "labels": {"source": "summary"},
                        "value": float(metrics_match.group(1)) if metrics_match else 0.0,
                    }
                )
                fallback.append(
                    {
                        "timestamp": ts,
                        "metric_name": "otel.exporter.metrics.data_points",
                        "labels": {"source": "summary"},
                        "value": float(points_match.group(1)) if points_match else 0.0,
                    }
                )
            if fallback:
                return pd.DataFrame(fallback).sort_values("timestamp")
            return pd.DataFrame(columns=["timestamp", "metric_name", "labels", "value"])
        return df.sort_values("timestamp")

    def parse_logs(self, raw: str) -> pd.DataFrame:
        lines = raw.splitlines()
        records: List[Dict] = []
        current = None
        in_attrs = False

        for line in lines:
            s = line.strip()
            if s.startswith("LogRecord #"):
                if current is not None:
                    records.append(current)
                current = {
                    "timestamp": "",
                    "severity": "",
                    "body": "",
                    "attributes": {},
                }
                in_attrs = False
                continue

            if current is None:
                continue

            if s.startswith("Timestamp:"):
                current["timestamp"] = _extract_field(s) or ""
            elif s.startswith("SeverityText:"):
                current["severity"] = _extract_field(s) or ""
            elif s.startswith("Body:"):
                current["body"] = _extract_field(s) or ""
            elif s == "Attributes:":
                in_attrs = True
            elif in_attrs and "->" in s:
                item = _parse_attr_line(line)
                if item:
                    k, v = item
                    current["attributes"][k] = v

        if current is not None:
            records.append(current)

        if not records:
            return pd.DataFrame(columns=["timestamp", "severity", "body", "attributes"])

        rows = []
        for r in records:
            ts = pd.to_datetime(r.get("timestamp"), errors="coerce", utc=True)
            if pd.isna(ts):
                continue
            rows.append(
                {
                    "timestamp": ts,
                    "severity": r.get("severity", ""),
                    "body": r.get("body", ""),
                    "attributes": r.get("attributes", {}),
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["timestamp", "severity", "body", "attributes"])
        return df.sort_values("timestamp")

    @staticmethod
    def _df_to_records(df: pd.DataFrame) -> List[Dict]:
        if df.empty:
            return []
        work = df.copy()
        for col in work.columns:
            if str(work[col].dtype).startswith("datetime64"):
                work[col] = work[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # pandas may produce NaN that is not valid JSON semantics.
        work = work.where(pd.notnull(work), None)
        return work.to_dict(orient="records")

    @staticmethod
    def _write_json(path: str, data):
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent)
        with open(path, "w", encoding="utf-8") as fw:
            json.dump(data, fw, ensure_ascii=False, indent=2)

    @staticmethod
    def _parse_exporter_events(raw: str) -> List[Dict]:
        events: List[Dict] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            ts_match = re.match(r"^(\S+)", line)
            if not ts_match:
                continue
            ts = ts_match.group(1)
            # Keep only exporter summary records for compact inspection.
            if "TracesExporter" in line or "MetricsExporter" in line or "LogsExporter" in line:
                event: Dict = {"timestamp": ts, "line": line}
                for key in ["data_type", "name", "resource spans", "spans", "resource metrics", "metrics", "data points"]:
                    m = re.search(rf'"{re.escape(key)}":\s*("?[^",}}]+"?)', line)
                    if m:
                        event[key] = m.group(1).strip('"')
                events.append(event)
        return events

    def export_raw_json(self, spans_df: pd.DataFrame, metrics_df: pd.DataFrame, logs_df: pd.DataFrame, raw_text: str, out_dir: str):
        """Write `raw/openclaw_otel_raw.json` with parsed spans, metrics, logs and full log lines."""
        raw_dir = os.path.join(out_dir, "raw")
        if not os.path.exists(raw_dir):
            os.makedirs(raw_dir)

        payload = {
            "meta": {
                "collector_namespace": self.namespace,
                "collector_deployment": self.deployment,
                "since": self.since,
                "container": self.container or "",
            },
            "counts": {
                "spans": int(len(spans_df)),
                "metric_points": int(len(metrics_df)),
                "log_records": int(len(logs_df)),
            },
            "spans": self._df_to_records(spans_df),
            "metrics": self._df_to_records(metrics_df),
            "logs": self._df_to_records(logs_df),
            "exporter_events": self._parse_exporter_events(raw_text),
            "raw_lines": raw_text.splitlines(),
        }
        self._write_json(os.path.join(raw_dir, "openclaw_otel_raw.json"), payload)

    def export_all(self, raw, out_dir):
        """Parse otel-collector log text and write raw JSON only."""
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        spans_df = self.parse_spans(raw)
        metrics_df = self.parse_metrics(raw)
        logs_df = self.parse_logs(raw)
        self.export_raw_json(spans_df, metrics_df, logs_df, raw, out_dir)

        return {
            "spans": int(len(spans_df)),
            "metric_points": int(len(metrics_df)),
            "log_records": int(len(logs_df)),
        }

