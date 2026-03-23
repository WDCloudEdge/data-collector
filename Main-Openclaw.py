import argparse
import json
import os
import subprocess
import threading
import time
from typing import Dict, List

from OpenClawCollector import OpenClawOtelCollector
from OpenClawResultBuilder import build_openclaw_result

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
    def _parse_jsonl(raw_text: str) -> Dict:
        raw_lines = raw_text.splitlines()
        records = []
        invalid_lines = []
        for idx, line in enumerate(raw_lines, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                records.append(json.loads(s))
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
    result_out_dir = os.path.join(out_dir, "result")
    merged_result = build_openclaw_result(
        otel_raw_path=otel_raw_path,
        proxy_raw_path=proxy_result["path"],
        out_dir=result_out_dir,
    )

    print(f"\nOpenClaw export completed: {out_dir}")
    print(f"  Raw JSON: {otel_raw_path}")
    print(f"  Proxy Raw JSON: {proxy_result['path']}")
    print(f"  Result JSON: {merged_result['result_json']}")
    print(f"  Result CSV: {merged_result['result_csv']}")
    print(
        f"  Spans: {result['spans']}, metric points: {result['metric_points']}, "
        f"log records: {result['log_records']}"
    )
    print(
        f"  Proxy lines: {proxy_result['raw_lines']}, parsed records: {proxy_result['records']}, "
        f"invalid lines: {proxy_result['invalid_lines']}"
    )
    print(
        "  Matched usage spans: "
        f"{merged_result['counts']['matched_usage_spans']}, "
        f"trace groups: {merged_result['counts']['trace_groups']}, "
        f"unmatched proxy records: {merged_result['counts']['unmatched_proxy_records']}"
    )


if __name__ == "__main__":
    main()