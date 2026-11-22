"""Pipeline control panel - a single-file HTTP UI that:

  * runs the e2e pipeline on demand (etl4/run_e2e.py)
  * streams stage-by-stage progress live
  * surfaces the five Grafana dashboards once the run completes

Dependency-free: only Python's stdlib. Launch with

    python -m etl4.control.control_panel

then open http://localhost:8090 in a browser.
"""
from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[2]
PORT = 8090
GRAFANA = "http://localhost:3000"

DASHBOARDS = [
    ("01 - Ingest Throughput",   "/d/etl4-ingest-throughput/etl4-ingest-throughput"),
    ("02 - System Resources",    "/d/etl4-system-resources/etl4-system-resources"),
    ("03 - Pipeline Results",    "/d/etl4-pipeline-results/etl4-pipeline-results"),
    ("04 - Flight Paths",        "/d/etl4-flight-paths/etl4-flight-paths"),
    ("05 - Ingest Comparison",   "/d/etl4-ingest-comparison/etl4-ingest-comparison"),
]


class RunState:
    """Thread-safe singleton for the current run (if any)."""
    def __init__(self):
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None
        self.lines: deque[str] = deque(maxlen=400)
        self.status = "idle"      # idle | running | ok | error
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.exit_code: int | None = None

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "status": self.status,
                "lines": list(self.lines),
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "elapsed": (
                    (self.finished_at or time.time()) - self.started_at
                    if self.started_at else 0
                ),
                "exit_code": self.exit_code,
            }

    def start(self, args: list[str]) -> bool:
        with self.lock:
            if self.status == "running":
                return False
            self.lines.clear()
            self.status = "running"
            self.started_at = time.time()
            self.finished_at = None
            self.exit_code = None
            self.proc = subprocess.Popen(
                [sys.executable, "-u", "etl4/run_e2e.py", *args],
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        threading.Thread(target=self._pump, daemon=True).start()
        return True

    def _pump(self):
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            with self.lock:
                self.lines.append(line.rstrip())
        rc = self.proc.wait()
        with self.lock:
            self.exit_code = rc
            self.finished_at = time.time()
            self.status = "ok" if rc == 0 else "error"


STATE = RunState()


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ETL4 Pipeline Control</title>
<style>
  :root {
    --bg: #0b0e14;
    --panel: #161b22;
    --panel-2: #1f2630;
    --border: #2a3240;
    --fg: #d8dee9;
    --muted: #8a94a5;
    --accent: #4f9cff;
    --ok: #3fb950;
    --err: #f85149;
    --running: #f0b429;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.5 system-ui, -apple-system, sans-serif;
         background: var(--bg); color: var(--fg); }
  header { padding: 24px 32px; border-bottom: 1px solid var(--border);
           display: flex; align-items: center; gap: 16px; }
  header h1 { margin: 0; font-size: 20px; font-weight: 600; }
  header .sub { color: var(--muted); font-size: 13px; }
  main { max-width: 1100px; margin: 0 auto; padding: 24px 32px; }
  section { background: var(--panel); border: 1px solid var(--border);
            border-radius: 8px; padding: 20px; margin-bottom: 20px; }
  section h2 { margin: 0 0 12px; font-size: 15px; font-weight: 600;
               color: var(--muted); text-transform: uppercase;
               letter-spacing: 0.04em; }

  .controls { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
  button, .btn { background: var(--accent); color: white; border: 0;
                 padding: 10px 18px; border-radius: 6px; font-size: 14px;
                 font-weight: 600; cursor: pointer; text-decoration: none;
                 display: inline-block; transition: opacity 0.1s; }
  button:disabled, .btn.disabled { opacity: 0.4; cursor: not-allowed; }
  button:hover:not(:disabled), .btn:hover:not(.disabled) { opacity: 0.85; }
  select, input[type=text] { background: var(--panel-2); color: var(--fg);
                             border: 1px solid var(--border); padding: 8px 10px;
                             border-radius: 6px; font: inherit; }

  .status-pill { display: inline-flex; align-items: center; gap: 8px;
                 padding: 4px 12px; border-radius: 999px; font-size: 13px;
                 font-weight: 500; background: var(--panel-2); }
  .status-pill .dot { width: 8px; height: 8px; border-radius: 50%;
                      background: var(--muted); }
  .status-pill.running .dot { background: var(--running);
                              animation: pulse 1s ease-in-out infinite; }
  .status-pill.ok      .dot { background: var(--ok); }
  .status-pill.error   .dot { background: var(--err); }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

  pre.log { background: #050709; border: 1px solid var(--border);
            border-radius: 6px; padding: 14px; height: 340px; overflow: auto;
            font: 12px/1.5 ui-monospace, Consolas, monospace; margin: 0;
            color: #cfd8e3; }
  pre.log .stage { color: var(--accent); font-weight: 600; }
  pre.log .ok    { color: var(--ok); }
  pre.log .err   { color: var(--err); }

  .dashboards { display: grid; grid-template-columns:
                repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
  .dashboards.locked .btn { opacity: 0.5; }
  .dash-card { background: var(--panel-2); border: 1px solid var(--border);
               border-radius: 6px; padding: 14px; display: flex;
               flex-direction: column; gap: 10px; }
  .dash-card .name { font-weight: 600; font-size: 14px; }
  .dash-card .btn { background: transparent; color: var(--accent);
                    border: 1px solid var(--accent); text-align: center;
                    padding: 8px; font-size: 13px; }

  .meta { color: var(--muted); font-size: 12px; margin-top: 8px; }
</style>
</head>
<body>
<header>
  <h1>ETL4 Pipeline Control</h1>
  <span class="sub">Zenodo &rarr; Vault &rarr; Marts &rarr; Reconstruction Matrix &rarr; Postgres</span>
</header>
<main>

<section id="run-panel">
  <h2>Run Pipeline</h2>
  <div class="controls">
    <button id="run-btn">Run End-to-End</button>
    <label>Profiles:
      <select id="profiles">
        <option value="N0">N0 only (fast, ~10 min)</option>
        <option value="N0 N1">N0 N1 (~25 min)</option>
        <option value="N0 N1 N2 N3" selected>N0 N1 N2 N3 (full, ~90 min)</option>
      </select>
    </label>
    <label>Skip:
      <select id="skip" multiple size="3" style="min-width:140px;">
        <option value="infra">infra</option>
        <option value="schema">schema</option>
        <option value="ingest">ingest</option>
      </select>
    </label>
    <span id="status" class="status-pill"><span class="dot"></span><span id="status-text">idle</span></span>
  </div>
  <div class="meta" id="meta"></div>
</section>

<section>
  <h2>Live Log</h2>
  <pre class="log" id="log"></pre>
</section>

<section>
  <h2>Grafana Dashboards</h2>
  <div class="dashboards" id="dashboards"></div>
</section>

</main>
<script>
const GRAFANA = "__GRAFANA__";
const DASHBOARDS = __DASHBOARDS__;

const logEl       = document.getElementById("log");
const runBtn      = document.getElementById("run-btn");
const statusEl    = document.getElementById("status");
const statusText  = document.getElementById("status-text");
const metaEl      = document.getElementById("meta");
const dashEl      = document.getElementById("dashboards");

function colorizeLine(l) {
  const safe = l.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  if (/^=== .+ ===$/.test(l))     return `<span class="stage">${safe}</span>`;
  if (/: ok /.test(l))            return `<span class="ok">${safe}</span>`;
  if (/FAIL|ERROR|Traceback/.test(l)) return `<span class="err">${safe}</span>`;
  return safe;
}

function renderDashboards(unlocked) {
  dashEl.className = "dashboards" + (unlocked ? "" : " locked");
  dashEl.innerHTML = DASHBOARDS.map(([name, path]) => `
    <div class="dash-card">
      <div class="name">${name}</div>
      <a class="btn" href="${GRAFANA}${path}" target="_blank" rel="noopener">Open &rarr;</a>
    </div>`).join("");
}

async function pollStatus() {
  try {
    const r = await fetch("/status");
    const s = await r.json();
    const isRunning = s.status === "running";
    runBtn.disabled = isRunning;
    runBtn.textContent = isRunning ? "Running..." : "Run End-to-End";
    statusEl.className = "status-pill " + s.status;
    statusText.textContent = s.status;
    if (s.started_at) {
      const el = s.elapsed || 0;
      const mm = Math.floor(el / 60), ss = Math.floor(el % 60);
      const tail = s.exit_code != null ? `  exit ${s.exit_code}` : "";
      metaEl.textContent = `elapsed ${mm}m ${ss}s${tail}`;
    }
    const log = s.lines.map(colorizeLine).join("\n");
    const wasAtBottom = (logEl.scrollTop + logEl.clientHeight) >= (logEl.scrollHeight - 20);
    logEl.innerHTML = log;
    if (wasAtBottom) logEl.scrollTop = logEl.scrollHeight;
    renderDashboards(s.status === "ok" || s.status === "idle");
  } catch (e) { /* ignore transient fetch failures */ }
}

runBtn.addEventListener("click", async () => {
  const profiles = document.getElementById("profiles").value.split(/\s+/);
  const skip = [...document.getElementById("skip").selectedOptions].map(o => o.value);
  runBtn.disabled = true;
  await fetch("/run", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ profiles, skip }),
  });
  pollStatus();
});

renderDashboards(true);
pollStatus();
setInterval(pollStatus, 1200);
</script>
</body>
</html>
"""


def _render_index() -> bytes:
    page = (INDEX_HTML
            .replace("__GRAFANA__", GRAFANA)
            .replace("__DASHBOARDS__", json.dumps(DASHBOARDS)))
    return page.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def _json(self, code: int, body: dict):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            body = _render_index()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/status":
            self._json(200, STATE.snapshot())
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/run":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            body = {}
        args: list[str] = []
        profiles = body.get("profiles") or []
        skip = body.get("skip") or []
        if profiles:
            args += ["--profiles", *profiles]
        if skip:
            args += ["--skip", *skip]
        started = STATE.start(args)
        self._json(200 if started else 409,
                   {"started": started, "args": args})

    def log_message(self, fmt, *a):  # quieter stdout
        pass


def _preflight():
    """Warn about anything the panel will need but can't start itself."""
    try:
        urlopen(f"{GRAFANA}/api/health", timeout=1).read()
        print(f"[control_panel] grafana  ok   {GRAFANA}", flush=True)
    except URLError:
        print(f"[control_panel] grafana  WARN {GRAFANA} not reachable - "
              "dashboard links will 404 until `docker compose up` brings it back",
              flush=True)


def main():
    _preflight()
    url = f"http://localhost:{PORT}"
    print(f"[control_panel] serving on {url}", flush=True)
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\n[control_panel] stopped", flush=True)


if __name__ == "__main__":
    main()
