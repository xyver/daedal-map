"""Small localhost wait page shown while the Mapping server starts."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opening Explore Map</title><style>
:root{color-scheme:dark;font-family:system-ui,Segoe UI,sans-serif;background:#071521;color:#e8f5ff}
*{box-sizing:border-box}body{min-height:100vh;margin:0;display:grid;place-items:center;padding:24px;
background:radial-gradient(circle at 50% 35%,#143a51 0,#071521 54%)}
main{width:min(540px,100%);padding:36px;border:1px solid #24506a;border-radius:18px;
background:#0b2131;box-shadow:0 24px 80px #0008}.brand{color:#5bc6f1;font-size:13px;font-weight:800;letter-spacing:.14em}
h1{font-size:clamp(28px,5vw,40px);margin:14px 0 9px}p{line-height:1.5;color:#b3cedd;margin:0 0 26px}
.track{height:7px;border-radius:999px;overflow:hidden;background:#193a50;margin-bottom:20px}
.track::before{content:"";display:block;width:35%;height:100%;border-radius:999px;background:#58bfff;
animation:travel 1.5s ease-in-out infinite alternate}@keyframes travel{from{transform:translateX(-100%)}to{transform:translateX(290%)}}
.status{min-height:22px;color:#e8f5ff}.note{margin-top:26px;font-size:13px;color:#83a8bd}
@media(prefers-reduced-motion:reduce){.track::before{animation:none;width:100%}}
</style></head><body><main><div class="brand">DAEDALMAP</div><h1>Opening Explore Map</h1>
<p>Your local map server is starting. This page will open the map as soon as it is ready.</p>
<div class="track" aria-hidden="true"></div><div class="status" role="status" id="status">Preparing the local workspace…</div>
<div class="note">You can keep this tab open while the launcher finishes setup.</div></main>
<script>
const status=document.getElementById('status');let ticks=0;
async function check(){
  try{const response=await fetch('/status',{cache:'no-store'});const state=await response.json();
    if(state.error){status.textContent=state.error;return}
    if(state.ready){status.textContent='Map ready. Opening…';location.replace(state.map_url);return}
    ticks++;status.textContent=ticks<8?'Starting the local map server…':
      ticks<20?'Loading the map and your data library…':'Still preparing; the launcher is checking the local server…';
  }catch{status.textContent='Waiting for the launcher…'}
  setTimeout(check,700);
}check();
</script></body></html>""".encode("utf-8")


class MapLaunchPage:
    def __init__(self, map_url: str, host: str = "127.0.0.1") -> None:
        self.map_url = map_url.rstrip("/")
        self.error = ""
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/status":
                    ready = False
                    if not parent.error:
                        try:
                            with urllib.request.urlopen(parent.map_url + "/health", timeout=0.5) as response:
                                health = json.load(response)
                                ready = (response.status == 200 and health.get("status") == "healthy"
                                         and health.get("service") == "county-map-api")
                        except (OSError, urllib.error.URLError):
                            pass
                    payload = json.dumps({"ready": ready, "error": parent.error,
                                          "map_url": parent.map_url}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                elif self.path == "/":
                    payload = _PAGE
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                else:
                    self.send_error(404)
                    return
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, _format, *_args):
                pass

        self.server = ThreadingHTTPServer((host, 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}/"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
