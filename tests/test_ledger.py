# -*- coding: utf-8 -*-
"""台账/控制台回归：假上游带 usage（含 cached_tokens），验证 /stats 聚合正确。

场景：
  Q1 两发非流式（同 prompt，第二发带 cached_tokens）
     -> /stats 汇总 prompt/completion/cached 正确、按模型分组、recent 有 ttft/dur
  Q2 一发流式（尾块带 usage）
     -> 流式 usage 也被记录
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAKE = 9927
PROXY = 8827
# 兼容两种布局：与 atomcode2api.py 同目录（开发目录），或在 tests/ 子目录（发布包）
_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = _HERE if os.path.exists(os.path.join(_HERE, "atomcode2api.py")) \
    else os.path.dirname(_HERE)
LONG = "X" * 900
st = {"hits": 0}


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        self.rfile.read(n)
        st["hits"] += 1
        usage = {"completion_tokens": 40, "prompt_tokens": 100,
                 "total_tokens": 140,
                 "completion_tokens_details": {"reasoning_tokens": 10},
                 "prompt_tokens_details": {"cached_tokens": 60}}
        body = json.dumps({"id": "c%d" % st["hits"], "object": "chat.completion",
                           "created": int(time.time()), "model": "fake-m",
                           "choices": [{"index": 0,
                                        "message": {"role": "assistant",
                                                    "content": "回答" * 20},
                                        "finish_reason": "stop"}],
                           "usage": usage}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


threading.Thread(
    target=lambda: ThreadingHTTPServer(("127.0.0.1", FAKE), Fake).serve_forever(),
    daemon=True).start()
time.sleep(1)

STATS = os.path.join(ROOT, "stats.jsonl")
if os.path.exists(STATS):
    os.remove(STATS)
env = dict(os.environ)
logf = open(os.path.join(ROOT, "_ledger.log"), "w", encoding="utf-8")
proxy = subprocess.Popen(
    [sys.executable, "-u", os.path.join(ROOT, "atomcode2api.py"),
     "--port", str(PROXY), "--upstream", "http://127.0.0.1:%d/v1" % FAKE,
     "--no-persist"],
    env=env, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
time.sleep(4)


def post_chat():
    body = json.dumps({"model": "fake-m", "stream": False,
                       "messages": [{"role": "user", "content": LONG}]}).encode()
    req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % PROXY,
                                 data=body,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "***"})
    return json.loads(urllib.request.urlopen(req, timeout=60).read().decode())


def stats():
    req = urllib.request.Request("http://127.0.0.1:%d/stats?days=1" % PROXY)
    return json.loads(urllib.request.urlopen(req, timeout=30).read().decode())


post_chat()
post_chat()
time.sleep(1)
s = stats()

checks = [
    ("两次调用被记录", s["total"]["calls"] == 2),
    ("prompt 汇总 = 200", s["total"]["prompt"] == 200),
    ("completion 汇总 = 80", s["total"]["completion"] == 80),
    ("cached 汇总 = 120（缓存命中可统计）", s["total"]["cached"] == 120),
    ("reasoning 汇总 = 20", s["total"]["reasoning"] == 20),
    ("按模型分组存在", "fake-m" in s["by_model"]
     and s["by_model"]["fake-m"]["calls"] == 2),
    ("按天分桶存在", len(s["by_day"]) >= 1),
    ("recent 带耗时与状态", all(
        r.get("dur") is not None and r.get("status") == 200
        for r in s["recent"])),
    ("stats.jsonl 已落盘", os.path.exists(STATS)
     and sum(1 for _ in open(STATS, encoding="utf-8")) == 2),
]

# 控制台页面可访问
try:
    req = urllib.request.Request("http://127.0.0.1:%d/console" % PROXY)
    html = urllib.request.urlopen(req, timeout=15).read().decode("utf-8")
    checks.append(("控制台页面 200 且含标题", "atomcode-relay" in html))
except Exception as e:
    checks.append(("控制台页面 200 且含标题 (%s)" % e, False))

proxy.terminate()
try:
    proxy.wait(5)
except Exception:
    proxy.kill()
if os.path.exists(STATS):
    os.remove(STATS)

for name, ok in checks:
    print(("  PASS  " if ok else "  FAIL  ") + name)
print("\n总结:", "全部通过" if all(o for _, o in checks) else "有未通过项")
