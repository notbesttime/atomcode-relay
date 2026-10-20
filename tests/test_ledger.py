# -*- coding: utf-8 -*-
"""台账/控制台回归：假上游带 usage（含 cached_tokens）与额度墙话术。

场景：
  Q1 两发非流式（fake-m）        -> usage 汇总、缓存命中、按模型/天分组
  Q2 wall-a 撞 A 池窗口墙        -> 改判 503 + 台账 wall 话术 + 窗口自记账计数
  Q3 org/wall-b 撞 B 池月度墙    -> 改判 503 + B 池本月账 walls=1
  Q4 控制台页面含额度面板        -> "A 池额度" / "B 池本月" 都在
  Q5 重启后 /stats 仍见旧 4 条   -> 历史不丢（文件是唯一事实源）
  Q6 新会话继续累计到 5 条       -> 多实例/重启都在同一台账上写
台账走临时文件（ATOMCODE_CALLLOG），绝不动真实 stats.jsonl。
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
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
        raw = self.rfile.read(n)
        st["hits"] += 1
        try:
            model = json.loads(raw.decode("utf-8", "replace")).get("model")
        except Exception:
            model = "fake-m"
        # 额度墙模型：上游拿 200 + 墙话术当正文（代理应改判 503 并打 wall）
        if model == "wall-a":
            phrase = "模型「wall-a」在当前 CodingPlan 用量窗口内已达上限，请稍后重试。"
        elif model == "org/wall-b":
            phrase = "每月token额度已不足"
        else:
            phrase = None
        if phrase:
            body = json.dumps({"id": "w%d" % st["hits"], "object": "chat.completion",
                               "created": int(time.time()), "model": model,
                               "choices": [{"index": 0,
                                            "message": {"role": "assistant",
                                                        "content": phrase},
                                            "finish_reason": "stop"}]}).encode()
        else:
            usage = {"completion_tokens": 40, "prompt_tokens": 100,
                     "total_tokens": 140,
                     "completion_tokens_details": {"reasoning_tokens": 10},
                     "prompt_tokens_details": {"cached_tokens": 60}}
            body = json.dumps({"id": "c%d" % st["hits"], "object": "chat.completion",
                               "created": int(time.time()), "model": model,
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

# 台账写进临时文件：绝不动真实 stats.jsonl（历史数据丢失的老根因）
STATS = os.path.join(tempfile.gettempdir(), "atomcode_ledger_test.jsonl")
if os.path.exists(STATS):
    os.remove(STATS)
env = dict(os.environ)
env["ATOMCODE_CALLLOG"] = STATS
logf = open(os.path.join(ROOT, "_ledger.log"), "w", encoding="utf-8")
proxy = subprocess.Popen(
    [sys.executable, "-u", os.path.join(ROOT, "atomcode2api.py"),
     "--port", str(PROXY), "--upstream", "http://127.0.0.1:%d/v1" % FAKE,
     "--no-persist"],
    env=env, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
time.sleep(4)


def post_chat(model="fake-m"):
    body = json.dumps({"model": model, "stream": False,
                       "messages": [{"role": "user", "content": LONG}]}).encode()
    req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % PROXY,
                                 data=body,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "***"})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=60).read().decode())
    except urllib.error.HTTPError as e:
        return {"__status": e.code}


def stats():
    req = urllib.request.Request("http://127.0.0.1:%d/stats?days=1" % PROXY)
    return json.loads(urllib.request.urlopen(req, timeout=30).read().decode())


post_chat()
post_chat()
wa = post_chat("wall-a")        # A 池墙：应 503 + wall 标记
wb = post_chat("org/wall-b")    # B 池月墙：应 503 + wall 标记
time.sleep(1)
s = stats()

checks = [
    ("四次调用全被记录", s["total"]["calls"] == 4),
    ("成功 2 次", s["total"]["ok"] == 2),
    ("prompt 汇总 = 200", s["total"]["prompt"] == 200),
    ("completion 汇总 = 80", s["total"]["completion"] == 80),
    ("cached 汇总 = 120（缓存命中可统计）", s["total"]["cached"] == 120),
    ("reasoning 汇总 = 20", s["total"]["reasoning"] == 20),
    ("A 池墙被改判 503", wa.get("__status") == 503),
    ("B 池墙被改判 503", wb.get("__status") == 503),
    ("wall-a 台账带墙话术", any(r.get("model") == "wall-a" and r.get("status") == 503
                              and "窗口内已达上限" in (r.get("wall") or "")
                              for r in s["recent"])),
    ("org/wall-b 台账带墙话术", any(r.get("model") == "org/wall-b"
                                   and "额度已不足" in (r.get("wall") or "")
                                   for r in s["recent"])),
    ("quota.窗口自记账含 wall-a 的撞墙计数",
     ((s.get("quota") or {}).get("window") or {}).get("by_model", {})
     .get("wall-a", {}).get("walls") == 1),
    ("quota.撞墙史有 A 池模型", "wall-a" in (s.get("quota") or {}).get("walls", {})),
    ("quota.B 池本月账有 org/wall-b",
     (s.get("quota") or {}).get("bpool_month", {}).get("org/wall-b", {}).get("walls") == 1),
    ("按模型分组存在", "fake-m" in s["by_model"]
     and s["by_model"]["fake-m"]["calls"] == 2),
    ("按天分桶存在", len(s["by_day"]) >= 1),
    ("stats.jsonl 已落盘(4行)", os.path.exists(STATS)
     and sum(1 for _ in open(STATS, encoding="utf-8")) == 4),
]

# 控制台页面可访问
try:
    req = urllib.request.Request("http://127.0.0.1:%d/console" % PROXY)
    html = urllib.request.urlopen(req, timeout=15).read().decode("utf-8")
    checks.append(("控制台页面 200 且含额度面板",
                   "atomcode-relay" in html and "A 池额度" in html and "B 池本月" in html))
except Exception as e:
    checks.append(("控制台页面 200 且含额度面板 (%s)" % e, False))

proxy.terminate()
try:
    proxy.wait(5)
except Exception:
    proxy.kill()

# ---- 场景 P：重启后历史还在吗（用户报"每次打开都是新数据"的正面回归）----
proxy2 = subprocess.Popen(
    [sys.executable, "-u", os.path.join(ROOT, "atomcode2api.py"),
     "--port", str(PROXY + 1), "--upstream", "http://127.0.0.1:%d/v1" % FAKE,
     "--no-persist"],
    env=env, cwd=ROOT, stdout=logf, stderr=subprocess.STDOUT)
time.sleep(4)
try:
    req = urllib.request.Request("http://127.0.0.1:%d/stats?days=7" % (PROXY + 1))
    s2 = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
    checks.append(("重启后 /stats 仍见旧 4 条", s2["total"]["calls"] == 4))
    checks.append(("重启后撞墙史仍在", "wall-a" in (s2.get("quota") or {}).get("walls", {})))
except Exception as e:
    checks.append(("重启后 /stats 仍见旧 4 条 (%s)" % e, False))
post_chat_model = "fake-m"
req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % (PROXY + 1),
                             data=json.dumps({"model": post_chat_model, "stream": False,
                                              "messages": [{"role": "user",
                                                            "content": LONG}]}).encode(),
                             headers={"Content-Type": "application/json",
                                      "Authorization": "***"})
urllib.request.urlopen(req, timeout=60).read()
time.sleep(1)
req = urllib.request.Request("http://127.0.0.1:%d/stats?days=7" % (PROXY + 1))
s3 = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
checks.append(("新会话继续在同一台账上累计", s3["total"]["calls"] == 5))

proxy2.terminate()
try:
    proxy2.wait(5)
except Exception:
    proxy2.kill()
logf.close()
if os.path.exists(STATS):
    os.remove(STATS)

for name, ok in checks:
    print(("  PASS  " if ok else "  FAIL  ") + name)
print("\n总结:", "全部通过" if all(o for _, o in checks) else "有未通过项")
