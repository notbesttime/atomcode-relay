"""假上游回归：透明重开 + 补发 [DONE] + 伪错误拦截 + 真流式。

场景：
  A. 首个可见事件前静默，第二次正常  -> 期望：自动重开，正文完整（上游命中>=2）
  B. 正常流 finish+[DONE]           -> 期望：一次成功，绝不重开（上游命中==1）
  C. 到 finish_reason 就 EOF，缺[DONE] -> 期望：代理补 [DONE]
  D. 只有 finish_reason:null 就 EOF  -> 期望：**不能**补 [DONE]（没写完）
  E. 200 + 中文伪错误                -> 期望：改判 503，错误文本绝不漏进正文
  F. 真流式                         -> 期望：收到**第一个字节**早于收到最后一个字节很远
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FAKE = 9923
PROXY = 8823
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
state = {"hits": 0, "script": "B"}


def dl(c):
    return {"choices": [{"index": 0, "delta": {"content": c},
                         "finish_reason": None}]}


def fin():
    return {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _hdr(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

    def _send(self, obj):
        blob = (b"data: " + json.dumps(obj).encode() + b"\n\n")
        self.wfile.write(b"%X\r\n%s\r\n" % (len(blob), blob))
        self.wfile.flush()

    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        self.rfile.read(n)
        state["hits"] += 1
        sc = state["script"]
        try:
            if sc == "A":
                if state["hits"] == 1:
                    self._hdr()
                    self._send({"choices": [{"index": 0,
                                             "delta": {"role": "assistant"},
                                             "finish_reason": None}]})
                    time.sleep(60)          # 元数据后静默
                    return
                self._hdr()
                for d in (dl("重开后的"), dl("完整回复")):
                    self._send(d)
                self._send(fin())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return

            if sc == "B":
                self._hdr()
                for d in (dl("正常"), dl("回复")):
                    self._send(d)
                    time.sleep(0.05)
                self._send(fin())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return

            if sc == "C":
                self._hdr()
                for d in (dl("已生成完"), dl("的答案")):
                    self._send(d)
                self._send(fin())
                # 不发 [DONE]，直接 EOF
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass
                return

            if sc == "D":
                self._hdr()
                for d in (dl("写了一半"), dl("还没结束")):
                    self._send(d)
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass
                return

            if sc == "E":
                self._hdr()
                self._send(dl("模型「x」的请求负载过高，请稍后再试。"))
                self._send(fin())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return

            if sc == "G":
                # 用户症状：先吐可见内容（于是流已开、不能重发），然后静默到死
                self._hdr()
                self._send(dl("前半段正常输出"))
                self._send(dl("，然后就"))
                time.sleep(120)            # 静默超过 STALL_S
                return

            if sc == "F":
                self._hdr()
                for i in range(40):
                    self._send(dl("词%02d " % i))
                    time.sleep(0.12)        # 共约 4.8s
                self._send(fin())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return

            if sc == "P":
                # 复读退化：同一周期狂刷 12 份。熔断应在 5~6 份处切断，
                # 而不是陪它刷完 + 挂死。
                self._hdr()
                self._send(dl("开头正常内容。"))
                unit = "这是一个被反复输出的句子用来验证透传路熔断能不能识别"
                for i in range(12):
                    self._send(dl(unit))
                    time.sleep(0.05)
                self._send(fin())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return
        except Exception:
            pass


threading.Thread(
    target=lambda: ThreadingHTTPServer(("127.0.0.1", FAKE), Fake).serve_forever(),
    daemon=True).start()
time.sleep(1)

env = dict(os.environ)
env.update({"ATOMCODE_FIRST_EVENT": "3", "ATOMCODE_STALL": "8",
            "ATOMCODE_STREAM_ATTEMPTS": "3", "ATOMCODE_RETRY_BACKOFF": "0.2"})
logf = open(os.path.join(HERE, "_pt.log"), "w", encoding="utf-8")
proxy = subprocess.Popen(
    [sys.executable, "-u", os.path.join(HERE, "atomcode2api.py"),
     "--port", str(PROXY), "--upstream", "http://127.0.0.1:%d/v1" % FAKE,
     "--no-persist"],
    env=env, cwd=HERE, stdout=logf, stderr=subprocess.STDOUT)
time.sleep(4)


def chat_raw(stall_probe=False):
    body = json.dumps({"model": "fake", "stream": True,
                       "messages": [{"role": "user", "content": "x"}]}).encode()
    req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % PROXY,
                                 data=body,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer sk"},
                                 method="POST")
    t0 = time.time()
    first_byte = None
    parts = ""
    done = False
    err = None
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            status = r.status
            raw = b""
            while True:
                ch = r.read1(4096) if hasattr(r, "read1") else r.read(1)
                if not ch:
                    break
                if first_byte is None:
                    first_byte = time.time() - t0
                raw += ch
            for ln in raw.decode("utf-8", "replace").splitlines():
                t = ln.strip()
                if not t.startswith("data:"):
                    continue
                p = t[5:].strip()
                if p == "[DONE]":
                    done = True
                    continue
                try:
                    d = json.loads(p)
                except Exception:
                    continue
                if d.get("error"):
                    err = d["error"].get("message", "")[:40]
                for c in d.get("choices") or []:
                    parts += (c.get("delta") or {}).get("content") or ""
    except urllib.error.HTTPError as e:
        status = e.code
        raw = e.read()[:200].decode("utf-8", "replace")
        first_byte = time.time() - t0
        if "负载过高" in raw:
            err = "masked->status"
        return {"status": status, "text": "", "done": False,
                "first": round(first_byte, 2), "total": round(time.time() - t0, 2),
                "err": "503" if status == 503 else raw[:40],
                "hits": state["hits"]}
    except Exception as e:
        return {"status": "ERR", "text": "", "done": False, "first": 0,
                "total": round(time.time() - t0, 2), "err": str(e)[:50],
                "hits": state["hits"]}
    return {"status": status, "text": parts, "done": done,
            "first": round(first_byte or 0, 2), "total": round(time.time() - t0, 2),
            "err": err, "hits": state["hits"]}


out = {}
for case in ("B", "A", "C", "D", "E", "F", "G", "P"):
    state["script"] = case
    state["hits"] = 0
    Proxy = chat_raw()
    out[case] = Proxy
    print("  %s: %s" % (case, Proxy))

logf.flush()
log = open(os.path.join(HERE, "_pt.log"), encoding="utf-8", errors="replace").read()
proxy.terminate()
try:
    proxy.wait(5)
except Exception:
    proxy.kill()

print("\n代理日志关键行:")
for l in log.splitlines():
    if any(k in l for k in ("reopen", "stall", "[tail]", "503")):
        print("   " + l)

B, A, C, D, E, F, G = (out[k] for k in "BACDEFG")
P = out["P"]
_unit = "这是一个被反复输出的句子用来验证透传路熔断能不能识别"
checks = [
    ("B 正常流一次成功、无重开", B["status"] == 200 and B["text"] == "正常回复"
     and B["done"] and B["hits"] == 1),
    ("A 元数据后静默 -> 自动重开成功", A["status"] == 200 and A["text"] == "重开后的完整回复"
     and A["hits"] >= 2),
    ("C finish后缺[DONE] -> 补齐", C["status"] == 200 and C["done"] is True
     and C["text"] == "已生成完的答案"),
    ("D 未写完 -> 绝不补[DONE]", D["status"] == 200 and D["done"] is False),
    ("E 伪错误 -> 503 且不漏正文", E["status"] == 503 and E["text"] == ""),
    ("F 真流式：首字节远早于结束", F["status"] == 200 and F["first"] < 1.5
     and F["total"] > 3.0),
    # G = 用户症状：流已开、吐了内容后上游静默。必须"结束"（不能永远挂着），
    # 且已输出内容保留、绝不因重发而变两遍。
    ("G 流中途静默 -> 按时收尾不悬挂", G["status"] == 200 and G["done"] is False
     and G["text"] == "前半段正常输出，然后就" and 6.0 < G["total"] < 20.0),
    # P = 复读退化：上游刷 12 份相同周期，熔断应在 ~8 份处切断（不陪跑到底），
    # 且切断=干净关传输，不伪造 [DONE]。
    ("P 透传路复读熔断 -> 提前切断（5~9份，非12份）",
     P["status"] == 200 and P["text"].startswith("开头正常内容。")
     and 5 <= P["text"].count(_unit) <= 9 and P["done"] is False
     and P["total"] < 5.0),
]
print()
for name, ok in checks:
    print(("  PASS  " if ok else "  FAIL  ") + name)
print("\n总结:", "全部通过" if all(o for _, o in checks) else "有未通过项")
