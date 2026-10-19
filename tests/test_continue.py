# -*- coding: utf-8 -*-
"""自动续写回归（/v1/responses 路，DSH 宿主走的就是这条）。

剧本（假上游按代理转发的 chat/completions 请求区分轮次）：
  H 吐了内容后永久静默     -> 第 2 次请求带 assistant 前缀；续写成功 -> completed
  K 续写内容开头与旧尾重复 -> 接缝去重，正文不得出现两遍
  I 每轮都卡死             -> 续写 2 次耗尽 -> incomplete，三段正文都在
  J 工具调用拼一半卡死     -> 绝不自动续写（hits==1），incomplete
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

FAKE = 9926
PROXY = 8826
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
st = {"hits": 0, "script": "H", "bodies": []}


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
        blob = b"data: " + json.dumps(obj, ensure_ascii=False).encode("utf-8") + b"\n\n"
        self.wfile.write(b"%X\r\n%s\r\n" % (len(blob), blob))
        self.wfile.flush()

    def _delta(self, c):
        return {"choices": [{"index": 0, "delta": {"content": c},
                             "finish_reason": None}]}

    def _finish(self):
        return {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}

    def _end(self):
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def do_POST(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n)
        st["hits"] += 1
        st["bodies"].append(json.loads(body.decode("utf-8")))
        sc, hit = st["script"], st["hits"]
        try:
            if sc == "H":
                self._hdr()
                if hit == 1:
                    self._send({"choices": [{"index": 0,
                                             "delta": {"reasoning_content": "先想想"}}]})
                    self._send(self._delta("内容A"))
                    self._send(self._delta("内容B"))
                    time.sleep(100)          # 吐字后挂死
                else:
                    self._send(self._delta("续上后半"))
                    self._finish_ok = self._send(self._finish())
                    self._end()
                return
            if sc == "K":
                self._hdr()
                if hit == 1:
                    self._send(self._delta("统一支付"))
                    self._send(self._delta("平台，方案"))
                    time.sleep(100)
                else:
                    self._send(self._delta("平台，方案"))   # 与旧尾重复！
                    self._send(self._delta("如下。"))
                    self._send(self._finish())
                    self._end()
                return

            if sc == "I":
                self._hdr()
                self._send(self._delta("片%d" % hit))
                time.sleep(100)              # 每一轮都挂死
                return

            if sc == "J":
                self._hdr()
                if hit == 1:
                    self._send({"choices": [{"index": 0, "delta": {"tool_calls": [
                        {"index": 0, "id": "call_1",
                         "function": {"name": "f", "arguments": "{\"a\":"}}]}}]})
                    time.sleep(100)          # 工具参数拼一半挂死
                else:
                    self._send(self._delta("不该出现的续写"))
                    self._send(self._finish())
                    self._end()
                return

            if sc == "L":
                # 上游 fin=length（写满预算）：代理应压掉 incomplete、自动续写
                self._hdr()
                if hit == 1:
                    self._send(self._delta("写到一半"))
                    self._send({"choices": [{"index": 0, "delta": {},
                                             "finish_reason": "length"}]})
                    self.wfile.write(b"data: [DONE]\n\n")
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                else:
                    self._send(self._delta("然后写完啦"))
                    self._send(self._finish())
                    self._end()
                return

            if sc == "M":
                # fin=length 但一个正文都没有：没东西可续，必须如实透传 incomplete
                self._hdr()
                self._send({"choices": [{"index": 0,
                                         "delta": {"reasoning_content": "只想不写"},
                                         "finish_reason": "length"}]})
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return

            if sc == "N":
                # 每一轮（含续写轮）都 fin=length：续写耗尽后如实收尾
                self._hdr()
                self._send(self._delta("段%d" % hit))
                self._send({"choices": [{"index": 0, "delta": {},
                                         "finish_reason": "length"}]})
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
env.update({"ATOMCODE_FIRST_EVENT": "3", "ATOMCODE_KEEPALIVE": "1",
            "ATOMCODE_STALL": "4", "ATOMCODE_STALL_AGAIN": "2",
            "ATOMCODE_CONTINUES": "2", "ATOMCODE_CONTINUE_WAIT": "15",
            "ATOMCODE_TOKEN_FLOOR": "4096"})
logf = open(os.path.join(HERE, "_cont.log"), "w", encoding="utf-8")
proxy = subprocess.Popen(
    [sys.executable, "-u", os.path.join(HERE, "atomcode2api.py"),
     "--port", str(PROXY), "--upstream", "http://127.0.0.1:%d/v1" % FAKE,
     "--no-persist"],
    env=env, cwd=HERE, stdout=logf, stderr=subprocess.STDOUT)
time.sleep(4)


def call_responses():
    """走宿主那条路：POST /v1/responses（流式），拼出客户端可见结果。"""
    body = json.dumps({"model": "fake-model", "input": "随便写点东西",
                       "stream": True}).encode()
    req = urllib.request.Request("http://127.0.0.1:%d/v1/responses" % PROXY,
                                 data=body,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer sk"},
                                 method="POST")
    t0 = time.time()
    raw = b""
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = b"".join(r.readlines())
    except urllib.error.HTTPError as e:
        return {"status": e.code, "text": "", "completed": False,
                "incomplete": False, "secs": round(time.time() - t0, 1),
                "err": e.read()[:60].decode("utf-8", "replace")}
    except Exception as e:
        return {"status": "ERR", "text": "", "completed": False,
                "incomplete": False, "secs": round(time.time() - t0, 1),
                "err": str(e)[:60]}
    text, reasons = "", []
    inc_reason = None
    for ln in raw.decode("utf-8", "replace").splitlines():
        t = ln.strip()
        if not t.startswith("data:") or t[5:].strip() in ("", "[DONE]"):
            continue
        try:
            ev = json.loads(t[5:].strip())
        except Exception:
            continue
        ty = ev.get("type")
        if ty == "response.output_text.delta":
            text += ev.get("delta") or ""
        if ty == "response.incomplete":
            inc_reason = ((ev.get("response") or {}).get("incomplete_details") or {}).get("reason")
        reasons.append(ty)
    return {"status": 200, "text": text,
            "completed": "response.completed" in reasons,
            "incomplete": "response.incomplete" in reasons,
            "inc_reason": inc_reason,
            "reasons": sorted(set(reasons)),
            "secs": round(time.time() - t0, 1), "err": None}


results = {}
for case in ("H", "K", "I", "J", "L", "M", "N"):
    st["script"] = case
    st["hits"] = 0
    st["bodies"] = []
    t0 = time.time()
    r = call_responses()
    r["hits"] = st["hits"]
    r["bodies_n"] = len(st["bodies"])
    if st["bodies"]:
        last = st["bodies"][-1]["messages"]
        r["cont_msgs"] = [(m["role"], (m.get("content") or "")[:24])
                          for m in last[-2:]]
        # 续写请求里的 assistant 前缀消息带没带 reasoning_content
        r["cont_reasoning"] = [m.get("reasoning_content")
                               for m in last if m["role"] == "assistant"]
    results[case] = r
    print("  %s: %s" % (case, {k: v for k, v in r.items() if k != "reasons"}))
    print("     事件类型:", r.get("reasons"))

logf.flush()
log = open(os.path.join(HERE, "_cont.log"), encoding="utf-8", errors="replace").read()
proxy.terminate()
try:
    proxy.wait(5)
except Exception:
    proxy.kill()

print("\n代理日志（续写相关行）:")
for l in log.splitlines():
    if "[cont" in l:
        print("   " + l)

H, K, I, J, L, M, N = (results[c] for c in "HKIJLMN")
checks = [
    ("H 挂死后自动续写成功 -> completed",
     H["completed"] and H["text"] == "内容A内容B续上后半"
     and H["hits"] == 2 and not H["incomplete"]),
    ("H 续写请求确实带了 assistant 前缀",
     any(r == "assistant" and c == "内容A内容B" for r, c in H.get("cont_msgs", []))),
    ("H 续写请求带回了已输出的思考（reasoning_content）",
     H.get("cont_reasoning") and H["cont_reasoning"][-1] == "先想想"),
    ("K 接缝重复被裁剪（正文只有一遍）",
     K["completed"] and K["text"] == "统一支付平台，方案如下。"
     and K["text"].count("平台，方案") == 1),
    ("I 每轮都死 -> 续写耗尽 -> incomplete 且三段都在",
     I["incomplete"] and not I["completed"] and I["text"] == "片1片2片3"
     and I["hits"] == 3),
    ("J 工具拼一半 -> 绝不自动续写",
     J["incomplete"] and J["hits"] == 1 and J["text"] == ""),
    ("L fin=length 有正文 -> 压掉终止事件、自动续写成 completed",
     L["completed"] and not L["incomplete"] and L["text"] == "写到一半然后写完啦"
     and L["hits"] == 2),
    ("L 续写请求带 assistant 前缀（写到一半）",
     any(r == "assistant" and c == "写到一半" for r, c in L.get("cont_msgs", []))),
    ("M fin=length 无正文 -> 如实透传 incomplete，不浪费续写",
     M["incomplete"] and not M["completed"] and M["hits"] == 1
     and M.get("inc_reason") == "max_output_tokens"),
    ("N 每轮都写满 -> 续写耗尽 -> incomplete(max_output_tokens) 且三段都在",
     N["incomplete"] and not N["completed"] and N["text"] == "段1段2段3"
     and N["hits"] == 3 and N.get("inc_reason") == "max_output_tokens"),
    ("心跳零宽字符没漏进正文",
     all("​" not in r["text"] for r in (H, K, I, J, L, M))),
]
print()
for name, ok in checks:
    print(("  PASS  " if ok else "  FAIL  ") + name)
print("\n总结:", "全部通过" if all(o for _, o in checks) else "有未通过项")
