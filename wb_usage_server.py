#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorkBuddy Token 监控看板（单文件版）
=====================================
启动后自动监控本机 WorkBuddy 所有任务的 token / 积分消耗，浏览器里实时看折线图。

用法:
    python wb_usage_server.py                # 启动并自动打开浏览器
    python wb_usage_server.py --port 9000    # 指定端口
    python wb_usage_server.py --no-open      # 不自动开浏览器
    python wb_usage_server.py --home D:/xx/.workbuddy   # 指定 WorkBuddy 数据目录

数据来源（全部只读，不联网）:
    <home>/projects/<workspace>/<sessionId>.jsonl  每次模型调用的 token / credit / 模型 / 时间
    <home>/workbuddy.db                            任务标题、项目路径、每轮积分（可选）

依赖: Python 3.8+ 标准库，无需安装任何第三方包。
"""

import argparse
import glob
import json
import os
import re
import socket
import sqlite3
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RID_RE = re.compile(r"codebuddy\.conversation_request_id=([0-9a-zA-Z\-]+)")
SR_RE = re.compile(r"<system-reminder[\s\S]*?</system-reminder>", re.I)


# ----------------------------------------------------------------------------
# 数据抓取
# ----------------------------------------------------------------------------

class FileState(object):
    __slots__ = ("offset", "buf", "sid")

    def __init__(self, sid):
        self.offset = 0
        self.buf = b""
        self.sid = sid


class Store(object):
    def __init__(self, home):
        self.home = home
        self.projects = os.path.join(home, "projects")
        self.db_path = os.path.join(home, "workbuddy.db")
        self.lock = threading.RLock()
        self.files = {}          # path -> FileState
        self.sessions = {}       # sid -> dict
        self.version = 1
        self.ready = False
        self.scan_total = 0
        self.scan_done = 0
        self.scan_bytes = 0
        self.last_change = 0.0
        self.db_sessions = {}
        self.db_usage = {}

    # ---------------- 会话结构 ----------------

    def _session(self, sid, path=None):
        s = self.sessions.get(sid)
        if s is None:
            s = {
                "id": sid,
                "path": path,
                "title": "",
                "cwd": "",
                "model": "",
                "models": {},
                "turns": {},
                "order": [],
                "steps": 0,
                "tin": 0,
                "tout": 0,
                "tcache": 0,
                "credit": 0.0,
                "firstTs": None,
                "lastTs": None,
                "ver": 0,
                "seen": set(),
            }
            self.sessions[sid] = s
        if path and not s.get("path"):
            s["path"] = path
        return s

    def _reset(self, s):
        s["turns"] = {}
        s["order"] = []
        s["steps"] = 0
        s["tin"] = s["tout"] = s["tcache"] = 0
        s["credit"] = 0.0
        s["firstTs"] = s["lastTs"] = None
        s["seen"] = set()

    # ---------------- 文件增量读取 ----------------

    def ingest(self, path, sid=None):
        try:
            st = os.stat(path)
        except OSError:
            return False
        if sid is None:
            sid = os.path.basename(path)
            if sid.endswith(".jsonl"):
                sid = sid[:-6]
        fs = self.files.get(path)
        s = self._session(sid, path)
        if fs is None:
            fs = self.files[path] = FileState(sid)
        if st.st_size < fs.offset:
            fs.offset = 0
            fs.buf = b""
            self._reset(s)
        if st.st_size == fs.offset:
            return False

        with open(path, "rb") as f:
            f.seek(fs.offset)
            chunk = f.read()
        fs.offset += len(chunk)
        self.scan_bytes += len(chunk)

        data = fs.buf + chunk
        parts = data.split(b"\n")
        fs.buf = parts.pop()
        changed = False
        for raw in parts:
            if not raw.strip():
                continue
            if self._line(raw, s):
                changed = True
        if changed:
            s["ver"] += 1
            self.version += 1
            self.last_change = time.time()
        return changed

    def _line(self, raw, s):
        try:
            line = raw.decode("utf-8", "replace")
        except Exception:
            return False
        # 快速预筛：绝大多数行是工具结果/推理过程，先做子串判断再 json 解析
        has_usage = '"rawUsage"' in line
        if not has_usage and '"role":"user"' not in line and '"ai-title"' not in line:
            if s["cwd"] and not s["title"]:
                pass
            if '"cwd"' not in line:
                return False
        try:
            o = json.loads(line)
        except Exception:
            return False

        if not s["cwd"] and isinstance(o.get("cwd"), str):
            s["cwd"] = o["cwd"]

        t = o.get("type")
        if t == "ai-title" and o.get("aiTitle"):
            s["title"] = str(o["aiTitle"])[:120]
            return True

        if t == "message" and o.get("role") == "user":
            txt, synthetic = self._user_text(o)
            rid = self._rid(o)
            ts = o.get("timestamp")
            if rid:
                turn = s["turns"].get(rid)
                if turn is None:
                    turn = {
                        "rid": rid,
                        "text": txt,
                        "synthetic": synthetic,
                        "ts": ts,
                        "endTs": ts,
                        "steps": [],
                        "tools": 0,
                        "cin": 0,
                        "cout": 0,
                        "ccache": 0,
                        "credit": 0.0,
                    }
                    s["turns"][rid] = turn
                    s["order"].append(rid)
                elif not turn["text"] and txt:
                    turn["text"] = txt
                    turn["synthetic"] = synthetic
            return True

        pd = o.get("providerData")
        if not isinstance(pd, dict):
            return False
        ru = pd.get("rawUsage")
        if not isinstance(ru, dict) or o.get("role") == "user":
            return False

        key = (pd.get("messageId") or "", pd.get("stepSeq"))
        if key[0] and key in s["seen"]:
            return False
        if key[0]:
            s["seen"].add(key)

        rid = pd.get("conversationRequestId") or ""
        ts = o.get("timestamp") or int(time.time() * 1000)
        pin = self._num(ru.get("prompt_tokens"))
        pout = self._num(ru.get("completion_tokens"))
        cache = self._num(ru.get("prompt_cache_hit_tokens"))
        if not cache:
            det = ru.get("prompt_tokens_details") or {}
            cache = self._num(det.get("cached_tokens"))
        credit = self._num(ru.get("credit"))
        model = pd.get("requestModelName") or pd.get("model") or ""
        tool = o.get("name") if t == "function_call" else ""
        desc = pd.get("argumentsDisplayText") or ""
        if not desc:
            content = o.get("content")
            if isinstance(content, list):
                for b in content:
                    if isinstance(b, dict) and b.get("type") in ("text", "output_text"):
                        desc = b.get("text") or ""
                        break
        if not desc and isinstance(o.get("arguments"), str):
            desc = o.get("arguments")
        desc = re.sub(r"\s+", " ", str(desc)).strip()[:180]

        turn = s["turns"].get(rid)
        if turn is None:
            turn = {
                "rid": rid,
                "text": "",
                "synthetic": False,
                "ts": ts,
                "endTs": ts,
                "steps": [],
                "tools": 0,
                "cin": 0,
                "cout": 0,
                "ccache": 0,
                "credit": 0.0,
            }
            s["turns"][rid] = turn
            s["order"].append(rid)

        turn["steps"].append(
            {
                "t": int(ts),
                "i": int(pin),
                "o": int(pout),
                "c": int(cache),
                "cr": round(credit, 4),
                "m": model,
                "tool": tool or "",
                "d": desc,
            }
        )
        turn["cin"] += pin
        turn["cout"] += pout
        turn["ccache"] += cache
        turn["credit"] += credit
        turn["endTs"] = max(turn["endTs"] or 0, int(ts))
        if tool:
            turn["tools"] += 1

        s["steps"] += 1
        s["tin"] += pin
        s["tout"] += pout
        s["tcache"] += cache
        s["credit"] += credit
        if model:
            s["models"][model] = s["models"].get(model, 0) + 1
            s["model"] = model
        s["firstTs"] = int(ts) if s["firstTs"] is None else min(s["firstTs"], int(ts))
        s["lastTs"] = int(ts) if s["lastTs"] is None else max(s["lastTs"], int(ts))
        return True

    @staticmethod
    def _num(v):
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _rid(o):
        meta = o.get("_meta") or {}
        bag = meta.get("baggage") or ""
        m = RID_RE.search(bag)
        if m:
            return m.group(1)
        pd = o.get("providerData") or {}
        return pd.get("conversationRequestId") or ""

    @staticmethod
    def _user_text(o):
        blocks = o.get("content")
        parts = []
        if isinstance(blocks, list):
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                ty = b.get("type")
                if ty in ("text", "input_text"):
                    parts.append(b.get("text") or b.get("input_text") or "")
                elif ty in ("resource_link", "resource"):
                    parts.append("[%s]" % (b.get("name") or "附件"))
                elif ty in ("image", "image_blob_ref", "input_image"):
                    parts.append("[图片]")
        txt = "\n".join(parts)
        txt = re.sub(r"</?user_query[^>]*>", "", txt)
        txt = re.sub(r"<image_local_path>[\s\S]*?</image_local_path>", "", txt)
        synthetic = False
        if "<compact-request" in txt or "<conversation_history_summary" in txt or "Please summarize the conversation" in txt:
            synthetic = True
        txt = SR_RE.sub("", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        if synthetic or not txt:
            txt = txt[:120] if txt else "（系统压缩 / 自动续跑）"
        return txt[:400], synthetic

    # ---------------- 数据库补充（可选） ----------------

    def load_db(self):
        if not os.path.exists(self.db_path):
            return
        for mode in ("ro", "immutable"):
            try:
                uri = "file:%s?mode=%s" % (self.db_path.replace("\\", "/"), mode)
                if mode == "immutable":
                    uri += "&immutable=1"
                con = sqlite3.connect(uri, uri=True, timeout=1.5)
                con.row_factory = sqlite3.Row
                rows = con.execute(
                    "select id,title,custom_title,cwd,status,model,created_at,updated_at,deleted_at,is_playground from sessions"
                ).fetchall()
                db = {}
                for r in rows:
                    db[r["id"]] = dict(r)
                usage = {}
                try:
                    for r in con.execute("select session_id, used, size, credit_json from session_usage"):
                        try:
                            cj = json.loads(r["credit_json"] or "{}")
                        except Exception:
                            cj = {}
                        usage[r["session_id"]] = {
                            "used": r["used"],
                            "size": r["size"],
                            "credit": cj if isinstance(cj, dict) else {},
                        }
                except Exception:
                    pass
                con.close()
                self.db_sessions = db
                self.db_usage = usage
                return
            except Exception:
                continue

    def _apply_db(self):
        for sid, s in self.sessions.items():
            row = self.db_sessions.get(sid)
            if row:
                t = row.get("custom_title") or row.get("title")
                if t:
                    s["title"] = str(t)
                if not s["cwd"] and row.get("cwd"):
                    s["cwd"] = row["cwd"]
                if row.get("model"):
                    s["model"] = row["model"]
                s["status"] = row.get("status") or ""
                s["createdAt"] = row.get("created_at")
                s["deleted"] = bool(row.get("deleted_at"))
                s["playground"] = bool(row.get("is_playground"))
            u = self.db_usage.get(sid)
            if u:
                s["ctxUsed"] = u.get("used")
                s["ctxSize"] = u.get("size")
                cmap = u.get("credit") or {}
                for rid, val in cmap.items():
                    turn = s["turns"].get(rid)
                    try:
                        val = float(val)
                    except (TypeError, ValueError):
                        continue
                    if turn is None:
                        continue
                    if val > turn["credit"] + 1e-6:
                        s["credit"] += val - turn["credit"]
                        turn["credit"] = val
                        turn["creditDb"] = True

    # ---------------- 对外快照 ----------------

    def session_summary(self, s):
        total = s["tin"] + s["tout"]
        hit = (s["tcache"] / s["tin"]) if s["tin"] else 0.0
        return {
            "id": s["id"],
            "title": s["title"] or "（未命名任务）",
            "cwd": s["cwd"],
            "project": os.path.basename((s["cwd"] or "").replace("\\", "/").rstrip("/")) or "",
            "model": s["model"],
            "models": s["models"],
            "status": s.get("status") or "",
            "createdAt": s.get("createdAt"),
            "firstTs": s["firstTs"],
            "lastTs": s["lastTs"],
            "turns": len(s["order"]),
            "steps": s["steps"],
            "tin": int(s["tin"]),
            "tout": int(s["tout"]),
            "tcache": int(s["tcache"]),
            "total": int(total),
            "credit": round(s["credit"], 2),
            "hit": round(hit, 4),
            "ctxUsed": s.get("ctxUsed"),
            "ctxSize": s.get("ctxSize"),
            "ver": s["ver"],
        }

    def detail(self, sid):
        s = self.sessions.get(sid)
        if not s:
            return None
        turns = []
        for rid in s["order"]:
            t = s["turns"][rid]
            steps = sorted(t["steps"], key=lambda x: x["t"])
            if not steps and not t["text"]:
                continue
            turns.append(
                {
                    "rid": rid,
                    "text": t["text"] or "（无文本输入）",
                    "synthetic": bool(t.get("synthetic")),
                    "ts": t["ts"],
                    "endTs": t["endTs"],
                    "steps": steps,
                    "tools": t["tools"],
                    "tin": int(t["cin"]),
                    "tout": int(t["cout"]),
                    "tcache": int(t["ccache"]),
                    "credit": round(t["credit"], 2),
                }
            )
        return {"ver": s["ver"], "turns": turns, "summary": self.session_summary(s)}

    def snapshot(self, sid, dv=None):
        with self.lock:
            self._apply_db()
            items = [self.session_summary(s) for s in self.sessions.values() if s["steps"]]
            items.sort(key=lambda x: (x["lastTs"] or 0), reverse=True)
            if not sid or sid not in self.sessions:
                sid = items[0]["id"] if items else None
            det = None
            if sid:
                cur = self.sessions.get(sid)
                if dv is None or not cur or str(dv) != str(cur["ver"]):
                    det = self.detail(sid)
            return {
                "version": self.version,
                "ready": self.ready,
                "scan": {
                    "total": self.scan_total,
                    "done": self.scan_done,
                    "mb": round(self.scan_bytes / 1048576.0, 1),
                    "lastChange": self.last_change,
                },
                "now": int(time.time() * 1000),
                "home": self.home,
                "sessions": items,
                "selected": sid,
                "detail": det,
            }


# ----------------------------------------------------------------------------
# 后台监控线程
# ----------------------------------------------------------------------------

class Watcher(threading.Thread):
    def __init__(self, store, days=0, interval=1.5):
        threading.Thread.__init__(self)
        self.daemon = True
        self.store = store
        self.days = days
        self.interval = interval
        self.stop_flag = False

    def _files(self):
        pats = [os.path.join(self.store.projects, "*", "*.jsonl")]
        out = []
        cutoff = time.time() - self.days * 86400 if self.days else 0
        for pat in pats:
            for p in glob.glob(pat):
                if p.endswith(".file-rollback.ndjson"):
                    continue
                try:
                    if cutoff and os.stat(p).st_mtime < cutoff:
                        continue
                except OSError:
                    continue
                out.append(p)
        return out

    def run(self):
        st = self.store
        st.load_db()
        files = self._files()
        st.scan_total = len(files)
        for i, p in enumerate(files):
            with st.lock:
                st.ingest(p)
            st.scan_done = i + 1
        with st.lock:
            st.ready = True
            st.version += 1
        # 增量轮询
        while not self.stop_flag:
            time.sleep(self.interval)
            with st.lock:
                for p in self._files():
                    try:
                        st_size = os.stat(p).st_size
                    except OSError:
                        continue
                    fs = st.files.get(p)
                    if fs is None or st_size != fs.offset:
                        st.ingest(p)
                if int(time.time()) % 60 == 0:
                    st.load_db()


# ----------------------------------------------------------------------------
# HTTP 服务
# ----------------------------------------------------------------------------

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>WorkBuddy Token 监控</title>
<style>
:root{
  --bg:#f4f6f9; --panel:#ffffff; --line:#e6e9ef; --line2:#eef1f5;
  --fg:#12161c; --fg2:#3d4653; --muted:#7b8494;
  --accent:#3b6ef6; --accent-soft:#e8eefe;
  --green:#10b981; --amber:#f59e0b; --violet:#8b5cf6; --red:#ef4444;
  --radius:14px;
}
*{box-sizing:border-box}
html,body{height:100%}
body{
  margin:0; background:var(--bg); color:var(--fg);
  font:13px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",system-ui,sans-serif;
  -webkit-font-smoothing:antialiased;
}
button{font-family:inherit;cursor:pointer}
::-webkit-scrollbar{width:9px;height:9px}
::-webkit-scrollbar-thumb{background:#d5dae3;border-radius:6px}
::-webkit-scrollbar-thumb:hover{background:#c2c9d6}
::-webkit-scrollbar-track{background:transparent}

/* ---------- 顶栏 ---------- */
header{
  display:flex; align-items:center; gap:16px;
  padding:12px 20px; background:rgba(255,255,255,.86);
  border-bottom:1px solid var(--line); position:sticky; top:0; z-index:20;
  backdrop-filter:saturate(180%) blur(12px);
}
.brand{display:flex;align-items:center;gap:10px;font-weight:650;font-size:14px;letter-spacing:.2px}
.logo{
  width:26px;height:26px;border-radius:8px;flex:0 0 auto;
  background:linear-gradient(135deg,#3b6ef6,#8b5cf6);
  display:grid;place-items:center;color:#fff;font-size:13px;font-weight:700;
}
.status{display:flex;align-items:center;gap:8px;margin-left:auto;color:var(--muted);font-size:12px}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 0 3px rgba(16,185,129,.16)}
.dot.pulse{animation:pulse 1.6s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.badge{
  background:var(--accent-soft); color:#2c56cc; border-radius:999px;
  padding:2px 9px; font-size:11.5px; font-weight:600;
}
.sep{width:1px;height:14px;background:var(--line)}
.btn{
  border:1px solid var(--line); background:#fff; color:var(--fg2);
  border-radius:9px; padding:5px 11px; font-size:12px; transition:.15s;
}
.btn:hover{border-color:#c9d2e2; background:#fafbfd}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}

/* ---------- 布局 ---------- */
main{
  display:grid; grid-template-columns:312px minmax(0,1fr);
  column-gap:16px; padding:16px 20px 28px; align-items:start;
}
@media (max-width:1080px){
  main{grid-template-columns:1fr}
}

.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:0 1px 2px rgba(16,24,40,.04)}

/* ---------- 任务列表 ---------- */
.side{position:sticky;top:64px;display:flex;flex-direction:column;max-height:calc(100vh - 84px);overflow:hidden;min-width:0}
.side-head{padding:12px 12px 10px;border-bottom:1px solid var(--line2);display:flex;flex-direction:column;gap:8px}
.search{position:relative}
.search input{
  width:100%; padding:7px 10px 7px 29px; border:1px solid var(--line); border-radius:9px;
  font-size:12.5px; outline:none; background:#fbfcfe; color:var(--fg);
}
.search input:focus{border-color:#b9c8ee;background:#fff;box-shadow:0 0 0 3px rgba(59,110,246,.1)}
.search svg{position:absolute;left:9px;top:50%;transform:translateY(-50%);opacity:.42}
.tools{display:flex;gap:6px;align-items:center}
.select{
  flex:1; padding:5px 8px; border:1px solid var(--line); border-radius:8px;
  font-size:12px; background:#fbfcfe; color:var(--fg2); outline:none; min-width:0;
}
.list{overflow:auto;padding:6px;flex:1}
.item{
  padding:9px 10px; border-radius:10px; cursor:pointer; border:1px solid transparent;
  transition:.12s; margin-bottom:2px;
}
.item:hover{background:#f7f9fc}
.item.active{background:#fff; border-color:#c9d8fb; box-shadow:0 1px 3px rgba(59,110,246,.12)}
.item-top{display:flex;align-items:center;gap:6px}
.item-title{font-size:12.8px;font-weight:600;color:var(--fg);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.item-sub{display:flex;align-items:center;gap:6px;color:var(--muted);font-size:11.5px;margin-top:3px}
.item-sub .proj{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:130px}
.item-nums{display:flex;gap:10px;margin-top:5px;font-size:11px;color:var(--fg2);font-variant-numeric:tabular-nums}
.item-nums b{font-weight:650;color:var(--fg)}
.live{width:6px;height:6px;border-radius:50%;background:var(--green);flex:0 0 auto}
.live.idle{background:#cfd6e2}
.spark{height:3px;border-radius:2px;background:var(--line2);margin-top:6px;overflow:hidden}
.spark i{display:block;height:100%;background:linear-gradient(90deg,#3b6ef6,#8b5cf6);border-radius:2px}

/* ---------- 右侧 ---------- */
.panel{display:flex;flex-direction:column;gap:16px;min-width:0}
.hero{padding:16px 18px}
.hero-top{display:flex;align-items:flex-start;gap:12px;flex-wrap:wrap}
.hero h1{font-size:16.5px;margin:0;font-weight:680;letter-spacing:.1px}
.hero .path{color:var(--muted);font-size:11.5px;margin-top:3px;word-break:break-all}
.chips{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.chip{
  font-size:11px;padding:2px 8px;border-radius:999px;background:#f3f5f9;
  color:var(--fg2);border:1px solid var(--line2);white-space:nowrap;
}
.chip.m{border-color:#dbe6fd;background:#f2f6ff;color:#2f5fd0;font-family:ui-monospace,Menlo,Consolas,monospace}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:10px;margin-top:14px}
.kpi{padding:9px 11px;border-radius:11px;background:#fafbfd;border:1px solid var(--line2)}
.kpi .k{font-size:11px;color:var(--muted)}
.kpi .v{font-size:16px;font-weight:680;font-variant-numeric:tabular-nums;margin-top:2px;letter-spacing:-.2px}
.kpi .v small{font-size:11px;color:var(--muted);font-weight:500;margin-left:2px}

/* ---------- 图表 ---------- */
.chart-card{display:flex;flex-direction:column;padding:14px 16px 14px}
.chart-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.tabs{display:flex;gap:4px;background:#f3f5f9;padding:3px;border-radius:10px}
.tab{border:0;background:transparent;padding:5px 11px;border-radius:8px;font-size:12px;color:var(--fg2);transition:.15s;white-space:nowrap}
.tab:hover{color:var(--fg)}
.tab.on{background:#fff;color:var(--fg);font-weight:640;box-shadow:0 1px 2px rgba(16,24,40,.08)}
.legend{display:flex;gap:12px;margin-left:auto;font-size:11.5px;color:var(--fg2);flex-wrap:wrap}
.legend i{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:5px;vertical-align:1px}
.chart-wrap{position:relative;margin-top:6px;height:326px;min-height:120px}
#chart{width:100%;height:100%;display:block}
.tip{
  position:absolute;pointer-events:none;background:rgba(17,24,39,.94);color:#f8fafc;
  border-radius:9px;padding:7px 9px;font-size:11.5px;line-height:1.55;white-space:nowrap;
  transform:translate(-50%,-112%);opacity:0;transition:opacity .1s;z-index:9;
  box-shadow:0 8px 22px rgba(0,0,0,.22);font-variant-numeric:tabular-nums;
}
.tip b{color:#fff}
.tip .t{color:#9db0d8;margin-bottom:2px;display:block}
.tip .row{display:flex;gap:10px;justify-content:space-between}
.empty{padding:40px 20px;text-align:center;color:var(--muted);font-size:12.5px}
.empty b{display:block;color:var(--fg2);font-size:13.5px;margin-bottom:4px}

/* ---------- 工具 × 积分 ---------- */
.viz-card{display:flex;flex-direction:column;padding:12px 16px 12px;position:relative}
.viz-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.viz-head h2{font-size:13px;margin:0;font-weight:650}
.viz-head .hint{color:var(--muted);font-size:11.5px}
.viz-body{
  display:grid;grid-template-columns:minmax(300px,.9fr) minmax(240px,1fr);
  gap:20px;height:212px;min-height:120px;margin-top:8px;overflow:hidden;
}
@media (max-width:1300px){
  .viz-body{grid-template-columns:1fr;grid-template-rows:1fr 1fr;height:372px}
}
.viz-pane{display:flex;flex-direction:column;min-width:0;min-height:0;overflow:hidden}
.viz-cap{font-size:10.5px;color:var(--muted);letter-spacing:.3px;margin-bottom:5px;display:flex;gap:6px;align-items:center}
.viz-cap b{color:var(--fg2);font-weight:600}
.tstats{overflow:auto;flex:1;min-height:0;padding-right:2px;overscroll-behavior:contain}
.trow{
  display:grid;grid-template-columns:8px 86px minmax(40px,1fr) 56px 44px;
  align-items:center;gap:8px;padding:2px 4px;border-radius:7px;cursor:pointer;
}
.trow:hover{background:#f4f7fe}
.trow.on{background:#eaf1ff;box-shadow:inset 0 0 0 1px #c9dbff}
.trow.on .tname{color:#1f4fd6;font-weight:600}
.tdot{width:8px;height:8px;border-radius:2.5px}
.tname{
  font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  color:var(--fg2);font-family:ui-monospace,Menlo,Consolas,monospace;
}
.tbar{height:9px;background:#f1f4f9;border-radius:99px;overflow:hidden}
.tbar i{display:block;height:100%;border-radius:99px;transition:width .35s}
.tval{text-align:right;font-size:11px;font-variant-numeric:tabular-nums;font-weight:650;color:var(--fg)}
.tcnt{text-align:right;font-size:10px;color:var(--muted);font-variant-numeric:tabular-nums}
.stackwrap{display:flex;flex-direction:column;flex:1;min-height:0;overflow:hidden;position:relative}
.sgrid{position:absolute;left:0;right:54px;top:0;bottom:15px;pointer-events:none}
.sgrid i{position:absolute;left:0;right:0;border-top:1px dashed #e9edf4}
.sgrid i span{position:absolute;right:-50px;top:-5px;font-size:9px;color:#aab3c2;white-space:nowrap}
.stack{display:flex;align-items:flex-end;gap:2px;flex:1;min-height:0;border-bottom:1px solid var(--line);padding-bottom:1px;padding-right:54px}
.scol{flex:1 1 0;min-width:2px;height:100%;display:flex;flex-direction:column;justify-content:flex-end;border-radius:3px 3px 0 0}
.scol:hover{background:#eef3ff}
.sbar{width:100%;display:flex;flex-direction:column;justify-content:flex-end;border-radius:2px;overflow:hidden}
.sseg{width:100%;min-height:1px}
.sseg:hover{filter:brightness(1.12)}
.slab{display:flex;gap:2px;height:15px;margin-top:2px;padding-right:54px}
.slab span{flex:1 1 0;min-width:2px;text-align:center;font-size:9.5px;color:#a9b2c2;overflow:hidden}
.vtip{
  position:absolute;pointer-events:none;background:rgba(17,24,39,.94);color:#f8fafc;
  border-radius:8px;padding:6px 9px;font-size:11.5px;white-space:nowrap;opacity:0;
  transition:opacity .1s;z-index:9;box-shadow:0 8px 22px rgba(0,0,0,.22);transform:translate(-50%,-115%);
}
.vtip b{color:#fff}

/* ---------- 表格 ---------- */
.table-card{display:flex;flex-direction:column;padding:4px 0 10px;overflow:hidden}
.table-head{display:flex;align-items:center;gap:10px;padding:11px 16px;flex-wrap:wrap}
.table-head h2{font-size:13px;margin:0;font-weight:650}
.table-head .hint{color:var(--muted);font-size:11.5px}
.table-head .btn{margin-left:auto}
.table-scroll{height:340px;min-height:90px;overflow:auto;border-top:1px solid var(--line2)}
table{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}
thead th{
  position:sticky;top:0;background:#fbfcfe;text-align:right;font-weight:600;color:var(--muted);
  padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap;font-size:11.5px;z-index:2;
}
thead th.l{text-align:left}
thead th:first-child{padding-left:16px}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:var(--fg);background:#f5f8ff}
th .ar{margin-left:3px;font-size:8px;opacity:.35}
th.asc .ar::after{content:"▲"}
th.desc .ar::after{content:"▼"}
th.asc,th.desc{color:var(--accent)}
th.asc .ar,th.desc .ar{opacity:1;color:var(--accent)}
tbody td{padding:7px 10px;border-bottom:1px solid #f4f6fa;text-align:right;color:var(--fg2);white-space:nowrap}
tbody tr{cursor:pointer;transition:background .12s}
tbody tr:hover{background:#fafbff}
tbody tr.hl{background:#f2f6ff}
tbody td.txt{text-align:left;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--fg)}
tbody td.mdl{max-width:118px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tbody td.l{text-align:left}
tbody td:first-child{padding-left:16px}
.idx{
  display:inline-grid;place-items:center;min-width:20px;height:20px;padding:0 4px;border-radius:6px;
  background:#eef1f7;color:var(--fg2);font-size:10.5px;font-weight:650;
}
.idx.syn{background:#fff5e6;color:#a2620a}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px}
.ttool{
  font-family:ui-monospace,Menlo,Consolas,monospace;font-size:10.5px;
  background:#f3f6fc;border:1px solid #e1e9f7;color:#2f558c;
  padding:1px 6px;border-radius:5px;white-space:nowrap;
}
.muted{color:var(--muted)}
.foot{color:var(--muted);font-size:11px;padding:0;display:flex;gap:12px;flex-wrap:wrap}
.foot code{background:#eceff5;padding:1px 5px;border-radius:4px;font-size:10.5px}
.scanbar{position:fixed;top:0;left:0;height:2px;background:var(--accent);transition:width .3s;z-index:99}
</style>
</head>
<body>
<div class="scanbar" id="scanbar" style="width:0"></div>

<header>
  <div class="brand"><span class="logo">W</span>WorkBuddy Token 监控</div>
  <span class="badge" id="badge">连接中…</span>
  <div class="status">
    <span class="dot pulse" id="live-dot"></span>
    <span id="live-text">正在扫描本机会话…</span>
    <span class="sep"></span>
    <span id="upd">—</span>
    <button class="btn" id="toggle">暂停</button>
  </div>
</header>

<main id="main">
  <aside class="card side">
    <div class="side-head">
      <div class="search">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>
        <input id="q" placeholder="搜索任务标题 / 项目" />
      </div>
      <div class="tools">
        <select class="select" id="sort">
          <option value="recent">按最近活跃</option>
          <option value="total">按 Token 总量</option>
          <option value="credit">按积分</option>
          <option value="turns">按对话轮次</option>
        </select>
      </div>
    </div>
    <div class="list" id="list"></div>
  </aside>


  <section class="panel">
    <div class="card hero" id="hero"></div>

    <div class="card chart-card">
      <div class="chart-head">
        <div class="tabs" id="tabs">
          <button class="tab on" data-m="cum">累计 Token</button>
          <button class="tab" data-m="step">单步 Token</button>
          <button class="tab" data-m="credit">累计积分</button>
          <button class="tab" data-m="hit">缓存命中率</button>
        </div>
        <div class="legend" id="legend"></div>
      </div>
      <div class="chart-wrap" id="chart-wrap">
        <svg id="chart" preserveAspectRatio="none"></svg>
        <div class="tip" id="tip"></div>
      </div>
        </div>

    <div class="card viz-card" id="viz-card">
      <div class="viz-head">
        <h2>工具 / 动作 × 消耗</h2>
        <div class="tabs" id="viztabs">
          <button class="tab on" data-vm="credit">积分</button>
          <button class="tab" data-vm="tokens">Token</button>
          <button class="tab" data-vm="count">次数</button>
        </div>
        <span class="hint" id="vizhint"></span>
      </div>
      <div class="viz-body">
        <div class="viz-pane">
          <div class="viz-cap">按工具聚合 <b>（点击只看该工具）</b></div>
          <div class="tstats" id="tstats"></div>
        </div>
        <div class="viz-pane">
          <div class="viz-cap">按轮次堆叠 <b id="stackcap"></b></div>
          <div class="stackwrap" id="stackwrap">
            <div class="sgrid" id="sgrid"></div>
            <div class="stack" id="stack"></div>
            <div class="slab" id="slab"></div>
          </div>
        </div>
      </div>
          <div class="vtip" id="vtip"></div>
    </div>

    <div class="card table-card">
      <div class="table-head">
        <h2>明细</h2>
        <div class="tabs" id="viewtabs">
          <button class="tab on" data-v="turn">逐轮</button>
          <button class="tab" data-v="step">逐步骤</button>
        </div>
        <span class="hint" id="turnhint"></span>
        <button class="btn" id="filterbtn" style="display:none"></button>
        <button class="btn" id="csv">导出 CSV</button>
      </div>
      <div class="table-scroll" id="table-scroll">
        <table id="dt">
          <thead id="thead"></thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
        </div>

    <div class="foot">
      <span>只读本机 <code>~/.workbuddy</code>，不联网</span>
      <span id="foot-stat"></span>
    </div>
  </section>
</main>

<script>
"use strict";

/* ================= 基础工具 ================= */
function $(id){ return document.getElementById(id); }
function esc(s){
  return String(s == null ? "" : s).replace(/[&<>"']/g, function(c){
    return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];
  });
}
function nf(n){ return (n == null ? 0 : n).toLocaleString("en-US"); }
function kfmt(n){
  n = n || 0;
  if (n >= 1e9) return (n/1e9).toFixed(2) + "B";
  if (n >= 1e6) return (n/1e6).toFixed(n >= 1e7 ? 1 : 2) + "M";
  if (n >= 1e3) return (n/1e3).toFixed(n >= 1e4 ? 0 : 1) + "k";
  return String(Math.round(n));
}
function pct(v){ return (v*100).toFixed(1) + "%"; }
function hhmm(ts){
  var d = new Date(ts);
  return String(d.getHours()).padStart(2,"0") + ":" + String(d.getMinutes()).padStart(2,"0");
}
function dt(ts){
  if (!ts) return "—";
  var d = new Date(ts), now = Date.now();
  var diff = (now - ts) / 1000;
  if (diff < 60) return "刚刚";
  if (diff < 3600) return Math.floor(diff/60) + " 分钟前";
  var sameDay = new Date(now).toDateString() === d.toDateString();
  if (sameDay) return hhmm(ts);
  var y = new Date(now).getFullYear() === d.getFullYear();
  return (y ? "" : (d.getFullYear() + "-")) + String(d.getMonth()+1).padStart(2,"0") + "-" + String(d.getDate()).padStart(2,"0");
}
function dur(ms){
  if (!ms || ms < 0) return "—";
  var s = Math.round(ms/1000);
  if (s < 60) return s + "s";
  var m = Math.floor(s/60);
  if (m < 60) return m + "m" + String(s%60).padStart(2,"0") + "s";
  var h = Math.floor(m/60);
  return h + "h" + String(m%60).padStart(2,"0") + "m";
}

/* ================= 状态 ================= */
var state = { sessions: [], detail: null, selected: null, scan: {}, home: "", ready: false, now: 0 };

var wantSid = null;      // 用户想看的任务
var detailSid = null;    // state.detail 属于哪个任务
var detailVer = -1;
var metric = "cum";
var view = "turn";
var vizMetric = "credit";
var sortTurn = { k: null, d: -1 };
var sortStep = { k: null, d: -1 };
var toolFilter = null;   // 在"工具 × 消耗"里点某个工具 = 只看该工具的步骤
var paused = false;
var reqSeq = 0, appliedSeq = 0, inFlight = false;
var lastKey = "";
var lastListKey = "";
var pollTimer = null;

/* ================= 拉数据 ================= */
function fetchState(force){
  if (paused && !force) return;
  if (inFlight && !force) return;
  var sid = wantSid;
  var seq = ++reqSeq;
  var q = "";
  if (sid){
    q = "?session=" + encodeURIComponent(sid);
    if (detailVer >= 0 && detailSid === sid) q += "&dv=" + detailVer;
  }
  inFlight = true;
  fetch("/api/state" + q, { cache: "no-store" })
    .then(function(r){ return r.json(); })
    .then(function(d){
      inFlight = false;
      if (seq < appliedSeq) return;        // 有更新的请求已落地，丢弃这条
      if (sid !== wantSid) return;         // 用户已切换任务，丢弃旧结果
      appliedSeq = seq;

      state.sessions = d.sessions || [];
      state.scan = d.scan || {};
      state.home = d.home || "";
      state.ready = !!d.ready;
      state.now = d.now;

      if (!wantSid && d.selected){
        wantSid = d.selected;
        state.selected = d.selected;
      }
      state.selected = wantSid;

      var det = d.detail;
      if (det && det.summary && det.summary.id === wantSid){
        state.detail = det;
        detailSid = wantSid;
        detailVer = det.ver;
      } else if (detailSid && detailSid !== wantSid){
        state.detail = null;
      }
      renderAll();
    })
    .catch(function(){
      inFlight = false;
      $("badge").textContent = "连接中断，重试中…";
      $("live-dot").classList.remove("pulse");
    });
}

function selectTask(id){
  if (!id || id === wantSid) return;
  wantSid = id;
  state.selected = id;
  state.detail = null;
  detailSid = null;
  detailVer = -1;
  sortTurn = { k: null, d: -1 };
  sortStep = { k: null, d: -1 };
  lastKey = "";
  renderAll();
  fetchState(true);
}

/* ================= 顶栏 ================= */
function renderTop(){
  var s = state.scan || {};
  var badge = $("badge");
  if (!state.ready){
    badge.textContent = "扫描中 " + (s.done||0) + "/" + (s.total||0);
    $("scanbar").style.width = (s.total ? (s.done/s.total*100) : 8) + "%";
  } else {
    badge.textContent = state.sessions.length + " 个任务";
    $("scanbar").style.width = "0%";
  }
  var busy = state.sessions.some(function(x){ return x.lastTs && (state.now - x.lastTs) < 45000; });
  var dot = $("live-dot");
  dot.className = "dot" + (busy && !paused ? " pulse" : "");
  $("live-text").textContent = paused ? "已暂停刷新" :
    (busy ? "检测到任务正在运行" : (state.ready ? "监控中" : "正在解析历史会话"));
  $("upd").textContent = new Date().toLocaleTimeString("zh-CN", {hour12:false});
  $("foot-stat").textContent = "已解析 " + (s.total||0) + " 个会话文件 · " + (s.mb||0) + " MB";
}

/* ================= 任务列表 ================= */
function renderList(){
  var q = ($("q").value || "").trim().toLowerCase();
  var sort = $("sort").value;
  var arr = state.sessions.slice();
  if (q){
    arr = arr.filter(function(x){
      return (x.title||"").toLowerCase().indexOf(q) >= 0 ||
             (x.project||"").toLowerCase().indexOf(q) >= 0 ||
             (x.cwd||"").toLowerCase().indexOf(q) >= 0;
    });
  }
  arr.sort(function(a,b){
    if (sort === "total") return b.total - a.total;
    if (sort === "credit") return b.credit - a.credit;
    if (sort === "turns") return b.turns - a.turns;
    return (b.lastTs||0) - (a.lastTs||0);
  });
  var sig = arr.map(function(x){
    return [x.id, x.total, x.credit, x.turns, x.lastTs, x.title].join(":");
  }).join("|") + "#" + state.selected + "#" + sort + "#" + q;
  if (sig === lastListKey) return;
  lastListKey = sig;

  var max = 1;
  arr.forEach(function(x){ if (x.total > max) max = x.total; });
  var html = arr.map(function(x){
    var live = x.lastTs && (state.now - x.lastTs) < 45000;
    return '<div class="item' + (x.id === state.selected ? " active" : "") + '" data-id="' + esc(x.id) + '">' +
      '<div class="item-top">' +
        '<span class="live' + (live ? "" : " idle") + '"></span>' +
        '<span class="item-title" title="' + esc(x.title) + '">' + esc(x.title) + '</span>' +
      '</div>' +
      '<div class="item-sub"><span class="proj" title="' + esc(x.cwd) + '">' + esc(x.project || x.cwd || "—") + '</span>' +
        '<span>·</span><span>' + dt(x.lastTs) + '</span></div>' +
      '<div class="item-nums"><span><b>' + kfmt(x.total) + '</b> tok</span>' +
        '<span><b>' + (x.credit||0).toFixed(2) + '</b> 分</span>' +
        '<span>' + x.turns + ' 轮</span></div>' +
      '<div class="spark"><i style="width:' + Math.max(3, Math.round(x.total/max*100)) + '%"></i></div>' +
    '</div>';
  }).join("");
  if (!html) html = '<div class="empty" style="padding:28px 12px">没有匹配的任务</div>';
  var box = $("list");
  var sc = box.scrollTop;
  box.innerHTML = html;
  box.scrollTop = sc;
  Array.prototype.forEach.call(box.querySelectorAll(".item"), function(el){
    el.addEventListener("click", function(){ selectTask(el.getAttribute("data-id")); });
  });
}

/* ================= 任务头 ================= */
function currentSession(){
  return state.sessions.filter(function(x){ return x.id === wantSid; })[0] || null;
}
function renderHero(){
  var s = currentSession();
  var head = $("hero");
  if (!s){
    head.innerHTML = '<div class="empty"><b>还没有数据</b>启动一个 WorkBuddy 任务后再回到这里</div>';
    return;
  }
  var sum = (detailSid === wantSid && state.detail && state.detail.summary) || s;
  var chips = [];
  if (sum.model) chips.push('<span class="chip m">' + esc(sum.model) + '</span>');
  if (sum.status) chips.push('<span class="chip">' + esc(sum.status) + '</span>');
  if (sum.ctxSize) chips.push('<span class="chip">上下文 ' + kfmt(sum.ctxUsed||0) + " / " + kfmt(sum.ctxSize) + '</span>');
  chips.push('<span class="chip">' + sum.steps + ' 次模型调用</span>');
  if (sum.lastTs) chips.push('<span class="chip">最近 ' + dt(sum.lastTs) + '</span>');

  head.innerHTML =
    '<div class="hero-top"><div style="min-width:0;flex:1">' +
      '<h1>' + esc(sum.title || "（未命名任务）") + '</h1>' +
      '<div class="path">' + esc(sum.cwd || "—") + '</div>' +
      '<div class="chips">' + chips.join("") + '</div>' +
    '</div></div>' +
    '<div class="kpis">' +
      kpi("总 Token", kfmt(sum.total)) +
      kpi("输入 / 输出", kfmt(sum.tin) + " / " + kfmt(sum.tout)) +
      kpi("累计积分", (sum.credit||0).toFixed(2), "分") +
      kpi("缓存命中率", sum.tin ? pct(sum.tcache / sum.tin) : "—") +
      kpi("对话轮次", sum.turns) +
      kpi("持续时长", dur((sum.lastTs||0) - (sum.firstTs||sum.lastTs||0))) +
    '</div>';
}
function kpi(k, v, unit){
  return '<div class="kpi"><div class="k">' + k + '</div><div class="v">' + v +
    (unit ? '<small>' + unit + '</small>' : '') + '</div></div>';
}

/* ================= 数据整理 ================= */
var COLORS = { low:"#3b6ef6", out:"#10b981", credit:"#f59e0b", hit:"#8b5cf6" };
var PALETTE = ["#3b6ef6","#10b981","#f59e0b","#8b5cf6","#ef4444","#06b6d4",
               "#84cc16","#ec4899","#f97316","#14b8a6","#6366f1","#a855f7"];

var flatCache = null, flatKey = "";
function getFlat(){
  var d = state.detail;
  if (!d || detailSid !== wantSid || !d.turns) return [];
  var k = (detailSid||"") + "|" + detailVer;
  if (k === flatKey && flatCache) return flatCache;
  var flat = [], seq = 0;
  d.turns.forEach(function(t, ti){
    t.steps.forEach(function(st){
      seq++;
      flat.push({
        seq: seq, t: st.t, i: st.i, o: st.o, c: st.c, cr: st.cr || 0,
        m: st.m || "", tool: st.tool || "", d: st.d || "", ti: ti, turnNo: ti + 1,
        act: st.tool || "模型输出",
        gap: 0
      });
    });
  });
  flat.sort(function(a,b){ return a.t - b.t; });
  flat.forEach(function(x, i){
    if (i > 0){
      x.gap = x.t - flat[i-1].t;
      x.seq = i + 1;
    } else {
      x.gap = 0;
    }
  });
  flatCache = flat;
  flatKey = k;
  return flat;
}

/* ================= 折线图 ================= */
var METRICS = {
  cum: {
    label: "累计 Token",
    series: [
      { key:"cumIn", name:"累计输入", color:COLORS.low, width:2, area:true },
      { key:"cumOut", name:"累计输出", color:COLORS.out, width:1.8 }
    ],
    fmt: function(v){ return kfmt(v); },
    tip: function(p){ return [["累计合计", kfmt(p.cumAll)], ["累计输入", nf(p.cumIn)], ["累计输出", nf(p.cumOut)]]; }
  },
  step: {
    label: "单步 Token",
    series: [
      { key:"i", name:"单步输入（≈上下文规模）", color:COLORS.low, width:1.7, area:true },
      { key:"o", name:"单步输出", color:COLORS.out, width:1.7 }
    ],
    fmt: function(v){ return kfmt(v); },
    tip: function(p){ return [["单步输入", nf(p.i)], ["缓存命中", nf(p.c)], ["单步输出", nf(p.o)]]; }
  },
  credit: {
    label: "累计积分",
    series: [{ key:"cumCredit", name:"累计积分", color:COLORS.credit, width:2, area:true }],
    fmt: function(v){ return v.toFixed(1); },
    tip: function(p){ return [["累计积分", p.cumCredit.toFixed(2)], ["本步积分", p.cr.toFixed(3)]]; }
  },
  hit: {
    label: "缓存命中率",
    series: [{ key:"hit", name:"本步缓存命中率", color:COLORS.hit, width:1.8, area:true }],
    fmt: function(v){ return (v*100).toFixed(0) + "%"; },
    pct: true,
    tip: function(p){ return [["本步命中率", pct(p.hit)], ["命中 token", nf(p.c)], ["输入 token", nf(p.i)]]; }
  }
};

function buildPoints(){
  var d = state.detail;
  if (!d || detailSid !== wantSid || !d.turns) return { pts: [], turns: [] };
  var pts = [], turns = [];
  d.turns.forEach(function(t, ti){
    turns.push({ ti: ti, t0: t.ts, t1: t.endTs || t.ts, text: t.text, credit: t.credit });
    t.steps.forEach(function(st){
      pts.push({ t: st.t, i: st.i, o: st.o, c: st.c, cr: st.cr || 0, m: st.m, tool: st.tool, ti: ti });
    });
  });
  pts.sort(function(a,b){ return a.t - b.t; });
  var ci = 0, co = 0, cc = 0;
  pts.forEach(function(p){
    ci += p.i; co += p.o; cc += p.cr;
    p.cumIn = ci; p.cumOut = co; p.cumAll = ci + co; p.cumCredit = cc;
    p.hit = p.i > 0 ? p.c / p.i : 0;
  });
  return { pts: pts, turns: turns };
}

function renderLegend(){
  var m = METRICS[metric];
  $("legend").innerHTML = m.series.map(function(s){
    return '<span><i style="background:' + s.color + '"></i>' + esc(s.name) + '</span>';
  }).join("");
}

function renderChart(){
  var box = $("chart-wrap");
  var svg = $("chart");
  var data = buildPoints();
  var pts = data.pts;
  var m = METRICS[metric];
  var old = box.querySelector(".empty");
  if (old) old.remove();

  if (!pts.length){
    svg.innerHTML = "";
    var e0 = document.createElement("div");
    e0.className = "empty";
    e0.innerHTML = state.detail ? "<b>暂无模型调用记录</b>这个任务可能刚开始，或产生的是纯工具调用"
                                : "<b>正在载入…</b>读取该任务的调用记录";
    box.appendChild(e0);
    return;
  }

  var W = Math.max(320, box.clientWidth || 900);
  var H = Math.max(120, box.clientHeight || 326);
  var pad = { l: 60, r: 16, t: 16, b: 26 };
  var iw = W - pad.l - pad.r, ih = H - pad.t - pad.b;

  svg.setAttribute("viewBox", "0 0 " + W + " " + H);
  svg.setAttribute("preserveAspectRatio", "none");

  var t0 = pts[0].t, t1 = pts[pts.length-1].t;
  if (t1 <= t0) t1 = t0 + 1000;
  var vmax = 0;
  m.series.forEach(function(s){
    pts.forEach(function(p){ if (p[s.key] > vmax) vmax = p[s.key]; });
  });
  if (m.pct) vmax = 1;
  if (vmax <= 0) vmax = 1;
  vmax = niceMax(vmax);

  function X(t){ return pad.l + (t - t0) / (t1 - t0) * iw; }
  function Y(v){ return pad.t + ih - (v / vmax) * ih; }

  var g = [];
  var ticks = 4;
  for (var k = 0; k <= ticks; k++){
    var v = vmax * k / ticks, y = Y(v);
    g.push('<line x1="' + pad.l + '" y1="' + y.toFixed(1) + '" x2="' + (W-pad.r) + '" y2="' + y.toFixed(1) +
      '" stroke="#eef1f6" stroke-width="1"/>');
    g.push('<text x="' + (pad.l-8) + '" y="' + (y+3.5).toFixed(1) + '" text-anchor="end" font-size="10.5" fill="#8b94a5">' +
      m.fmt(v) + '</text>');
  }
  data.turns.forEach(function(tn, idx){
    var x0 = X(Math.max(tn.t0, t0)), x1 = X(Math.min(Math.max(tn.t1, tn.t0), t1));
    if (x1 - x0 < 1) x1 = x0 + 1;
    if (idx % 2 === 1){
      g.push('<rect x="' + x0.toFixed(1) + '" y="' + pad.t + '" width="' + (x1-x0).toFixed(1) +
        '" height="' + ih + '" fill="#3b6ef6" opacity="0.035"/>');
    }
    if (idx > 0){
      g.push('<line x1="' + x0.toFixed(1) + '" y1="' + pad.t + '" x2="' + x0.toFixed(1) + '" y2="' + (pad.t+ih) +
        '" stroke="#dfe5ef" stroke-width="1" stroke-dasharray="3 3"/>');
    }
    if (x1 - x0 > 30){
      g.push('<text x="' + ((x0+x1)/2).toFixed(1) + '" y="' + (pad.t-4) + '" text-anchor="middle" font-size="9.5" fill="#a9b2c2">轮' + (idx+1) + '</text>');
    }
  });
  var n = 4;
  for (var j = 0; j <= n; j++){
    var tv = t0 + (t1-t0) * j / n, x = X(tv);
    var label = (t1 - t0 > 86400000) ? (new Date(tv).getMonth()+1) + "/" + new Date(tv).getDate() + " " + hhmm(tv) : hhmm(tv);
    g.push('<text x="' + x.toFixed(1) + '" y="' + (H-8) + '" text-anchor="middle" font-size="10.5" fill="#8b94a5">' + label + '</text>');
  }

  m.series.forEach(function(s){
    var dline = "", prevT = null;
    pts.forEach(function(p, idx){
      var x = X(p.t).toFixed(1), y = Y(p[s.key]).toFixed(1);
      var gap = prevT != null && (p.t - prevT) > 1200000;
      dline += (idx && !gap ? "L" : "M") + x + " " + y;
      prevT = p.t;
    });
    if (s.area){
      var darea = dline + "L" + X(pts[pts.length-1].t).toFixed(1) + " " + Y(0).toFixed(1) +
                  "L" + X(pts[0].t).toFixed(1) + " " + Y(0).toFixed(1) + "Z";
      var gid = "grad_" + s.key + "_" + metric;
      g.push('<defs><linearGradient id="' + gid + '" x1="0" y1="0" x2="0" y2="1">' +
        '<stop offset="0%" stop-color="' + s.color + '" stop-opacity="0.22"/>' +
        '<stop offset="100%" stop-color="' + s.color + '" stop-opacity="0.01"/></linearGradient></defs>');
      g.push('<path d="' + darea + '" fill="url(#' + gid + ')" stroke="none"/>');
    }
    g.push('<path d="' + dline + '" fill="none" stroke="' + s.color + '" stroke-width="' + (s.width||1.8) +
      '" stroke-linejoin="round" stroke-linecap="round"/>');
  });

  g.push('<line id="guide" x1="0" y1="' + pad.t + '" x2="0" y2="' + (pad.t+ih) + '" stroke="#98a6c0" stroke-width="1" stroke-dasharray="2 3" opacity="0"/>');
  g.push('<rect x="' + pad.l + '" y="' + pad.t + '" width="' + iw + '" height="' + ih + '" fill="transparent" id="hover"/>');
  svg.innerHTML = g.join("");

  var tip = $("tip"), guide = svg.querySelector("#guide");
  var hover = svg.querySelector("#hover");
  hover.addEventListener("mousemove", function(ev){
    var rect = svg.getBoundingClientRect();
    var px = (ev.clientX - rect.left) / rect.width * W;
    var best = 0, bd = 1e18;
    for (var i2 = 0; i2 < pts.length; i2++){
      var dd = Math.abs(X(pts[i2].t) - px);
      if (dd < bd){ bd = dd; best = i2; }
    }
    var p = pts[best];
    var gx = X(p.t);
    guide.setAttribute("x1", gx); guide.setAttribute("x2", gx); guide.setAttribute("opacity", "1");
    var rows = m.tip(p).map(function(r){
      return '<div class="row"><span>' + esc(r[0]) + '</span><b>' + esc(r[1]) + '</b></div>';
    }).join("");
    tip.innerHTML = '<span class="t">' + hhmm(p.t) + ' · 轮' + (p.ti+1) + (p.m ? " · " + esc(p.m) : "") +
      (p.tool ? " · " + esc(p.tool) : "") + '</span>' + rows;
    tip.style.left = Math.min(Math.max(gx, 70), W - 70) + "px";
    tip.style.top = Y(p[m.series[0].key]) + "px";
    tip.style.opacity = "1";
  });
  hover.addEventListener("mouseleave", function(){
    tip.style.opacity = "0";
    guide.setAttribute("opacity", "0");
  });
}

function niceMax(v){
  if (v <= 0) return 1;
  var exp = Math.floor(Math.log10(v));
  var base = Math.pow(10, exp);
  var f = v / base;
  var mult = f <= 1 ? 1 : f <= 1.5 ? 1.5 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 3 ? 3 : f <= 4 ? 4 : f <= 5 ? 5 : f <= 8 ? 8 : 10;
  return mult * base;
}

/* ================= 工具 × 消耗 可视化 ================= */
function vizValue(st){
  if (vizMetric === "count") return 1;
  if (vizMetric === "tokens") return st.i + st.o;
  return st.cr;
}
function vizFmt(v){
  if (vizMetric === "count") return Math.round(v) + " 次";
  if (vizMetric === "tokens") return kfmt(v) + " tok";
  return v.toFixed(2) + " 分";
}
var vizColors = {};

function buildToolStats(flat){
  var map = {};
  flat.forEach(function(st){
    var name = st.act;
    var o = map[name] || (map[name] = { name: name, credit: 0, tokens: 0, count: 0, color: "" });
    o.credit += st.cr;
    o.tokens += st.i + st.o;
    o.count += 1;
  });
  var arr = Object.keys(map).map(function(k){ return map[k]; });
  var key = vizMetric === "count" ? "count" : vizMetric === "tokens" ? "tokens" : "credit";
  arr.sort(function(a,b){ return b[key] - a[key]; });
  vizColors = {};
  arr.forEach(function(o, i){
    o.color = o.name === "模型输出" ? "#94a3b8" : PALETTE[i % PALETTE.length];
    vizColors[o.name] = o.color;
  });
  return arr;
}

function renderViz(){
  var flat = getFlat();
  var box = $("tstats"), stack = $("stack"), slab = $("slab");
  if (!flat.length){
    box.innerHTML = '<div class="empty" style="padding:16px">暂无步骤数据</div>';
    stack.innerHTML = "";
    slab.innerHTML = "";
    $("vizhint").textContent = "";
    $("stackcap").textContent = "";
    return;
  }
  var stats = buildToolStats(flat);
  var key = vizMetric === "count" ? "count" : vizMetric === "tokens" ? "tokens" : "credit";
  var maxV = stats.length ? stats[0][key] : 1;
  var totalAll = stats.reduce(function(a,b){ return a + b[key]; }, 0);
  var TOPN = 12;

  box.innerHTML = stats.slice(0, TOPN).map(function(o){
    var v = o[key];
    var share = totalAll ? (v / totalAll * 100) : 0;
    return '<div class="trow' + (toolFilter === o.name ? " on" : "") + '" data-tool="' + esc(o.name) +
      '" data-tip="' + esc(o.name + " · " + vizFmt(v) + " · 占 " + share.toFixed(1) + "% · 共 " + o.count +
      " 步 · 点击只看它") + '">' +
      '<span class="tdot" style="background:' + o.color + '"></span>' +
      '<span class="tname" title="' + esc(o.name) + '">' + esc(o.name) + '</span>' +
      '<span class="tbar"><i style="width:' + Math.max(2, v / (maxV||1) * 100).toFixed(1) + '%;background:' + o.color + '"></i></span>' +
      '<span class="tval">' + (vizMetric === "count" ? Math.round(v) : vizMetric === "tokens" ? kfmt(v) : v.toFixed(2)) + '</span>' +
      '<span class="tcnt">×' + o.count + '</span>' +
    '</div>';
  }).join("") + (stats.length > TOPN ? '<div class="trow" style="grid-template-columns:8px 1fr;color:#9aa3b2;font-size:10.5px">' +
    '<span></span><span>另有 ' + (stats.length - TOPN) + ' 个较少的工具，见明细表</span></div>' : "");

  /* 按轮次堆叠 */
  var d = state.detail;
  var topNames = stats.slice(0, 8).map(function(o){ return o.name; });
  var turns = d.turns.map(function(t){
    var segs = {};
    var tot = 0;
    t.steps.forEach(function(st){
      var nm = topNames.indexOf(st.tool || "模型输出") >= 0 ? (st.tool || "模型输出") : "其他";
      var v = vizMetric === "count" ? 1 : vizMetric === "tokens" ? (st.i + st.o) : (st.cr || 0);
      segs[nm] = (segs[nm] || 0) + v;
      tot += v;
    });
    return { tot: tot, segs: segs, credit: t.credit, steps: t.steps.length, text: t.text };
  });
  var maxTurn = Math.max.apply(null, turns.map(function(x){ return x.tot; }).concat([1]));
  var colorOf = function(nm){
    if (nm === "其他") return "#cbd5e1";
    return vizColors[nm] || "#94a3b8";
  };
  stack.innerHTML = turns.map(function(tu, i){
    var h = tu.tot > 0 ? Math.max(1.2, tu.tot / maxTurn * 100) : 0;
    var segs = Object.keys(tu.segs).sort(function(a,b){ return tu.segs[b] - tu.segs[a]; }).map(function(nm){
      var share = tu.tot ? tu.segs[nm] / tu.tot * 100 : 0;
      return '<i class="sseg" style="flex:' + Math.max(0.001, tu.segs[nm]) + ' 0 auto;background:' +
        colorOf(nm) + '" data-tip="' + esc("轮" + (i+1) + " · " + nm + " · " + vizFmt(tu.segs[nm]) +
        " · 占本轮 " + share.toFixed(0) + "%") + '"></i>';
    }).join("");
    return '<div class="scol" data-tip="' + esc("轮" + (i+1) + " · 合计 " + vizFmt(tu.tot) +
      " · " + tu.steps + " 步") + '"><div class="sbar" style="height:' + h.toFixed(1) + '%">' + segs + '</div></div>';
  }).join("");

  $("sgrid").innerHTML = [1, 0.5].map(function(f){
    return '<i style="bottom:' + (f * 100).toFixed(0) + '%"><span>' + vizFmt(maxTurn * f) + '</span></i>';
  }).join("");

  var showLab = turns.length <= 40;
  slab.innerHTML = turns.map(function(tu, i){
    return '<span>' + (showLab ? (i+1) : "") + '</span>';
  }).join("");

  var total = stats.reduce(function(a,b){ return a + b[key]; }, 0);
  $("vizhint").textContent = stats.length + " 种动作 · " + flat.length + " 步 · 合计 " +
    (vizMetric === "count" ? Math.round(total) + " 次" : vizMetric === "tokens" ? kfmt(total) + " tok" : total.toFixed(2) + " 积分");
  $("stackcap").textContent = "（单轮最高 " + vizFmt(maxTurn) + "）";
}

var vtip = null;
function bindVizTips(){
  var card = $("viz-card");
  vtip = $("vtip");
  card.addEventListener("mousemove", function(ev){
    var el = ev.target;
    var node = null;
    while (el && el !== card){
      if (el.getAttribute && el.getAttribute("data-tip")){ node = el; break; }
      el = el.parentNode;
    }
    if (!node){ vtip.style.opacity = "0"; return; }
    var r = card.getBoundingClientRect();
    vtip.textContent = node.getAttribute("data-tip");
    vtip.style.left = Math.min(Math.max(ev.clientX - r.left, 90), r.width - 90) + "px";
    vtip.style.top = (ev.clientY - r.top) + "px";
    vtip.style.opacity = "1";
  });
  card.addEventListener("mouseleave", function(){ vtip.style.opacity = "0"; });
}

/* ================= 明细表 ================= */
function curSort(){ return view === "turn" ? sortTurn : sortStep; }

function thCell(k, label, opts){
  opts = opts || {};
  var sp = curSort();
  var cls = "sortable" + (opts.l ? " l" : "");
  if (sp.k === k) cls += sp.d < 0 ? " desc" : " asc";
  var style = opts.w ? ' style="width:' + opts.w + 'px"' : "";
  return '<th class="' + cls + '" data-k="' + k + '"' + style + '>' + label + '<span class="ar"></span></th>';
}

function sortArr(arr, getters, numericSet){
  var sp = curSort();
  if (!sp.k) return arr;
  var get = getters[sp.k];
  if (!get) return arr;
  var out = arr.slice();
  out.sort(function(a,b){
    var va = get(a), vb = get(b);
    if (typeof va === "string" || typeof vb === "string"){
      return String(va).localeCompare(String(vb), "zh") * sp.d;
    }
    return ((va || 0) - (vb || 0)) * sp.d;
  });
  return out;
}

function bindHeader(){
  Array.prototype.forEach.call($("thead").querySelectorAll("th.sortable"), function(th){
    th.addEventListener("click", function(){
      var k = th.getAttribute("data-k");
      var sp = curSort();
      var numeric = ["idx","ts","steps","tools","tin","tcache","tout","credit","dur",
                     "seq","t","turnNo","i","hitv","o","cr","gap"].indexOf(k) >= 0;
      var firstDir = numeric ? -1 : 1;
      if (sp.k !== k){ sp.k = k; sp.d = firstDir; }
      else if (sp.d === firstDir){ sp.d = -firstDir; }
      else { sp.k = null; sp.d = -1; }
      renderTable();
    });
  });
}

function bindRowClick(tbody){
  Array.prototype.forEach.call(tbody.querySelectorAll("tr[data-r]"), function(tr){
    tr.addEventListener("click", function(){
      Array.prototype.forEach.call(tbody.querySelectorAll("tr"), function(x){ x.classList.remove("hl"); });
      tr.classList.add("hl");
    });
  });
}

function renderTable(){
  var thead = $("thead"), tbody = $("tbody"), table = $("dt");
  if (detailSid !== wantSid || !state.detail || !state.detail.turns){
    thead.innerHTML = "";
    tbody.innerHTML = '<tr><td colspan="12" style="text-align:center;color:#8b94a5;padding:22px">正在载入该任务的调用记录…</td></tr>';
    $("turnhint").textContent = "";
    return;
  }
  var d = state.detail;
  if (view === "turn"){
    table.style.minWidth = "";
    renderTurnRows(d, thead, tbody);
  } else {
    table.style.minWidth = "1040px";
    renderStepRows(d, thead, tbody);
  }
  var fb = $("filterbtn");
  if (toolFilter){
    fb.style.display = "";
    fb.textContent = "只看 " + toolFilter + " ✕";
  } else {
    fb.style.display = "none";
  }
  bindHeader();
  bindRowClick(tbody);
}

function renderTurnRows(d, thead, tbody){
  thead.innerHTML = '<tr>' +
    thCell("idx", "轮次", { l:true, w:56 }) +
    thCell("text", "你的输入", { l:true }) +
    thCell("ts", "时间", { w:84 }) +
    thCell("steps", "步骤", { w:58 }) +
    thCell("tools", "工具", { w:58 }) +
    thCell("tin", "输入 tok", { w:88 }) +
    thCell("tcache", "缓存命中", { w:88 }) +
    thCell("tout", "输出 tok", { w:84 }) +
    thCell("credit", "积分", { w:70 }) +
    thCell("dur", "耗时", { w:66 }) + '</tr>';

  var rows = d.turns.map(function(t, i){
    return {
      idx: i + 1, text: t.text || "", ts: t.ts, steps: t.steps.length, tools: t.tools,
      tin: t.tin, tcache: t.tcache, tout: t.tout, credit: t.credit || 0,
      dur: (t.endTs || t.ts) - t.ts, raw: t
    };
  });
  rows = sortArr(rows, {
    idx: function(x){ return x.idx; }, text: function(x){ return x.text; },
    ts: function(x){ return x.ts; }, steps: function(x){ return x.steps; },
    tools: function(x){ return x.tools; }, tin: function(x){ return x.tin; },
    tcache: function(x){ return x.tcache; }, tout: function(x){ return x.tout; },
    credit: function(x){ return x.credit; }, dur: function(x){ return x.dur; }
  });

  tbody.innerHTML = rows.map(function(x){
    var tag = x.raw.synthetic ? ' class="idx syn" title="系统压缩 / 自动续跑"' : ' class="idx"';
    return '<tr data-r="1">' +
      '<td><span' + tag + '>' + x.idx + '</span></td>' +
      '<td class="txt" title="' + esc(x.text) + '">' + esc(x.text) + '</td>' +
      '<td class="muted">' + hhmm(x.ts) + '</td>' +
      '<td>' + x.steps + '</td>' +
      '<td>' + (x.tools || '<span class="muted">0</span>') + '</td>' +
      '<td>' + kfmt(x.tin) + '</td>' +
      '<td class="muted">' + kfmt(x.tcache) + '</td>' +
      '<td>' + kfmt(x.tout) + '</td>' +
      '<td>' + x.credit.toFixed(2) + '</td>' +
      '<td class="muted">' + dur(x.dur) + '</td>' +
    '</tr>';
  }).join("");

  var total = rows.reduce(function(a,b){ return a + b.tin + b.tout; }, 0);
  var credit = rows.reduce(function(a,b){ return a + b.credit; }, 0);
  var sp = curSort();
  var sortTxt = sp.k ? " · 已按 " + $("thead").querySelector('th[data-k="' + sp.k + '"]').textContent.replace(/[▲▼]/g,"") +
    (sp.d < 0 ? " ↓" : " ↑") + " 排序（再点一次切换）" : "";
  $("turnhint").textContent = rows.length + " 轮 · " + kfmt(total) + " token · " + credit.toFixed(2) + " 积分" + sortTxt;
}

function renderStepRows(d, thead, tbody){
  thead.innerHTML = '<tr>' +
    thCell("seq", "#", { l:true, w:48 }) +
    thCell("t", "时间", { w:80 }) +
    thCell("turnNo", "轮", { w:44 }) +
    thCell("act", "工具 / 动作", { l:true, w:160 }) +
    thCell("d", "参数 / 内容", { l:true }) +
    thCell("m", "模型", { l:true, w:132 }) +
    thCell("i", "输入 tok", { w:80 }) +
    thCell("hitv", "缓存", { w:62 }) +
    thCell("o", "输出 tok", { w:80 }) +
    thCell("cr", "积分", { w:64 }) +
    thCell("gap", "距上步", { w:70 }) + '</tr>';

  var flat = getFlat();
  flat.forEach(function(x){ x.hitv = x.i > 0 ? x.c / x.i : 0; });
  if (toolFilter) flat = flat.filter(function(x){ return x.act === toolFilter; });
  var rows = sortArr(flat, {
    seq: function(x){ return x.seq; }, t: function(x){ return x.t; },
    turnNo: function(x){ return x.turnNo; }, act: function(x){ return x.act; },
    d: function(x){ return x.d; }, m: function(x){ return x.m; },
    i: function(x){ return x.i; }, hitv: function(x){ return x.hitv; },
    o: function(x){ return x.o; }, cr: function(x){ return x.cr; },
    gap: function(x){ return x.gap; }
  });

  var LIMIT = 3000;
  var shown = rows.slice(0, LIMIT);
  tbody.innerHTML = shown.map(function(st){
    var act = st.tool
      ? '<span class="ttool">' + esc(st.tool) + '</span>'
      : '<span class="muted">' + (st.d ? "生成回复" : "思考") + '</span>';
    return '<tr data-r="1">' +
      '<td class="muted">' + st.seq + '</td>' +
      '<td class="muted">' + hhmm(st.t) + '</td>' +
      '<td class="muted">' + st.turnNo + '</td>' +
      '<td class="l">' + act + '</td>' +
      '<td class="txt" title="' + esc(st.d) + '">' + (st.d ? esc(st.d) : '<span class="muted">—</span>') + '</td>' +
      '<td class="mono muted l mdl" title="' + esc(st.m) + '">' + esc(st.m || "—") + '</td>' +
      '<td>' + kfmt(st.i) + '</td>' +
      '<td class="muted">' + (st.i ? Math.round(st.hitv * 100) + "%" : "—") + '</td>' +
      '<td>' + kfmt(st.o) + '</td>' +
      '<td>' + st.cr.toFixed(2) + '</td>' +
      '<td class="muted">' + (st.gap ? dur(st.gap) : "—") + '</td>' +
    '</tr>';
  }).join("");

  var tin = rows.reduce(function(a,b){ return a + b.i; }, 0);
  var tout = rows.reduce(function(a,b){ return a + b.o; }, 0);
  var cr = rows.reduce(function(a,b){ return a + b.cr; }, 0);
  var sp = curSort();
  var sortTxt = sp.k ? " · 已按 " + $("thead").querySelector('th[data-k="' + sp.k + '"]').textContent.replace(/[▲▼]/g,"") +
    (sp.d < 0 ? " ↓" : " ↑") + " 排序" : "";
  var tail = rows.length > LIMIT ? "（仅显示前 " + LIMIT + " 行，完整数据请导出 CSV）" : "";
  $("turnhint").textContent = rows.length + " 步 · " + kfmt(tin + tout) + " token · " + cr.toFixed(2) + " 积分" + sortTxt + tail;
}

/* ================= 汇总渲染 ================= */
function renderAll(){
  renderTop();
  renderList();
  renderHero();
  renderLegend();
  var key = [wantSid||"", detailSid||"", detailVer, metric, view,
             state.detail ? state.detail.turns.length : -1].join("|");
  if (key !== lastKey){
    lastKey = key;
    renderChart();
    renderViz();
    renderTable();
  }
}

/* ================= 导出 ================= */
function exportCsv(){
  if (detailSid !== wantSid || !state.detail) return;
  var lines, name;
  if (view === "step"){
    var flat = getFlat();
    var sp = curSort();
    var rows = flat.slice();
    if (sp.k){
      var g = { seq:function(x){return x.seq;}, t:function(x){return x.t;}, turnNo:function(x){return x.turnNo;},
                act:function(x){return x.act;}, d:function(x){return x.d;}, m:function(x){return x.m;},
                i:function(x){return x.i;}, hitv:function(x){return x.i?x.c/x.i:0;}, o:function(x){return x.o;},
                cr:function(x){return x.cr;}, gap:function(x){return x.gap;} };
      var get = g[sp.k];
      if (get) rows.sort(function(a,b){
        var va = get(a), vb = get(b);
        if (typeof va === "string") return String(va).localeCompare(String(vb),"zh") * sp.d;
        return ((va||0)-(vb||0)) * sp.d;
      });
    }
    lines = ["步骤,时间,轮次,工具,参数摘要,模型,输入token,缓存命中token,输出token,积分,距上步秒"];
    rows.forEach(function(st){
      lines.push([st.seq, new Date(st.t).toLocaleString("zh-CN"), st.turnNo, st.tool || "模型输出",
        '"' + (st.d||"").replace(/"/g,'""') + '"', st.m || "",
        st.i, st.c, st.o, st.cr.toFixed(3), st.gap ? Math.round(st.gap/1000) : ""].join(","));
    });
    name = "workbuddy-steps-";
  } else {
    lines = ["轮次,时间,输入摘要,步骤数,工具调用,输入token,缓存命中token,输出token,积分,耗时秒"];
    state.detail.turns.forEach(function(t, i){
      lines.push([i+1, new Date(t.ts).toLocaleString("zh-CN"), '"' + (t.text||"").replace(/"/g,'""') + '"',
        t.steps.length, t.tools, t.tin, t.tcache, t.tout, (t.credit||0).toFixed(3),
        Math.round(((t.endTs||t.ts) - t.ts)/1000)].join(","));
    });
    name = "workbuddy-turns-";
  }
  var blob = new Blob(["\ufeff" + lines.join("\r\n")], {type:"text/csv;charset=utf-8"});
  var a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name + (wantSid || "task").slice(0,8) + ".csv";
  a.click();
}

/* ================= 事件 ================= */
$("q").addEventListener("input", renderList);
$("sort").addEventListener("change", renderList);
$("toggle").addEventListener("click", function(){
  paused = !paused;
  this.textContent = paused ? "继续" : "暂停";
  $("live-dot").classList.toggle("pulse", !paused);
  if (!paused) fetchState(true);
});
$("csv").addEventListener("click", exportCsv);

function setMetric(m){
  metric = m;
  Array.prototype.forEach.call($("tabs").querySelectorAll(".tab"), function(x){
    x.classList.toggle("on", x.getAttribute("data-m") === m);
  });
  lastKey = "";
  renderLegend();
  renderChart();
}
function setView(v){
  view = v;
  Array.prototype.forEach.call($("viewtabs").querySelectorAll(".tab"), function(x){
    x.classList.toggle("on", x.getAttribute("data-v") === v);
  });
  renderTable();
}
function clearFilter(){
  toolFilter = null;
  renderViz();
  renderTable();
}
Array.prototype.forEach.call($("tabs").querySelectorAll(".tab"), function(b){
  b.addEventListener("click", function(){ setMetric(b.getAttribute("data-m")); });
});
Array.prototype.forEach.call($("viztabs").querySelectorAll(".tab"), function(b){
  b.addEventListener("click", function(){
    Array.prototype.forEach.call($("viztabs").querySelectorAll(".tab"), function(x){ x.classList.remove("on"); });
    b.classList.add("on");
    vizMetric = b.getAttribute("data-vm");
    renderViz();
  });
});
Array.prototype.forEach.call($("viewtabs").querySelectorAll(".tab"), function(b){
  b.addEventListener("click", function(){ setView(b.getAttribute("data-v")); });
});
$("filterbtn").addEventListener("click", clearFilter);
$("tstats").addEventListener("click", function(ev){
  var el = ev.target;
  while (el && el !== this && !(el.getAttribute && el.getAttribute("data-tool") !== null)){
    el = el.parentNode;
  }
  if (!el || el === this) return;
  var name = el.getAttribute("data-tool");
  if (!name) return;
  toolFilter = (toolFilter === name) ? null : name;
  if (toolFilter && view !== "step") setView("step");
  renderViz();
  renderTable();
});

var rz;
window.addEventListener("resize", function(){
  clearTimeout(rz);
  rz = setTimeout(function(){ lastKey = ""; renderChart(); }, 180);
});

/* 深链：#steps / #credit 等 + ?task=<sid> */
(function initFromLocation(){
  var hash = (location.hash || "").replace("#", "");
  if (METRICS[hash]) metric = hash;
  if (hash === "steps"){
    view = "step";
    Array.prototype.forEach.call($("viewtabs").querySelectorAll(".tab"), function(b){
      b.classList.toggle("on", b.getAttribute("data-v") === "step");
    });
  }
  Array.prototype.forEach.call($("tabs").querySelectorAll(".tab"), function(b){
    b.classList.toggle("on", b.getAttribute("data-m") === metric);
  });
  var m = /[?&]task=([^&#]+)/.exec(location.search || "");
  if (m){
    wantSid = decodeURIComponent(m[1]);
    state.selected = wantSid;
  }
  bindVizTips();
})();

window.addEventListener("hashchange", function(){
  var hash = (location.hash || "").replace("#", "");
  if (METRICS[hash]) setMetric(hash);
  if (hash === "steps") setView("step");
});

fetchState(true);
pollTimer = setInterval(fetchState, 1800);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    store = None
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self._send(200, INDEX_HTML, "text/html; charset=utf-8")
            return
        if path == "/api/state":
            sid = None
            dv = None
            if "?" in self.path:
                for kv in self.path.split("?", 1)[1].split("&"):
                    if kv.startswith("session="):
                        sid = kv.split("=", 1)[1]
                    elif kv.startswith("dv="):
                        dv = kv.split("=", 1)[1]
            self._send(200, json.dumps(self.store.snapshot(sid, dv), ensure_ascii=False))
            return
        if path == "/api/ping":
            self._send(200, json.dumps({"ok": True}))
            return
        self._send(404, json.dumps({"error": "not found"}))


def pick_port(start):
    for p in range(start, start + 30):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", p))
            return p
        except OSError:
            continue
        finally:
            s.close()
    raise SystemExit("找不到可用端口")


def default_home():
    env = os.environ.get("WORKBUDDY_HOME")
    if env and os.path.isdir(env):
        return env
    return os.path.join(os.path.expanduser("~"), ".workbuddy")


def main():
    ap = argparse.ArgumentParser(description="WorkBuddy Token 监控看板")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--home", default=None, help="WorkBuddy 数据目录（默认 ~/.workbuddy）")
    ap.add_argument("--days", type=int, default=0, help="只扫描最近 N 天的会话，0=全部")
    ap.add_argument("--no-open", action="store_true")
    ap.add_argument("--interval", type=float, default=1.5, help="文件轮询间隔（秒）")
    args = ap.parse_args()

    home = os.path.abspath(args.home or default_home())
    if not os.path.isdir(os.path.join(home, "projects")):
        print("[!] 没找到 %s\\projects" % home)
        print("    请确认 WorkBuddy 已登录并至少跑过一个任务，或用 --home 指定数据目录。")
        raise SystemExit(2)

    global INDEX_HTML
    local_page = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.exists(local_page):
        try:
            with open(local_page, encoding="utf-8") as f:
                INDEX_HTML = f.read()
        except Exception:
            pass

    store = Store(home)
    watcher = Watcher(store, days=args.days, interval=args.interval)
    watcher.start()

    port = pick_port(args.port)
    Handler.store = store
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    url = "http://127.0.0.1:%d/" % port
    print("=" * 58)
    print(" WorkBuddy Token 监控看板")
    print(" 数据目录: %s" % home)
    print(" 访问地址: %s" % url)
    print(" 按 Ctrl+C 停止")
    print("=" * 58)
    if not args.no_open:
        threading.Timer(0.9, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        watcher.stop_flag = True
        httpd.server_close()


if __name__ == "__main__":
    main()
