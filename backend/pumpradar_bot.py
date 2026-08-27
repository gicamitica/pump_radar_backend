#!/usr/bin/env python3
"""
PumpRadar interactive Telegram bot.
Standalone long-polling service. Uses the same bot token as the channel poster,
but only READS updates (getUpdates) and replies to DMs/commands — it does not
post to the channel, so there's no conflict with the broadcast job.

Commands:
  /start   - welcome + links
  /help    - command list
  /check <address>  - run an OSINT rug-check on a token (EVM or Solana)
  /signals - latest WATCH signals

Run as a systemd service. Reads config from the backend .env.
"""
import asyncio
import logging
import os
import sys

import json
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("pumpradar_bot")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
API_BASE = os.environ.get("BOT_INTERNAL_API_BASE", "http://127.0.0.1:8020").rstrip("/")
SITE_URL = "https://pump.arbitrajz.com"
CHANNEL_URL = "https://t.me/PumpRadarSignals"
TRACK_RECORD_URL = f"{SITE_URL}/track-record"

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Simple per-user rate limit (seconds between /check calls)
_last_check: dict[int, float] = {}
CHECK_COOLDOWN = 15.0


def _esc(text: str) -> str:
    """Escape HTML special chars for Telegram parse_mode=HTML."""
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


SITE_BUTTON = json.dumps(
    {
        "inline_keyboard": [
            [{"text": "Open PumpRadar — 7 days free", "url": SITE_URL}]
        ]
    }
)


async def tg_send(
    client: httpx.AsyncClient,
    chat_id: int,
    text: str,
    preview: bool = False,
    button: bool = False,
):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "false" if preview else "true",
    }
    if button:
        payload["reply_markup"] = SITE_BUTTON
    try:
        await client.post(f"{TG_API}/sendMessage", data=payload, timeout=20)
    except Exception as e:
        logger.error(f"tg_send error: {e}")


WELCOME = (
    "Welcome to PumpRadar.\n\n"
    "We scan Ethereum, BSC and Solana for early on-chain activity before it hits the charts — "
    "with automated honeypot and rug-pull checks. No hype, just data you can verify.\n\n"
    "Try it here:\n"
    "/check &lt;token address&gt; — run a safety scan on a token\n"
    "/signals — latest watch signals\n"
    "/help — show commands\n\n"
    "This bot is a sample. The full platform adds what Telegram cannot:\n"
    "• Whale movements — who is buying and selling, live\n"
    "• Token OSINT Lab — holders, LP locks, contract security\n"
    "• Manipulation risk scoring on every signal\n"
    "• AI market analysis and instant alerts\n\n"
    f"7 days free, no card required: {SITE_URL}\n\n"
    f"Past calls, timestamped: {TRACK_RECORD_URL}\n"
    f"Free signals channel: {CHANNEL_URL}"
)


HELP = (
    "PumpRadar commands:\n\n"
    "/check &lt;token address&gt; — safety scan (EVM contract or Solana mint)\n"
    "Example: /check 0x1234...\n\n"
    "/signals — latest watch signals from the scanner\n"
    "/start — welcome message\n\n"
    "Full platform — whale tracking, OSINT, manipulation risk, live alerts:\n"
    f"{SITE_URL} (7 days free)\n\n"
    f"Track record: {TRACK_RECORD_URL}\n"
    f"Free signals channel: {CHANNEL_URL}\n\n"
    "Note: signals are not financial advice. Always do your own research."
)


def _fmt_check(data: dict) -> str:
    """Format an OSINT scan result into a clean Telegram message."""
    identity = data.get("identity") or {}
    verdict = data.get("ai_verdict") or {}
    safety = data.get("contract_safety") or {}
    scores = data.get("scores") or {}
    top_red_flags = data.get("red_flags") or []

    name = _esc(identity.get("name") or "Unknown")
    symbol = _esc(identity.get("symbol") or "?")
    label = _esc(verdict.get("label") or "Unknown")
    conf = verdict.get("confidence")
    summary = _esc((verdict.get("summary") or "")[:300])

    lines = [f"<b>{name}</b> (${symbol})", f"Verdict: {label}"]
    if isinstance(conf, (int, float)):
        lines.append(f"Confidence: {int(conf)}/100")

    # safety flags
    risk = scores.get("risk_score")
    if isinstance(risk, (int, float)):
        lines.append(f"Risk score: {risk}")
    if top_red_flags:
        shown = ", ".join(_esc(f) for f in top_red_flags[:5])
        lines.append(f"Red flags: {shown}")
    elif safety.get("available"):
        lines.append("Red flags: none detected")
    elif not safety.get("available"):
        reason = (safety.get("reason") or "")
        if "solana" in reason.lower():
            lines.append("Safety: Solana deep checks not connected yet (DEX + metadata only)")

    if summary:
        lines.append(f"\n{summary}")

    lines.append(
        f"\nSee who is buying and selling this token, holder breakdown "
        f"and LP locks: {SITE_URL} (7 days free)"
    )
    return "\n".join(lines)


async def cmd_check(client: httpx.AsyncClient, chat_id: int, user_id: int, arg: str):
    import time
    arg = (arg or "").strip()
    if not arg:
        await tg_send(
            client, chat_id,
            "Send a token address to scan.\nExample: <code>/check 0x...</code> or a Solana mint address.",
        )
        return

    now = time.monotonic()
    last = _last_check.get(user_id, 0)
    if now - last < CHECK_COOLDOWN:
        wait = int(CHECK_COOLDOWN - (now - last))
        await tg_send(client, chat_id, f"Easy — try again in {wait}s.")
        return
    _last_check[user_id] = now

    await tg_send(client, chat_id, "Scanning… this can take a few seconds.")
    try:
        r = await client.post(
            f"{API_BASE}/api/osint/scan",
            json={"query": arg},
            timeout=60,
        )
        if r.status_code == 400:
            await tg_send(
                client, chat_id,
                "That doesn't look like a valid EVM contract or Solana mint address. "
                "Symbol search is disabled to avoid clones — please paste the exact token address.",
            )
            return
        if r.status_code != 200:
            await tg_send(client, chat_id, "Scan failed right now. Try again in a bit.")
            return
        raw = r.json()
        data = raw.get("data", raw) if isinstance(raw, dict) else raw
        await tg_send(client, chat_id, _fmt_check(data), button=True)
    except Exception as e:
        logger.error(f"cmd_check error: {e}")
        await tg_send(client, chat_id, "Something went wrong running that scan. Try again shortly.")


async def cmd_signals(client: httpx.AsyncClient, chat_id: int):
    try:
        r = await client.get(f"{API_BASE}/api/crypto/signals-v2", timeout=30)
        if r.status_code != 200:
            await tg_send(client, chat_id, "Couldn't load signals right now. Try again shortly.")
            return
        payload = r.json()
        # signals-v2 structure: try common shapes
        sigs = []
        if isinstance(payload, dict):
            data = payload.get("data") or payload
            watch = data.get("watch_signals") if isinstance(data, dict) else None
            if isinstance(watch, list) and watch:
                sigs = watch
            else:
                # fall back: any category list
                for key in ("early_signals", "pump_signals", "watch_signals"):
                    v = data.get(key) if isinstance(data, dict) else None
                    if isinstance(v, list) and v:
                        sigs = v
                        break
        if not sigs:
            await tg_send(
                client, chat_id,
                f"No fresh watch signals at the moment. "
                f"Live scanner runs 24/7 at {SITE_URL} (7 days free).",
            )
            return

        lines = ["Latest watch signals:\n"]
        for s in sigs[:5]:
            sym = _esc(s.get("symbol", "?"))
            net = _esc((s.get("network") or "").upper())
            conf = s.get("confidence", "")
            lines.append(f"• <b>${sym}</b> {net} — confidence {conf}/100")
        lines.append(
            f"\nFull signals with whale activity, manipulation risk and AI "
            f"analysis: {SITE_URL} (7 days free)"
        )
        lines.append(f"Free channel: {CHANNEL_URL}")
        lines.append("Not financial advice.")
        await tg_send(client, chat_id, "\n".join(lines), button=True)
    except Exception as e:
        logger.error(f"cmd_signals error: {e}")
        await tg_send(client, chat_id, "Couldn't load signals right now. Try again shortly.")


async def handle_update(client: httpx.AsyncClient, update: dict):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    user = msg.get("from") or {}
    user_id = user.get("id", 0)
    text = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return

    # only react to commands
    if not text.startswith("/"):
        return

    parts = text.split(maxsplit=1)
    cmd = parts[0].lower().split("@")[0]  # strip @botname if present
    arg = parts[1] if len(parts) > 1 else ""

    if cmd == "/start":
        await tg_send(client, chat_id, WELCOME, preview=False, button=True)
    elif cmd == "/help":
        await tg_send(client, chat_id, HELP, preview=False, button=True)
    elif cmd == "/check":
        await cmd_check(client, chat_id, user_id, arg)
    elif cmd == "/signals":
        await cmd_signals(client, chat_id)
    # ignore unknown commands silently


async def main():
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set — exiting")
        sys.exit(1)

    logger.info("PumpRadar interactive bot starting (long polling)")
    offset = None
    async with httpx.AsyncClient() as client:
        # drop any backlog so we don't reply to old messages on first start
        try:
            r = await client.get(f"{TG_API}/getUpdates", params={"timeout": 0, "offset": -1}, timeout=15)
            if r.status_code == 200:
                res = r.json().get("result", [])
                if res:
                    offset = res[-1]["update_id"] + 1
        except Exception as e:
            logger.warning(f"initial getUpdates skip: {e}")

        while True:
            try:
                params = {"timeout": 30}
                if offset is not None:
                    params["offset"] = offset
                r = await client.get(f"{TG_API}/getUpdates", params=params, timeout=40)
                if r.status_code != 200:
                    await asyncio.sleep(3)
                    continue
                updates = r.json().get("result", [])
                for u in updates:
                    offset = u["update_id"] + 1
                    try:
                        await handle_update(client, u)
                    except Exception as e:
                        logger.error(f"handle_update error: {e}")
            except httpx.TimeoutException:
                continue
            except Exception as e:
                logger.error(f"polling loop error: {e}")
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
