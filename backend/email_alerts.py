import asyncio
import logging
import resend
import os

logger = logging.getLogger(__name__)
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "noreply@arbitrajz.com")
APP_URL = os.environ.get("APP_URL", "http://localhost:3000")
resend.api_key = os.environ.get("RESEND_API_KEY", "")

def build_signal_card_html(signal: dict, app_url: str) -> str:
    cat = signal.get("category", "watch")
    sym = signal.get("symbol", "")
    name = signal.get("name", sym)
    network = signal.get("network", "")
    confidence = signal.get("confidence", 0)
    reason = signal.get("reason", "")
    price = signal.get("price_usd", 0)
    h1 = signal.get("price_change_h1", 0)
    h24 = signal.get("price_change_h24", 0)
    vol = signal.get("volume_h24", 0)
    whale_score = signal.get("whale_score", 0)
    manip = signal.get("manipulation_probability", 0)
    bs = signal.get("buy_sell_ratio_h1", 0)
    pre_pump = signal.get("pre_pump_activity", False)
    coin_url = f"{app_url}/coin/{sym}"

    def fmt_price(n):
        if not n: return "n/a"
        return f"${n:.2f}" if n > 1 else f"${n:.6f}"
    def fmt_vol(n):
        if not n: return "n/a"
        if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
        if n >= 1_000: return f"${n/1_000:.0f}K"
        return f"${n:.0f}"
    def fmt_pct(n):
        arrow = "▲" if n > 0 else "▼" if n < 0 else "→"
        color = "#4ade80" if n > 0 else "#f87171" if n < 0 else "#94a3b8"
        return f'<span style="color:{color}">{arrow} {abs(n):.2f}%</span>'

    if cat == "early":
        bg = "#1c133a"
        badge_style = "background-color:rgba(147,51,234,0.3);color:#c084fc;"
        icon_style = "background-color:#6366f1;color:#fff;"
        text_color = "#a5b4fc"
        conf_color = "#818cf8"
        label_color = "#4f46e5"
        btn_style = "background-color:#6366f1;color:#fff;"
        badge_text = "⚡ Early Detection"
        headline = "Early movement detected — market hasn't reacted yet"
        desc = f"Our online scan detected unusual buying activity on {sym} before any price movement. Buy/sell ratio {bs:.1f} signals accumulation in progress. This is an early estimate based on real-time data — not a confirmed pump."
        btn_text = f"⚡ Monitor {sym} on PumpRadar →"
    elif cat == "pump":
        bg = "#0a2419"
        badge_style = "background-color:rgba(34,197,94,0.2);color:#4ade80;"
        icon_style = "background-color:#10b981;color:#fff;"
        text_color = "#86efac"
        conf_color = "#34d399"
        label_color = "#10b981"
        btn_style = "background-color:#10b981;color:#0d0f1a;"
        badge_text = "🔥 Pump Signal"
        headline = f"Pump imminent on {sym}" + (" — pre-pump activity confirmed" if pre_pump else "")
        desc = f"Our scan detected strong pump signals on {sym}. Whale score {whale_score}/100. {reason}"
        btn_text = f"🔥 View {sym} Signal →"
    else:
        bg = "#2a1215"
        badge_style = "background-color:rgba(239,68,68,0.2);color:#f87171;"
        icon_style = "background-color:#ef4444;color:#fff;"
        text_color = "#fca5a5"
        conf_color = "#f87171"
        label_color = "#ef4444"
        btn_style = "background-color:#dc2626;color:#fff;"
        badge_text = "☠️ Dump Warning"
        headline = f"Distribution risk detected on {sym} — high reversal probability"
        desc = f"Our scan detected dump/distribution signals on {sym}. Manipulation {manip}%. {reason}"
        btn_text = f"☠ View {sym} Risk →"

    return f"""
<tr>
  <td style="padding:0 16px 16px 16px;">
    <table width="100%" style="border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr>
        <td style="background-color:{bg};padding:20px;border-radius:12px;">
          <div style="display:inline-block;padding:4px 10px;border-radius:20px;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;margin-bottom:15px;{badge_style}">{badge_text}</div>
          <table width="100%">
            <tr>
              <td width="54" valign="top">
                <div style="width:44px;height:44px;border-radius:50%;text-align:center;line-height:44px;font-weight:bold;font-size:14px;display:inline-block;{icon_style}">{sym[:2].upper()}</div>
              </td>
              <td valign="top">
                <div style="font-size:26px;font-weight:800;color:#fff;line-height:1;">{sym}</div>
                <div style="font-size:11px;margin-top:2px;color:{text_color};">{network}</div>
              </td>
              <td valign="top" align="right">
                <div style="font-size:32px;font-weight:800;text-align:right;line-height:1;color:{conf_color};">{confidence}%</div>
                <div style="font-size:10px;text-align:right;margin-top:2px;color:{text_color};">confidence</div>
              </td>
            </tr>
          </table>
          <div style="font-size:15px;font-weight:700;margin:20px 0 10px 0;line-height:1.4;color:{text_color};">{headline}</div>
          <div style="font-size:12px;line-height:1.5;margin-bottom:20px;color:{text_color};">{desc}</div>
          <table width="100%" cellspacing="5" style="margin-bottom:15px;">
            <tr>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">Price</div>
                <div style="font-size:13px;font-weight:700;">{fmt_price(price)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">1h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_pct(h1)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">24h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_pct(h24)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">Vol 24h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_vol(vol)}</div>
              </td>
            </tr>
          </table>
          <a href="{coin_url}" style="display:block;text-align:center;text-decoration:none;padding:14px;font-size:14px;font-weight:700;border-radius:8px;margin-top:5px;{btn_style}">{btn_text}</a>
        </td>
      </tr>
    </table>
  </td>
</tr>"""


async def send_signal_alert_emails(db, signals: list):
    """Send signal alert email to all active subscribers."""
    if not signals:
        return

    early = [s for s in signals if s.get("category") == "early" and s.get("pre_pump_activity")]
    pumps = [s for s in signals if s.get("category") == "pump" and s.get("confidence", 0) >= 75]
    dumps = [s for s in signals if s.get("category") in ("dump", "risk") and s.get("confidence", 0) >= 70]

    alert_signals = early + pumps + dumps
    if not alert_signals:
        return

    # get active subscribers
    try:
        users = await db.users.find(
            {"subscription": {"$exists": True, "$nin": ["free", None, ""]}, "email": {"$exists": True}}
        ).to_list(length=1000)
    except Exception as e:
        logger.error(f"Signal alert: failed to fetch users: {e}")
        return

    if not users:
        return

    from datetime import datetime as dt
    now_str = dt.now().strftime("%A, %B %d · %H:%M")

    cards_html = "".join([build_signal_card_html(s, APP_URL) for s in alert_signals[:5]])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Signal Alert - PumpRadar V2</title>
</head>
<body style="margin:0;padding:0;background-color:#0d0f1a;font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,Arial,sans-serif;color:#ffffff;">
<center style="width:100%;background-color:#0d0f1a;padding:20px 0;">
<table style="background-color:#121424;margin:0 auto;width:100%;max-width:550px;border-spacing:0;border-radius:16px;overflow:hidden;">

  <!-- HEADER -->
  <tr>
    <td style="padding:35px 20px 25px 20px;text-align:center;background:linear-gradient(180deg,#161933 0%,#121424 100%);">
      <div style="color:#707599;font-size:11px;text-transform:uppercase;letter-spacing:2px;font-weight:bold;margin-bottom:5px;">ArbitrajZ</div>
      <h1 style="margin:0;font-size:32px;color:#ffffff;font-weight:800;letter-spacing:-0.5px;">Early Detection</h1>
      <div style="color:#525875;font-size:11px;margin-top:6px;">{now_str} · AI powered detection</div>
    </td>
  </tr>

  {cards_html}

  <!-- DASHBOARD BUTTON -->
  <tr>
    <td style="padding:20px 16px;">
      <a href="{APP_URL}/dashboard" style="background-color:#4f46e5;color:#ffffff;text-decoration:none;display:block;text-align:center;padding:16px;border-radius:8px;font-weight:700;font-size:15px;">Open Full Dashboard →</a>
    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td style="text-align:center;padding:30px 20px;font-size:11px;color:#414666;line-height:1.6;background-color:#0b0c16;">
      <div>ArbitrajZ · PumpRadar V2 · AI-powered crypto signal detection</div>
      <div style="margin-top:4px;">AI-generated signals. Not financial advice. Trade responsibly.</div>
    </td>
  </tr>

</table>
</center>
</body>
</html>"""

    subject = f"⚡ PumpRadar Early Detection: {early[0]['symbol']} — market hasn't moved yet"

    sent = 0
    for user in users:
        email = user.get("email")
        if not email:
            continue
        try:
            await asyncio.to_thread(resend.Emails.send, {
                "from": f"PumpRadar <{SENDER_EMAIL}>",
                "to": [email],
                "subject": subject,
                "html": html,
            })
            sent += 1
        except Exception as e:
            logger.error(f"Signal alert email error for {email}: {e}")

    logger.info(f"Signal alert: sent to {sent}/{len(users)} subscribers — {subject}")
