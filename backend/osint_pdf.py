"""
OSINT PDF Report Generator (Phase 2).
Izolat: nu modifica scanner/judge/enricher. Doar consuma osint data + genereaza PDF.
"""
from io import BytesIO
from datetime import datetime, timezone
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER

# Brand colors (match frontend)
NEON = colors.HexColor("#27EAA4")
CYAN = colors.HexColor("#2BD9E8")
AMBER = colors.HexColor("#FFAE3B")
DANGER = colors.HexColor("#FF5468")
INK = colors.HexColor("#04070C")
PANEL = colors.HexColor("#0C121C")
LINE = colors.HexColor("#26314A")
MUTED = colors.HexColor("#7D88A3")
MUTED2 = colors.HexColor("#525A73")
WHITE = colors.HexColor("#E2E8F0")


def _fmt_usd(v):
    if v is None:
        return "n/a"
    try:
        v = float(v)
    except Exception:
        return "n/a"
    if v >= 1e9: return f"${v/1e9:.2f}B"
    if v >= 1e6: return f"${v/1e6:.2f}M"
    if v >= 1e3: return f"${v/1e3:.2f}K"
    if v >= 1:   return f"${v:.2f}"
    if v > 0:    return f"${v:.8f}".rstrip("0").rstrip(".")
    return "$0"


def _fmt_pct(v, plus=False):
    if v is None:
        return "n/a"
    try:
        v = float(v)
    except Exception:
        return "n/a"
    sign = "+" if (plus and v >= 0) else ""
    return f"{sign}{v:.2f}%"


def _short_addr(a):
    if not a or len(a) < 12: return a or ""
    return f"{a[:6]}...{a[-4:]}"


def _risk_color(level):
    l = (level or "").upper()
    if l in ("LOW",): return NEON
    if l in ("MEDIUM", "MODERATE"): return AMBER
    if l in ("HIGH", "CRITICAL"): return DANGER
    return MUTED


def _verdict_color(verdict):
    v = (verdict or "").lower()
    if "pump" in v or "buy" in v or "accumul" in v: return NEON
    if "dump" in v or "sell" in v: return DANGER
    if "watch" in v or "early" in v: return CYAN
    return AMBER


def build_osint_pdf(osint_data: dict, generated_for_email: str = None) -> bytes:
    """
    Build compact 2-3 page OSINT PDF.
    osint_data = the 'data' object from /api/crypto/osint/{chain}/{address}
    Returns raw PDF bytes.
    """
    d = osint_data or {}
    token = d.get("token") or {}
    market = d.get("market") or {}
    verdict = d.get("verdict") or {}
    holders = d.get("holders") or {}
    security = d.get("security") or {}
    links = d.get("links") or []
    signal_meta = d.get("signal_meta") or {}
    cg = d.get("cg_metrics") or {}
    query = d.get("query") or {}

    symbol = token.get("symbol") or query.get("address", "?")[:8].upper()
    chain = (token.get("chain") or query.get("chain") or "").upper()
    address = query.get("address") or ""

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=14*mm, bottomMargin=14*mm,
        title=f"PumpRadar OSINT Report - {symbol}",
        author="PumpRadar",
    )

    styles = getSampleStyleSheet()
    s_title = ParagraphStyle('t', parent=styles['Title'], fontSize=22, textColor=WHITE, spaceAfter=2, fontName='Helvetica-Bold')
    s_sub = ParagraphStyle('sub', parent=styles['Normal'], fontSize=9, textColor=MUTED, spaceAfter=8, fontName='Helvetica')
    s_h = ParagraphStyle('h', parent=styles['Heading2'], fontSize=11, textColor=CYAN, spaceAfter=6, spaceBefore=12, fontName='Helvetica-Bold', leading=13)
    s_body = ParagraphStyle('b', parent=styles['Normal'], fontSize=10, textColor=WHITE, leading=14, fontName='Helvetica')
    s_muted = ParagraphStyle('m', parent=styles['Normal'], fontSize=8, textColor=MUTED2, fontName='Helvetica')
    s_verdict = ParagraphStyle('v', parent=styles['Normal'], fontSize=14, textColor=WHITE, alignment=TA_CENTER, fontName='Helvetica-Bold', leading=18)

    story = []

    # ===== HEADER =====
    header_tbl = Table([[
        Paragraph(f"<b>PumpRadar</b> <font color='#7D88A3'>OSINT Report</font>", s_title),
        Paragraph(f"<para align=right><font size=8 color='#7D88A3'>Generated {datetime.now(timezone.utc).strftime('%d %b %Y, %H:%M UTC')}</font></para>", s_muted),
    ]], colWidths=[110*mm, 60*mm])
    header_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('LINEBELOW', (0,0), (-1,-1), 1, LINE),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 6*mm))

    # ===== TOKEN HEADER =====
    price = token.get("price_usd")
    ch24 = token.get("price_change_h24")
    ch1  = token.get("price_change_h1")

    token_hdr = Table([[
        Paragraph(f"<font size=24 color='white'><b>{symbol}</b></font><br/><font size=9 color='#7D88A3'>{token.get('name') or symbol} · {chain}</font>", s_body),
        Paragraph(f"<para align=right><font size=18 color='white'><b>{_fmt_usd(price)}</b></font><br/><font size=9 color='{'#27EAA4' if (ch24 or 0)>=0 else '#FF5468'}'>{_fmt_pct(ch24, plus=True)} (24h)</font> <font size=9 color='#7D88A3'> · {_fmt_pct(ch1, plus=True)} (1h)</font></para>", s_body),
    ]], colWidths=[85*mm, 85*mm])
    token_hdr.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BACKGROUND', (0,0), (-1,-1), PANEL),
        ('BOX', (0,0), (-1,-1), 0.5, LINE),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))
    story.append(token_hdr)
    story.append(Spacer(1, 4*mm))

    # Address row
    story.append(Paragraph(f"<font size=8 color='#7D88A3'>Contract:</font> <font size=8 color='#E2E8F0' face='Courier'>{address}</font>", s_muted))
    story.append(Spacer(1, 5*mm))

    # ===== AI VERDICT =====
    story.append(Paragraph("AI VERDICT", s_h))
    v_col = _verdict_color(verdict.get("verdict"))
    r_col = _risk_color(verdict.get("risk_level"))
    conf = verdict.get("confidence") or 0

    v_tbl = Table([
        [Paragraph(f"<font size=14 color='{v_col.hexval().replace('0x','#')}'><b>{verdict.get('verdict') or '—'}</b></font>",  s_body),
         Paragraph(f"<para align=center><font size=10 color='#7D88A3'>Risk</font><br/><font size=13 color='{r_col.hexval().replace('0x','#')}'><b>{verdict.get('risk_level') or '—'}</b></font></para>", s_body),
         Paragraph(f"<para align=right><font size=10 color='#7D88A3'>Confidence</font><br/><font size=13 color='white'><b>{conf}%</b></font></para>", s_body)],
    ], colWidths=[70*mm, 50*mm, 50*mm])
    v_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BACKGROUND', (0,0), (-1,-1), PANEL),
        ('BOX', (0,0), (-1,-1), 0.5, LINE),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    story.append(v_tbl)
    story.append(Spacer(1, 3*mm))

    reason = verdict.get("reason") or ""
    if reason:
        story.append(Paragraph(f"<font size=9 color='#E2E8F0'>{reason}</font>", s_body))
        story.append(Spacer(1, 2*mm))
    story.append(Paragraph(f"<font size=8 color='#525A73'>Source: {verdict.get('ai_source') or 'n/a'} · Dump risk: {(verdict.get('dump_risk_level') or 'n/a').upper()} · Manipulation prob: {verdict.get('manipulation_probability') or 0}%</font>", s_muted))

    # ===== MARKET =====
    story.append(Paragraph("MARKET DATA", s_h))
    mcap = cg.get("market_cap")
    rank = cg.get("rank")
    m_rows = [
        ["Liquidity", _fmt_usd(market.get("liquidity_usd")), "Market Cap", _fmt_usd(mcap)],
        ["Volume 24h", _fmt_usd(market.get("volume_h24")), "CG Rank", f"#{rank}" if rank else "n/a"],
        ["Buy/Sell 1h", f"{market.get('buy_sell_ratio_h1'):.2f}" if market.get('buy_sell_ratio_h1') else "n/a",
         "ATH", _fmt_usd(cg.get("ath"))],
    ]
    m_tbl = Table(m_rows, colWidths=[35*mm, 50*mm, 35*mm, 50*mm])
    m_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), PANEL),
        ('BOX', (0,0), (-1,-1), 0.5, LINE),
        ('INNERGRID', (0,0), (-1,-1), 0.3, LINE),
        ('TEXTCOLOR', (0,0), (0,-1), MUTED),
        ('TEXTCOLOR', (2,0), (2,-1), MUTED),
        ('TEXTCOLOR', (1,0), (1,-1), WHITE),
        ('TEXTCOLOR', (3,0), (3,-1), WHITE),
        ('FONTNAME', (1,0), (1,-1), 'Helvetica-Bold'),
        ('FONTNAME', (3,0), (3,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(m_tbl)

    # ===== SECURITY =====
    story.append(Paragraph("SECURITY (GoPlus)", s_h))
    def _yn(v):
        if v is None: return "n/a"
        if isinstance(v, bool): return "Yes" if v else "No"
        return str(v)
    sec_rows = [
        ["Honeypot", _yn(security.get("is_honeypot")), "Mintable", _yn(security.get("is_mintable"))],
        ["Buy Tax", _yn(security.get("buy_tax")), "Sell Tax", _yn(security.get("sell_tax"))],
        ["Pausable", _yn(security.get("transfer_pausable")), "Open Source", _yn(security.get("is_open_source"))],
        ["Contract Risk", (security.get("contract_risk") or "n/a").upper(), "Security Score", f"{security.get('score')}/100" if security.get("score") is not None else "n/a"],
    ]
    sec_tbl = Table(sec_rows, colWidths=[35*mm, 50*mm, 35*mm, 50*mm])
    sec_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), PANEL),
        ('BOX', (0,0), (-1,-1), 0.5, LINE),
        ('INNERGRID', (0,0), (-1,-1), 0.3, LINE),
        ('TEXTCOLOR', (0,0), (0,-1), MUTED),
        ('TEXTCOLOR', (2,0), (2,-1), MUTED),
        ('TEXTCOLOR', (1,0), (1,-1), WHITE),
        ('TEXTCOLOR', (3,0), (3,-1), WHITE),
        ('FONTNAME', (1,0), (1,-1), 'Helvetica-Bold'),
        ('FONTNAME', (3,0), (3,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(sec_tbl)

    # ===== WHALE ACTIVITY (from signal_meta) =====
    if signal_meta.get("whale_score") is not None:
        story.append(Paragraph("WHALE ACTIVITY", s_h))
        w_rows = [
            ["Whale Score", f"{signal_meta.get('whale_score')}/100",
             "Accumulation", _yn(signal_meta.get("whale_accumulation"))],
            ["Unique Buyers", str(signal_meta.get("whale_unique_buyers") or 0),
             "Unique Sellers", str(signal_meta.get("whale_unique_sellers") or 0)],
            ["Dump Risk", _yn(signal_meta.get("whale_dump_risk")),
             "Sources", ", ".join(signal_meta.get("sources") or []) or "n/a"],
        ]
        w_tbl = Table(w_rows, colWidths=[35*mm, 50*mm, 35*mm, 50*mm])
        w_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), PANEL),
            ('BOX', (0,0), (-1,-1), 0.5, LINE),
            ('INNERGRID', (0,0), (-1,-1), 0.3, LINE),
            ('TEXTCOLOR', (0,0), (0,-1), MUTED),
            ('TEXTCOLOR', (2,0), (2,-1), MUTED),
            ('TEXTCOLOR', (1,0), (1,-1), WHITE),
            ('TEXTCOLOR', (3,0), (3,-1), WHITE),
            ('FONTNAME', (1,0), (1,-1), 'Helvetica-Bold'),
            ('FONTNAME', (3,0), (3,-1), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ]))
        story.append(w_tbl)

    # ===== PAGE 2 =====
    story.append(PageBreak())
    story.append(Paragraph(f"<font size=14 color='white'><b>{symbol}</b></font> <font size=10 color='#7D88A3'>· Top Holders & Links</font>", s_body))
    story.append(Spacer(1, 4*mm))

    # ===== TOP HOLDERS =====
    top_holders = (holders.get("top_holders") or [])[:10]
    story.append(Paragraph("TOP 10 HOLDERS", s_h))
    if top_holders:
        h_rows = [["#", "Address", "Holdings %", "Type"]]
        for i, h in enumerate(top_holders, 1):
            h_rows.append([
                str(i),
                _short_addr(h.get("address") or ""),
                f"{h.get('pct'):.2f}%" if h.get('pct') is not None else "n/a",
                "Contract" if h.get("is_contract") else "Wallet",
            ])
        h_tbl = Table(h_rows, colWidths=[10*mm, 70*mm, 40*mm, 30*mm])
        h_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#1a2438")),
            ('BACKGROUND', (0,1), (-1,-1), PANEL),
            ('TEXTCOLOR', (0,0), (-1,0), CYAN),
            ('TEXTCOLOR', (0,1), (-1,-1), WHITE),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTNAME', (1,1), (1,-1), 'Courier'),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('BOX', (0,0), (-1,-1), 0.5, LINE),
            ('INNERGRID', (0,0), (-1,-1), 0.3, LINE),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING', (0,0), (-1,-1), 6),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('ALIGN', (2,0), (2,-1), 'RIGHT'),
        ]))
        story.append(h_tbl)

        # Concentration warning
        top1 = top_holders[0].get("pct") or 0
        top5 = sum((h.get("pct") or 0) for h in top_holders[:5])
        story.append(Spacer(1, 3*mm))
        conc_color = "#FF5468" if top1 > 20 else ("#FFAE3B" if top1 > 10 else "#27EAA4")
        story.append(Paragraph(
            f"<font size=9 color='#7D88A3'>Top 1 holds </font><font size=9 color='{conc_color}'><b>{top1:.2f}%</b></font><font size=9 color='#7D88A3'> · Top 5 combined </font><font size=9 color='white'><b>{top5:.2f}%</b></font>",
            s_body
        ))
    else:
        story.append(Paragraph("<font size=9 color='#7D88A3'>No holder data available for this chain.</font>", s_muted))

    # ===== LINKS =====
    story.append(Spacer(1, 8*mm))
    story.append(Paragraph("OFFICIAL LINKS", s_h))
    if links:
        l_rows = [[l.get("label") or "Link", l.get("url") or ""] for l in links]
        l_tbl = Table(l_rows, colWidths=[35*mm, 135*mm])
        l_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), PANEL),
            ('BOX', (0,0), (-1,-1), 0.5, LINE),
            ('INNERGRID', (0,0), (-1,-1), 0.3, LINE),
            ('TEXTCOLOR', (0,0), (0,-1), MUTED),
            ('TEXTCOLOR', (1,0), (1,-1), CYAN),
            ('FONTNAME', (0,0), (0,-1), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(l_tbl)
    else:
        story.append(Paragraph("<font size=9 color='#7D88A3'>No official links available.</font>", s_muted))

    # ===== FOOTER =====
    story.append(Spacer(1, 15*mm))
    story.append(Paragraph(
        f"<font size=7 color='#525A73'>Generated by PumpRadar for <b>{generated_for_email or 'anonymous'}</b> · pump.arbitrajz.com · Not financial advice · Data as of {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')}</font>",
        s_muted
    ))

    def _bg(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(INK)
        canvas.rect(0, 0, A4[0], A4[1], stroke=0, fill=1)
        canvas.restoreState()

    doc.build(story, onFirstPage=_bg, onLaterPages=_bg)
    return buf.getvalue()
