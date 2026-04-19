#!/usr/bin/env python3
"""
consolidated_send.py — Phase 1 of the briefing consolidation.

Reads the most recent Cadiz brief HTML and the most recent HERA daily-brief
HTML from disk, assembles a single email (HERA hook subject + HERA body
+ Cadiz divider + Cadiz body), and ships it once to tylerk + robk via
Gmail SMTP (the same path Cadiz already uses — proven, no Graph token churn
in this consolidation step).

If exactly one source is missing or stale, we still ship the other with a
"[partial]" subject prefix so Tyler and Rob always get something at 06:05 ET.
If both are missing, we exit non-zero and let the watchdog fire.

Inputs (written by the source scripts in --no-send mode):
  /opt/briefing/logs/last_cadiz.html        + last_cadiz_subject.txt
  /opt/briefing/logs/last_cadiz_attach.json (list of absolute paths)
  /opt/hera/logs/last_hera.html             + last_hera_subject.txt

Output:
  /opt/briefing/logs/consolidated.log       (rolling)
  ONE email to RECIPIENTS via Gmail SMTP.

Design ref: docs/architecture/2026-04-19-briefing-consolidation.md
"""
from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
import traceback
from datetime import date, datetime, timedelta
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

# -------- Paths --------
CADIZ_HTML       = Path("/opt/briefing/logs/last_cadiz.html")
CADIZ_SUBJ       = Path("/opt/briefing/logs/last_cadiz_subject.txt")
CADIZ_ATTACH     = Path("/opt/briefing/logs/last_cadiz_attach.json")
HERA_HTML        = Path("/opt/hera/logs/last_hera.html")
HERA_SUBJ        = Path("/opt/hera/logs/last_hera_subject.txt")
LOG_PATH         = Path("/opt/briefing/logs/consolidated.log")

# Source files older than this many hours = "stale, treat as missing"
STALE_HOURS = 6

# -------- Config (from briefing.env, already loaded by systemd EnvironmentFile) --------
GMAIL_ADDRESS  = os.environ.get("GMAIL_ADDRESS", "tyk216@gmail.com")
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASS", "")
RECIPIENTS     = os.environ.get(
    "RECIPIENTS",
    "tylerk@timironmp.com,robk@timirontrading.com",
).split(",")


def log(msg: str) -> None:
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"  (log write failed: {e})", file=sys.stderr)


def _read_text(p: Path) -> str | None:
    if not p.exists():
        return None
    age_h = (datetime.now().timestamp() - p.stat().st_mtime) / 3600.0
    if age_h > STALE_HOURS:
        log(f"  stale ({age_h:.1f}h > {STALE_HOURS}h): {p}")
        return None
    try:
        return p.read_text(encoding="utf-8")
    except Exception as e:
        log(f"  read failed for {p}: {e}")
        return None


def _read_attachments() -> list[str]:
    if not CADIZ_ATTACH.exists():
        return []
    try:
        data = json.loads(CADIZ_ATTACH.read_text(encoding="utf-8"))
        return [p for p in data if isinstance(p, str) and Path(p).exists()]
    except Exception as e:
        log(f"  attach manifest read failed: {e}")
        return []


def assemble(hera_html: str | None, cadiz_html: str | None) -> str:
    """Build the consolidated HTML body. HERA on top, Cadiz below the divider."""
    today_str = date.today().strftime("%A, %B %d, %Y")

    parts: list[str] = [
        '<html><head><meta charset="utf-8"></head>',
        '<body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#222;max-width:760px;margin:0 auto;padding:8px;">',
        f'<div style="font-size:10pt;color:#888;margin-bottom:8px;">Timiron Daily Consolidated Brief — {today_str}</div>',
    ]

    if hera_html:
        parts.append(hera_html)
    else:
        parts.append(
            '<div style="background:#fff8e1;border-left:4px solid #f5a623;padding:10px 14px;margin:10px 0;">'
            '<b>HERA brief not available this morning.</b> See /opt/hera/logs.'
            '</div>'
        )

    parts.append(
        '<hr style="border:none;border-top:2px solid #b9452a;margin:28px 0 20px;">'
        '<div style="font-size:13pt;font-weight:700;color:#b9452a;letter-spacing:1px;margin-bottom:10px;">'
        '— CADIZ OPERATIONS —</div>'
    )

    if cadiz_html:
        parts.append(cadiz_html)
    else:
        parts.append(
            '<div style="background:#fff8e1;border-left:4px solid #f5a623;padding:10px 14px;margin:10px 0;">'
            '<b>Cadiz brief not available this morning.</b> See /opt/briefing/logs/briefing.log.'
            '</div>'
        )

    parts.append('</body></html>')
    return "\n".join(parts)


def send_via_gmail(subject: str, html_body: str, attachments: list[str]) -> None:
    if not GMAIL_APP_PASS:
        raise RuntimeError("GMAIL_APP_PASS not set in environment")

    msg = MIMEMultipart()
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    for path in attachments:
        try:
            p = Path(path)
            with p.open("rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{p.name}"')
            msg.attach(part)
            log(f"  attached: {p.name}")
        except Exception as e:
            log(f"  ATTACH FAIL {path}: {e}")

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=45) as s:
        s.starttls()
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        s.sendmail(GMAIL_ADDRESS, RECIPIENTS, msg.as_string())
    log(f"  sent to {', '.join(RECIPIENTS)} (subject: {subject!r})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Assemble + log; do not send.")
    args = ap.parse_args()

    log("=" * 62)
    log(f"consolidated_send start (dry_run={args.dry_run})")

    hera_html  = _read_text(HERA_HTML)
    cadiz_html = _read_text(CADIZ_HTML)
    hera_subj  = (_read_text(HERA_SUBJ)  or "").strip()
    cadiz_subj = (_read_text(CADIZ_SUBJ) or "").strip()

    log(f"  hera_html : {'OK ' + str(len(hera_html))  + ' bytes' if hera_html  else 'MISSING'}")
    log(f"  cadiz_html: {'OK ' + str(len(cadiz_html)) + ' bytes' if cadiz_html else 'MISSING'}")

    if not hera_html and not cadiz_html:
        log("  FATAL: neither source available — aborting; watchdog will alert.")
        return 2

    # Subject: prefer HERA hook (engagement winner). Fall back to Cadiz, then default.
    today_str = date.today().strftime("%A, %B %d, %Y")
    if hera_html and cadiz_html:
        subject = hera_subj or cadiz_subj or f"Timiron Daily Brief | {today_str}"
    elif hera_html:
        subject = f"[partial] {hera_subj or 'HERA Daily Brief'} | {today_str}"
    else:
        subject = f"[partial] {cadiz_subj or 'Timiron Cadiz Brief'} | {today_str}"

    html = assemble(hera_html, cadiz_html)
    attachments = _read_attachments() if cadiz_html else []
    log(f"  body assembled: {len(html):,} bytes; {len(attachments)} attachments")

    if args.dry_run:
        log("  DRY RUN — not sending.")
        # Drop a copy for inspection
        preview = LOG_PATH.parent / "last_consolidated_preview.html"
        preview.write_text(html, encoding="utf-8")
        log(f"  preview written: {preview}")
        return 0

    try:
        send_via_gmail(subject, html, attachments)
    except Exception as e:
        log(f"  SEND FAILED: {e}")
        log(traceback.format_exc())
        return 1

    log("consolidated_send done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
