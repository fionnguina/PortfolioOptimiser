"""Exception-alert email sender for the Portfolio Optimiser daily run.

READ-ONLY reporting: composes and sends one email. Never touches trades,
the engine, or market data. Called by daily_auto.ps1 ONLY on action-
required verdicts (RUN / HALTED) — never on SKIP, which would train the
recipient to ignore the whole channel.

Credentials live OUTSIDE the repo at ~/.portfolio_optimiser_mail.json so
they survive dist rebuilds and cannot be committed by accident. A
dedicated THROWAWAY sender account is used (e.g. guina.fund.bot@gmail.com)
so the user's personal Gmail password never touches this machine — only
the bot account's app password does, revocable on its own.

If the config is missing or still holds the placeholder password, this
exits 0 with a notice (NOT an error) so the daily wrapper never breaks
on an unconfigured mailer.
"""
from __future__ import annotations

import argparse
import json
import smtplib
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path

CONFIG_PATH = Path.home() / ".portfolio_optimiser_mail.json"
PLACEHOLDER = "PASTE_16_CHAR_APP_PASSWORD_HERE"

DEFAULT_CONFIG = {
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "guina.fund.bot@gmail.com",
    "sender_app_password": PLACEHOLDER,
    "recipient_email": "fionn.guina@gmail.com",
}


def _load_config():
    if not CONFIG_PATH.exists():
        return None, f"no mail config at {CONFIG_PATH}"
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        return None, f"mail config unreadable: {e}"
    pw = str(cfg.get("sender_app_password", "")).strip()
    if not pw or pw == PLACEHOLDER:
        return None, "placeholder credentials (bot app password not set yet)"
    for k in ("smtp_host", "smtp_port", "sender_email", "recipient_email"):
        if not str(cfg.get(k, "")).strip():
            return None, f"config missing {k}"
    return cfg, None


def _write_placeholder_config() -> bool:
    if CONFIG_PATH.exists():
        return False
    CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
    return True


def send(subject: str, body: str, timeout: int = 20) -> int:
    cfg, why = _load_config()
    if cfg is None:
        print(f"[mail] SKIP — {why}")
        return 0  # unconfigured mailer must never break the daily wrapper
    msg = EmailMessage()
    msg["From"] = cfg["sender_email"]
    msg["To"] = cfg["recipient_email"]
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(cfg["smtp_host"], int(cfg["smtp_port"]), timeout=timeout) as s:
            s.starttls(context=ctx)
            s.login(cfg["sender_email"], cfg["sender_app_password"])
            s.send_message(msg)
        print(f"[mail] sent '{subject}' -> {cfg['recipient_email']}")
        return 0
    except Exception as e:
        print(f"[mail][ERR] send failed ({type(e).__name__}): {e}")
        return 1


def main() -> int:
    p = argparse.ArgumentParser(description="Portfolio Optimiser email alert sender.")
    p.add_argument("--subject", default="[Portfolio Optimiser] alert")
    p.add_argument("--body", default="")
    p.add_argument("--body-file", default="")
    p.add_argument("--test", action="store_true",
                   help="Send a test email (or report config state) and exit.")
    p.add_argument("--init-config", action="store_true",
                   help="Write a placeholder config file if none exists, then exit.")
    args = p.parse_args()

    if args.init_config:
        made = _write_placeholder_config()
        print(f"[mail] {'wrote placeholder' if made else 'config already exists'}: {CONFIG_PATH}")
        return 0

    if args.test:
        cfg, why = _load_config()
        if cfg is None:
            print(f"[mail] NOT CONFIGURED — {why}")
            print(f"[mail] edit {CONFIG_PATH} with the bot-account app password.")
            return 0
        return send("[Portfolio Optimiser] test email",
                    "Test alert. If you received this, the daily exception "
                    "mailer is working.")

    body = args.body
    if args.body_file:
        try:
            body = Path(args.body_file).read_text(encoding="utf-8")
        except Exception as e:
            body = f"(body file unreadable: {e})\n{args.body}"
    return send(args.subject, body)


if __name__ == "__main__":
    sys.exit(main())
