# Felix Store — Telegram Account Bot

## Deploy on Railway

1. Push this folder to a GitHub repo, then in Railway: **New Project → Deploy from GitHub repo**.
2. Go to your new service → **Variables** tab and add all keys from `.env.example`:
   - `API_ID`, `API_HASH` — from https://my.telegram.org
   - `BOT_TOKEN` — from @BotFather
   - `ADMIN_ID` — your numeric Telegram user ID (from @userinfobot)
   - `UPI_ID`, `SUPPORT_USER`, `BOT_NAME`, `LOG_CHANNEL`, `WELCOME_IMG`, `DEFAULT_2FA`, `DEFAULT_NAME`
   - `FAMPAY_GMAIL`, `FAMPAY_GMAIL_APP_PASSWORD` — see below
3. Railway auto-detects `requirements.txt` and `Procfile`. No web port needed — it runs as a background worker.
4. Add a **Volume** in Railway and mount it at `/app` (or at least at the paths in `SESSIONS_DIR`/`SOLD_DIR`/`PENDING_DIR`/`DB_PATH`). Without a volume, the SQLite database and saved Telegram sessions are wiped on every redeploy.

## Gmail App Password (for auto-verify deposits)

1. Enable 2-Step Verification on the Gmail account that receives your UPI payment alerts.
2. Go to https://myaccount.google.com/apppasswords and generate an app password.
3. Set `FAMPAY_GMAIL` to that Gmail address and `FAMPAY_GMAIL_APP_PASSWORD` to the 16-character password (spaces don't matter, they're stripped automatically).
4. Leave both blank to disable auto-verify — deposits will fall back to manual screenshot + admin approval only.

## How auto-verify deposits work

When a user enters a deposit amount, the bot adds a random paise value to it (e.g. ₹500 → ₹500.37) and stores that exact figure. The user pays that exact amount and taps **I Have Paid**, and the bot scans the configured Gmail inbox for a matching UPI-credit email in the last 15 minutes. If found, the wallet is credited instantly; if not, the user can retry or fall back to sending a screenshot for manual admin approval — nothing is lost either way.

**Note on the `fampay-verify` package:** the version on PyPI ships an async `verify_payment()` that internally does `async with MailBox(...)`, but the `imap_tools` package it depends on doesn't implement the async context-manager protocol on that class — calling it as documented raises a `TypeError` every time. `fampay_gmail.py` in this project re-implements the same matching logic (amount/UTR extraction, 15-minute window, sender-name parsing) directly against `imap_tools` in a plain synchronous function, run inside `asyncio.to_thread(...)` so it doesn't block the bot while scanning Gmail. It still returns the same `VerificationResult` shape from `fampay_verify.models`, so nothing else changes if that upstream bug ever gets fixed.

## Files

- `bot.py` — the bot
- `fampay_gmail.py` — Gmail-based UPI payment verification
- `requirements.txt`, `Procfile`, `railway.json`, `runtime.txt` — deployment config
- `.env.example` — copy the variable names into Railway's Variables tab

## Local run

```
pip install -r requirements.txt
cp .env.example .env   # fill in real values, then export them or use a tool like `honcho`/`direnv`
python bot.py
```
