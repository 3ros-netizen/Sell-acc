import re
import imaplib
from datetime import datetime
from imap_tools import MailBox, AND
from fampay_verify.models import VerificationResult


def verify_gmail_payment(gmail: str, app_password: str, amount, utr: str = "", txnid: str = "") -> VerificationResult:
    email_clean = gmail if "@" in gmail else f"{gmail}@gmail.com"
    app_password_clean = (app_password or "").replace(" ", "")
    expected_amount = float(amount)
    try:
        with MailBox("imap.gmail.com", 993).login(email_clean, app_password_clean, "INBOX") as mailbox:
            search_val = utr or txnid or str(amount)
            messages = list(mailbox.fetch(AND(text=search_val), reverse=True, limit=50))
            if not messages:
                return VerificationResult(verified=False, message="Transaction not found")
            for msg in messages:
                full_text = f"{msg.subject or ''}\n{msg.text or ''}".lower()
                if not any(kw in full_text for kw in ("received", "credited", "added")):
                    continue
                if not utr and not txnid and msg.date:
                    if msg.date.timestamp() < datetime.now().timestamp() - 900:
                        continue
                amount_pattern = rf"(?:rs\.?|inr|₹|\s|^){expected_amount}(?:\.00)?(?:\s|$|\.)"
                if not (re.search(amount_pattern, full_text, re.IGNORECASE) or str(expected_amount) in full_text):
                    continue
                sender_name = "UPI User"
                name_match = re.search(r"(?:from|received from|sender)\s+([a-zA-Z ]{3,30})", full_text)
                if name_match:
                    sender_name = name_match.group(1).strip()
                    if sender_name.lower().endswith(" at"):
                        sender_name = sender_name[:-3].strip()
                extracted_utr = utr
                if not extracted_utr:
                    utr_match = re.search(r"(?:utr|upi ref no|ref no|reference no)\s*:\s*([0-9]{12})", full_text, re.IGNORECASE)
                    if utr_match:
                        extracted_utr = utr_match.group(1).strip()
                payment_time = msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return VerificationResult(
                    verified=True,
                    transaction_id=extracted_utr or None,
                    amount=expected_amount,
                    utr=extracted_utr,
                    sender_name=sender_name,
                    payment_time_ist=payment_time,
                )
            return VerificationResult(verified=False, message="Transaction not found")
    except imaplib.IMAP4.error:
        return VerificationResult(verified=False, message="Invalid Gmail credentials")
    except Exception as e:
        return VerificationResult(verified=False, message=str(e)[:200])
