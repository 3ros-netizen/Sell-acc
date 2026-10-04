import re
import imaplib
from datetime import datetime
from imap_tools import MailBox, AND
from fampay_verify.models import VerificationResult


def verify_gmail_payment(gmail: str, app_password: str, amount, utr: str = "", txnid: str = "") -> VerificationResult:
    email_clean = gmail if "@" in gmail else f"{gmail}@gmail.com"
    app_password_clean = (app_password or "").replace(" ", "")
    expected_amount = float(amount)
    # Build every plausible textual representation of the amount, since real
    # bank/UPI emails are inconsistent about formatting:
    #   487.0  (naive str(float), what we used to search for — rarely appears literally)
    #   487    (whole-rupee amounts are usually shown with no decimals)
    #   487.00 (two-decimal currency formatting)
    #   1,250 / 1,250.00 (comma thousands separator, common for amounts >= 1000)
    is_whole = expected_amount == int(expected_amount)
    int_amt = int(expected_amount)
    amount_variants = {str(expected_amount), f"{expected_amount:.2f}"}
    if is_whole:
        amount_variants.add(str(int_amt))
        amount_variants.add(f"{int_amt:,}")
        amount_variants.add(f"{int_amt:,}.00")
    # Longest-first so e.g. "1,250.00" is tried before "1,250" (both would
    # otherwise match the same text, order doesn't change correctness here,
    # but keeps the pattern's intent clear).
    amount_regex_part = "|".join(re.escape(v) for v in sorted(amount_variants, key=len, reverse=True))
    amount_pattern = rf"(?:rs\.?|inr|₹|\s|^)(?:{amount_regex_part})(?:\s|$|\.|,|/-)"

    try:
        with MailBox("imap.gmail.com", 993).login(email_clean, app_password_clean, "INBOX") as mailbox:
            if utr or txnid:
                # Exact reference code — safe to let IMAP pre-filter by it.
                search_val = utr or txnid
                messages = list(mailbox.fetch(AND(text=search_val), reverse=True, limit=50))
            else:
                # No reference code to search by. IMAP's literal-substring TEXT
                # search on a float-formatted amount (e.g. "487.0") very often
                # does not appear verbatim in the email, silently returning zero
                # results. Fetch recent messages unfiltered instead and let the
                # amount regex below (which handles real formatting variants)
                # do the actual matching.
                messages = list(mailbox.fetch(reverse=True, limit=50))
            if not messages:
                return VerificationResult(verified=False, message="Transaction not found")
            for msg in messages:
                full_text = f"{msg.subject or ''}\n{msg.text or ''}".lower()
                if not any(kw in full_text for kw in ("received", "credited", "added")):
                    continue
                if not utr and not txnid and msg.date:
                    if msg.date.timestamp() < datetime.now().timestamp() - 900:
                        continue
                if not re.search(amount_pattern, full_text, re.IGNORECASE):
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
