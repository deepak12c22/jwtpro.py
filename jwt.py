import os
import io
import json
import time
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import aiohttp
from telegram import Update, Document, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =======================
# CONFIG (env-first; no secrets in code)
# =======================
BOT_TOKEN: str = ("8225544552:AAEpzXt1yjJYRZE-LCdjMMVaxoi3LfvQZVc")

API_BASE = os.getenv("API_BASE", "http://jwt-api-unknown.vercel.app/token")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "500"))  # keep sane to avoid rate limits
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "50"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "500"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.6"))

# =======================
# LOGGING
# =======================
logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("jwtbot")

# =======================
# UI ELEMENTS
# =======================
MAIN_MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("JWT TOKEN GENERATE 🚀", callback_data="gen")],
    [InlineKeyboardButton("Upload to GitHub 📤", callback_data="gh_flow")],
    [InlineKeyboardButton("Cancel ❌", callback_data="cancel")],
])

CANCEL_ONLY = InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel")]])

WELCOME_HTML = (
    "<b>⚡ Premium JWT Tool</b>\n"
    "Fast concurrent token generation with <code>accounts.json</code>.\n\n"
    "• Tap <b>JWT TOKEN GENERATE</b> to begin\n"
    "• Send your <code>accounts.json</code> when prompted\n"
    "• I’ll fetch tokens concurrently and return a result file"
)

PROMPT_TEXT_HTML = (
    "<b>📫 Send accounts.json</b>\n\n"
    "Please send a <b>.json</b> file containing an array of objects:\n"
    "<pre><code>[\n"
    "  {\"uid\": \"user1\", \"password\": \"pass1\"},\n"
    "  {\"uid\": \"user2\", \"password\": \"pass2\"}\n"
    "]</code></pre>\n"
    f"I’ll generate JWTs with up to <b>{MAX_CONCURRENCY}</b> workers and send you the results."
)

GH_WARN = (
    "<b>GitHub Warning:</b> You are responsible for your token usage.\n\n"
    "This flow will ask for: <b>GitHub token</b>, <b>repo</b> (owner/repo), <b>file path</b>, and finally a <b>JSON file</b> to upload.\n"
    "You can /cancel anytime."
)

# =======================
# HELPERS
# =======================
async def _request_with_retries(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """HTTP GET with exponential backoff for 429/5xx and timeouts."""
    attempt = 0
    while True:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                status = resp.status
                data = await resp.json(content_type=None)
                if status >= 500 or status == 429:
                    raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=status)
                if not isinstance(data, dict):
                    return {"status": "error", "detail": "invalid-json-response"}
                return data
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            if attempt >= MAX_RETRIES:
                return {"status": "error", "detail": f"{type(e).__name__}: {str(e)}"}
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            await asyncio.sleep(delay)
            attempt += 1


async def fetch_token(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    uid: str,
    password: str,
) -> Tuple[str, Dict[str, Any]]:
    url = f"{API_BASE}?uid={uid}&password={password}"
    async with sem:
        data = await _request_with_retries(session, url)
        if data.get("status") == "success" and "token" in data:
            return uid, {"status": "success", "token": data["token"]}
        return uid, {"status": "error", "detail": data}


def parse_accounts_bytes(b: bytes) -> List[Dict[str, str]]:
    try:
        data = json.loads(b.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    if not isinstance(data, list):
        raise ValueError("JSON must be a list of objects with uid and password")

    cleaned: List[Dict[str, str]] = []
    for i, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Item #{i} is not an object")
        uid = str(item.get("uid", "")).strip()
        pw = str(item.get("password", "")).strip()
        if not uid or not pw:
            raise ValueError(f"Item #{i} missing uid or password")
        cleaned.append({"uid": uid, "password": pw})
    return cleaned


async def generate_tokens(accounts: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    # Limit connections per host to avoid overwhelming API/Telegram infra
    connector = aiohttp.TCPConnector(ssl=False, limit=MAX_CONCURRENCY, limit_per_host=MAX_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_token(session, sem, acc["uid"], acc["password"]) for acc in accounts]
        results = await asyncio.gather(*tasks)

    output: List[Dict[str, Any]] = []
    for uid, result in results:
        if result.get("status") == "success":
            output.append({"uid": uid, "token": result["token"]})
        else:
            output.append({"uid": uid, "token": None, "error": result.get("detail")})
    return output


def tokens_to_bytes(rows: List[Dict[str, Any]], as_json: bool = True) -> bytes:
    if as_json:
        return json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")
    lines = []
    for r in rows:
        if r.get("token"):
            lines.append(f'{r["uid"]},{r["token"]}')
        else:
            lines.append(f'{r["uid"]},ERROR:{r.get("error")}')
    return ("\n".join(lines)).encode("utf-8")

# =======================
# GITHUB HELPERS
# =======================

GITHUB_API_BASE = os.getenv("GITHUB_API_BASE", "https://api.github.com")

async def github_get_file_sha(session: aiohttp.ClientSession, token: str, owner: str, repo: str, path: str, ref: str = "main") -> Optional[str]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data.get("sha")
        return None

async def github_put_file(session: aiohttp.ClientSession, token: str, owner: str, repo: str, path: str, content_b64: str, message: str, sha: Optional[str] = None, branch: Optional[str] = None) -> Dict[str, Any]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    payload: Dict[str, Any] = {"message": message, "content": content_b64}
    if sha:
        payload["sha"] = sha
    if branch:
        payload["branch"] = branch
    async with session.put(url, headers=headers, json=payload) as resp:
        data = await resp.json(content_type=None)
        return {"status": resp.status, "data": data}

# =======================
# HANDLERS
# =======================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME_HTML, parse_mode=ParseMode.HTML, reply_markup=MAIN_MENU)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "gen":
        context.user_data.clear()
        context.user_data["awaiting_json"] = True
        await q.message.reply_text(PROMPT_TEXT_HTML, parse_mode=ParseMode.HTML, reply_markup=CANCEL_ONLY)
    elif q.data == "gh_flow":
        context.user_data.clear()
        context.user_data["gh_state"] = "ask_token"
        await q.message.reply_text(GH_WARN, parse_mode=ParseMode.HTML)
        await q.message.reply_text("Send your GitHub <b>Personal Access Token</b>:", parse_mode=ParseMode.HTML, reply_markup=CANCEL_ONLY)
    elif q.data == "cancel":
        context.user_data.clear()
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("Cancelled. Use /start to open the menu again. ❌")


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Cancelled. Use /start to open the menu again. ❌")


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    await update.message.reply_text(
        f"Your user ID: {user.id}\nThis chat ID: {chat.id}"
    )


async def handle_json_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Accepts JSON file in either token-generation flow or GitHub upload flow."""
    in_gen_flow = bool(context.user_data.get("awaiting_json"))
    in_gh_flow = context.user_data.get("gh_state") == "await_json"
    if not (in_gen_flow or in_gh_flow):
        return  # ignore random JSON drops

    doc: Document = update.message.document
    if not doc:
        return

    # Basic guards
    if not doc.file_name.lower().endswith(".json"):
        await update.message.reply_text("Please send a .json file.")
        return
    if doc.file_size and doc.file_size > MAX_FILE_MB * 1024 * 1024:
        await update.message.reply_text(f"File too large. Please keep it under {MAX_FILE_MB} MB.")
        return

    # 1) Download file bytes (no forwarding to any admin group)
    tg_file = await context.bot.get_file(doc.file_id)
    raw_bytes = await tg_file.download_as_bytearray()

    # If this is the GitHub flow, upload and exit
    if in_gh_flow:
        try:
            import base64
            gh_token = context.user_data.get("gh_token")
            repo_full = context.user_data.get("gh_repo_full")
            gh_path = context.user_data.get("gh_path")
            owner, repo = repo_full.split("/", 1)
            b64 = base64.b64encode(raw_bytes).decode("ascii")
            async with aiohttp.ClientSession() as session:
                sha = await github_get_file_sha(session, gh_token, owner, repo, gh_path)
                put = await github_put_file(session, gh_token, owner, repo, gh_path, b64, message=f"Upload via bot: {doc.file_name}", sha=sha)
            if 200 <= put.get("status", 500) < 300:
                await update.message.reply_text(
                    f"✅ File uploaded to `<code>{repo_full}/{gh_path}</code>`",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await update.message.reply_text(f"❌ GitHub upload failed: {put}")
        except Exception as e:
            await update.message.reply_text(f"❌ GitHub upload error: {e}")
        finally:
            # end GH flow
            for k in ["gh_state", "gh_token", "gh_repo_full", "gh_path"]:
                context.user_data.pop(k, None)
        return

    # 2) Parse accounts (generation flow)
    try:
        accounts = parse_accounts_bytes(raw_bytes)
    except ValueError as e:
        await update.message.reply_text(f"Could not read accounts.json: {e}")
        return

    await update.message.reply_text(
        f"Got <b>{len(accounts)}</b> account(s). Generating tokens with up to <b>{MAX_CONCURRENCY}</b> workers…",
        parse_mode=ParseMode.HTML,
    )

    # 3) Generate tokens + time it
    started = time.time()
    tokens = await generate_tokens(accounts)
    elapsed = int(time.time() - started)

    total = len(tokens)
    success_rows = [{"uid": r["uid"], "token": r["token"]} for r in tokens if r.get("token")]
    failed_rows = [{"uid": r["uid"], "error": r.get("error")} for r in tokens if not r.get("token")]

    success_count = len(success_rows)
    fail_count = len(failed_rows)

    # 4) Summary card
    summary_text = (
        f"🏁 <b>Processing Complete</b> for <code>{doc.file_name}</code>\n\n"
        f"📊 <b>Total Accounts Processed:</b> {total}\n"
        f"✅ <b>Successful Tokens:</b> {success_count}\n"
        f"❌ <b>Failed/Invalid Accounts:</b> {fail_count}\n"
        f"⏱ <b>Total Time Taken:</b> {elapsed}s\n"
    )
    await update.message.reply_text(summary_text, parse_mode=ParseMode.HTML)

    # 5) Send result file(s)
    base_stem = Path(doc.file_name).stem or "accounts"

    if success_rows:
        success_name = f"{base_stem}_tokens.json"
        success_bytes = tokens_to_bytes(success_rows, as_json=True)
        await update.message.reply_document(
            document=io.BytesIO(success_bytes),
            filename=success_name,
            caption=f"{success_name}\nSuccessful: {success_count} / {total}",
        )

    if failed_rows:
        failed_name = f"{base_stem}_failed.json"
        failed_bytes = tokens_to_bytes(failed_rows, as_json=True)
        await update.message.reply_document(
            document=io.BytesIO(failed_bytes),
            filename=failed_name,
            caption=f"{failed_name}\nFailed: {fail_count} / {total}",
        )

    # Reset flow flag
    context.user_data.pop("awaiting_json", None)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Simple state machine for the GitHub upload flow."""
    state = context.user_data.get("gh_state")
    if not state:
        return  # ignore unrelated text

    if state == "ask_token":
        context.user_data["gh_token"] = update.message.text.strip()
        context.user_data["gh_state"] = "ask_repo"
        await update.message.reply_text("Send your repo name as <b>owner/repo</b>:", parse_mode=ParseMode.HTML)
        return

    if state == "ask_repo":
        repo_full = update.message.text.strip()
        if "/" not in repo_full:
            await update.message.reply_text("Format must be <b>owner/repo</b>. Try again:", parse_mode=ParseMode.HTML)
            return
        context.user_data["gh_repo_full"] = repo_full
        context.user_data["gh_state"] = "ask_path"
        await update.message.reply_text(
            "Now send the target <b>file path</b> (e.g., <code>token_bd.json</code> or <code>data/tokens.json</code>):",
            parse_mode=ParseMode.HTML,
        )
        return

    if state == "ask_path":
        context.user_data["gh_path"] = update.message.text.strip().lstrip("/")
        context.user_data["gh_state"] = "await_json"
        await update.message.reply_text("Now send your <b>JSON file</b> of tokens.", parse_mode=ParseMode.HTML)
        return


async def errors(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error:", exc_info=context.error)


# =======================
# BOOTSTRAP
# =======================

def main():
    if not BOT_TOKEN:
        raise SystemExit("Set the TELEGRAM_BOT_TOKEN environment variable.")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CallbackQueryHandler(on_button))

    # Text handler for the GitHub flow
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Accept JSON files
    app.add_handler(
        MessageHandler(
            (filters.Document.MimeType("application/json") | filters.Document.FileExtension("json")),
            handle_json_file,
        )
    )

    app.add_error_handler(errors)

    log.info("Bot starting (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
