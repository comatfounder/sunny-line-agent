"""
LINE OA AI 客服 Template Server
---------------------------------
快組隊標準版本 v1.0

功能模組：
  7-1  對話寫入 Google Sheets
  7-2  SYSTEM_PROMPT + 知識庫從 Sheets 動態載入
  7-3  業主 Admin 指令模式（偵測 FOUNDER_LINE_USER_ID）
  7-4  業主觸發對話分析（report / report 週 / report 月）
  7-5  每日自動報告（APScheduler）
  7-6  提示詞更新確認流程（update prompt → confirm/cancel）
  7-7  ESCALATE 例外升級 + pause/resume + relay mode

啟動方式（本機開發）：
  1. cp .env.example .env  並填入環境變數
  2. pip install -r requirements.txt
  3. python3 line_oa_server.py
  4. ngrok http 5000
  5. 把 ngrok URL + /webhook 貼到 LINE Developers Webhook URL

部署（Railway）：
  railway up --detach

環境變數（必填）：
  LINE_CHANNEL_SECRET          LINE Messaging API Channel Secret
  LINE_CHANNEL_ACCESS_TOKEN    LINE Messaging API Long-lived Token
  ANTHROPIC_API_KEY            Anthropic API Key
  GOOGLE_CREDENTIALS_JSON      GCP Service Account JSON 完整內容
  GOOGLE_SHEET_ID              Google Sheet ID
  FOUNDER_LINE_USER_ID         業主的 LINE User ID（admin 模式）
"""

import os
import re
import json
import hashlib
import hmac
import base64
import logging
import pathlib
from collections import defaultdict
from datetime import datetime, timedelta

from flask import Flask, request, abort
import anthropic
import requests
from dotenv import load_dotenv

# APScheduler（每日自動報告）
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False

# Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials as GCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False


# ── 環境變數 ──────────────────────────────────────────────────────────────────

_ENV_FILE = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=True)

LINE_CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
ANTHROPIC_API_KEY         = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CREDENTIALS_JSON   = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID           = os.environ.get("GOOGLE_SHEET_ID", "")
FOUNDER_LINE_USER_ID      = os.environ.get("FOUNDER_LINE_USER_ID", "")

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
LINE_PUSH_URL  = "https://api.line.me/v2/bot/message/push"
CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5")

# Sheets 分頁名稱（依客戶 Sheets 設定調整）
SHEET_LOG       = "對話記錄"
SHEET_SETTINGS  = "設定"
SHEET_KB        = "知識庫"
SHEET_ESCALATE  = "例外記錄"
SHEET_PAUSED    = "暫停用戶"


# ── Flask App ─────────────────────────────────────────────────────────────────

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── 全域狀態（in-memory） ──────────────────────────────────────────────────────

conversation_history: dict[str, list[dict]] = defaultdict(list)
# ESCALATE 後被暫停的用戶（重啟後清空；如需持久化，讀寫 SHEET_PAUSED）
paused_users: set[str] = set()
# 等待業主 confirm/cancel 的提示詞更新（key = admin user_id）
pending_prompt_update: dict[str, str] = {}

MAX_HISTORY = 20  # 每位用戶保留的最大對話輪數


# ── Google Sheets 連線（懶載入） ──────────────────────────────────────────────

_gs_workbook = None

def get_workbook():
    """取得 Google Sheets workbook，失敗回傳 None"""
    global _gs_workbook
    if _gs_workbook is not None:
        return _gs_workbook
    if not GSPREAD_AVAILABLE or not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = GCredentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        _gs_workbook = gc.open_by_key(GOOGLE_SHEET_ID)
        log.info("Google Sheets 連線成功")
    except Exception as e:
        log.error("Google Sheets 初始化失敗: %s", e)
        return None
    return _gs_workbook


def get_sheet(tab_name: str):
    """取得指定分頁，失敗回傳 None"""
    wb = get_workbook()
    if wb is None:
        return None
    try:
        return wb.worksheet(tab_name)
    except Exception as e:
        log.error("取得分頁 %s 失敗: %s", tab_name, e)
        return None


def read_settings() -> dict:
    """從 Sheets 設定分頁讀取所有 key-value"""
    sheet = get_sheet(SHEET_SETTINGS)
    if sheet is None:
        return {}
    try:
        rows = sheet.get_all_values()
        return {r[0]: r[1] for r in rows if len(r) >= 2 and r[0]}
    except Exception as e:
        log.error("讀取設定失敗: %s", e)
        return {}


def write_setting(key: str, value: str) -> bool:
    """更新 Sheets 設定分頁中的特定 key"""
    sheet = get_sheet(SHEET_SETTINGS)
    if sheet is None:
        return False
    try:
        cell = sheet.find(key)
        if cell:
            sheet.update_cell(cell.row, 2, value)
        else:
            sheet.append_row([key, value])
        return True
    except Exception as e:
        log.error("寫入設定失敗 %s: %s", key, e)
        return False


# ── 模組 7-2：SYSTEM_PROMPT 從 Sheets 動態載入 ───────────────────────────────

_cached_system_prompt: str = ""


def build_system_prompt() -> str:
    """
    組合完整 SYSTEM_PROMPT：
    SYSTEM_PROMPT_BASE（Sheets 設定分頁）+ 知識庫（Sheets 知識庫分頁）
    """
    settings = read_settings()
    base = settings.get("SYSTEM_PROMPT_BASE", "你是一位專業的客服助理。")

    # 讀取知識庫分頁
    kb_sheet = get_sheet(SHEET_KB)
    kb_text = ""
    if kb_sheet:
        try:
            rows = kb_sheet.get_all_values()
            # 跳過標題列，格式：類別 | 問題 | 標準回答 | 備註
            entries = []
            current_category = ""
            for row in rows[1:]:
                if len(row) < 3 or not row[1]:
                    continue
                cat, q, a = row[0], row[1], row[2]
                if cat and cat != current_category:
                    current_category = cat
                    entries.append(f"\n[{cat}]")
                entries.append(f"問題：{q}\n回答：{a}")
            if entries:
                kb_text = "\n\n# 知識庫\n" + "\n".join(entries)
        except Exception as e:
            log.error("讀取知識庫失敗: %s", e)

    return base + kb_text


def load_system_prompt() -> str:
    """載入並快取 SYSTEM_PROMPT，回傳完整內容"""
    global _cached_system_prompt
    _cached_system_prompt = build_system_prompt()
    log.info("SYSTEM_PROMPT 已載入（%d 字）", len(_cached_system_prompt))
    return _cached_system_prompt


def get_system_prompt() -> str:
    """取得快取的 SYSTEM_PROMPT，若尚未載入則先載入"""
    global _cached_system_prompt
    if not _cached_system_prompt:
        return load_system_prompt()
    return _cached_system_prompt


# ── 模組 7-1：對話寫入 Sheets ─────────────────────────────────────────────────

def log_conversation(
    user_id: str,
    user_text: str,
    reply_text: str,
    status: str = "OK",
) -> None:
    """將對話記錄寫入 Sheets，失敗不影響主流程"""
    sheet = get_sheet(SHEET_LOG)
    if sheet is None:
        return
    try:
        sheet.append_row([
            datetime.now().isoformat(),
            user_id,
            user_text,
            reply_text,
            status,
            len(conversation_history.get(user_id, [])),
        ])
    except Exception as e:
        log.error("Sheets 對話記錄寫入失敗: %s", e)


def log_escalate(user_id: str, trigger_text: str) -> None:
    """將 ESCALATE 事件寫入例外記錄分頁"""
    sheet = get_sheet(SHEET_ESCALATE)
    if sheet is None:
        return
    try:
        sheet.append_row([
            datetime.now().isoformat(),
            user_id,
            trigger_text,
            "未處理",
            "",
        ])
    except Exception as e:
        log.error("Sheets 例外記錄寫入失敗: %s", e)


# ── LINE API 工具 ─────────────────────────────────────────────────────────────

def verify_signature(body: bytes, signature: str) -> bool:
    hash_ = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256
    ).digest()
    expected = base64.b64encode(hash_).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def line_reply(reply_token: str, text: str) -> None:
    """透過 LINE Reply API 回覆（免費，webhook 5 秒內）"""
    if not reply_token:
        return
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}],
    }
    resp = requests.post(LINE_REPLY_URL, headers=headers, json=payload, timeout=10)
    if not resp.ok:
        log.error("LINE Reply API error %s: %s", resp.status_code, resp.text)


def line_push(user_id: str, text: str) -> None:
    """透過 LINE Push API 主動推送（計費）"""
    if not user_id or not LINE_CHANNEL_ACCESS_TOKEN:
        return
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    payload = {
        "to": user_id,
        "messages": [{"type": "text", "text": text}],
    }
    resp = requests.post(LINE_PUSH_URL, headers=headers, json=payload, timeout=10)
    if not resp.ok:
        log.error("LINE Push API error %s: %s", resp.status_code, resp.text)
    else:
        log.info("Push sent to %s: %s", user_id, text[:60])


# ── 預約模組 ──────────────────────────────────────────────────────────────────

BOOKING_KEYWORDS = ["預約", "時段", "什麼時候", "哪天", "幾號", "哪個時間",
                    "訂", "排", "檔期", "何時", "available", "book"]

SHEET_SLOTS   = "可預約時段"
SHEET_BOOKING = "預約記錄"


def get_available_slots() -> list[dict]:
    """讀取 Sheets 可預約時段分頁，回傳可用時段清單"""
    sheet = get_sheet(SHEET_SLOTS)
    if sheet is None:
        return []
    try:
        rows = sheet.get_all_values()
        available = []
        for row in rows[1:]:
            if len(row) >= 3 and row[2].strip() == "可預約":
                available.append({"date": row[0].strip(), "slot": row[1].strip()})
        return available
    except Exception as e:
        log.error("讀取可預約時段失敗: %s", e)
        return []


def book_slot(user_id: str, date: str, slot: str,
              name: str, contact: str) -> tuple[bool, str]:
    """
    原子性預約操作（workers=1 保證無 race condition）：
    1. 再次確認時段仍為「可預約」
    2. 將該行改為「已滿」
    3. 寫入預約記錄
    回傳 (success, message)
    """
    ws_slots   = get_sheet(SHEET_SLOTS)
    ws_records = get_sheet(SHEET_BOOKING)
    if ws_slots is None or ws_records is None:
        return False, "系統暫時無法完成預約，請稍後再試"
    try:
        rows = ws_slots.get_all_values()
        target_row = None
        for i, row in enumerate(rows[1:], start=2):
            if len(row) >= 3 and row[0].strip() == date and row[1].strip() == slot:
                target_row = i
                if row[2].strip() != "可預約":
                    return False, f"很抱歉，{date} {slot} 這個時段剛被預約走了，請選擇其他時段。"
                break
        if target_row is None:
            return False, "找不到這個時段，請確認日期和時間是否正確。"

        # 標記為已滿
        ws_slots.update_cell(target_row, 3, "已滿")

        # 寫入預約記錄
        ws_records.append_row([
            datetime.now().isoformat(),
            user_id,
            name,
            contact,
            f"{date} {slot}",
            "已確認",
        ])

        log.info("預約成功：%s %s → %s (%s)", date, slot, name, contact)
        return True, f"已為 {name} 預約 {date} {slot} 的拍攝時段"
    except Exception as e:
        log.error("預約操作失敗: %s", e)
        return False, "預約時發生錯誤，請稍後再試。"


def is_booking_related(text: str) -> bool:
    return any(kw in text for kw in BOOKING_KEYWORDS)


def get_dynamic_system_prompt(user_message: str) -> str:
    """根據訊息內容動態組合 SYSTEM_PROMPT，預約相關訊息注入即時時段"""
    base = get_system_prompt()
    if not is_booking_related(user_message):
        return base

    slots = get_available_slots()
    if slots:
        slots_text = "\n".join(f"- {s['date']} {s['slot']}" for s in slots)
        booking_ctx = f"""

# 目前可預約時段（即時資料，請以此為準）

{slots_text}

# 預約確認流程

引導客人完成以下三步驟後才執行預約：
步驟一：確認拍攝類型（親子、個人、全家福、情侶）
步驟二：客人從上方時段中選擇一個
步驟三：請客人提供姓名和聯絡方式（電話或 LINE ID）

三項資訊齊全後，在回覆末尾加入預約標記（客人看不到此標記）：
BOOK:date:[日期]:slot:[時段]:name:[姓名]:contact:[聯絡方式]

範例：BOOK:date:2026-05-10:slot:10:00-12:00:name:王小明:contact:0912345678

注意：
- 若客人未選定時段，先列出可用時段讓他選
- 確認資訊後才輸出 BOOK 標記，不要提前觸發
- 輸出 BOOK 標記時，同時用文字告知客人預約已完成並說明細節"""
        return base + booking_ctx
    else:
        no_slot_ctx = """

# 目前可預約時段

目前所有時段均已預約。
請告知客人目前無可用時段，並詢問是否要留下姓名和聯絡方式，有空檔時主動通知。"""
        return base + no_slot_ctx


# ── Claude API ────────────────────────────────────────────────────────────────

def call_claude(user_id: str, user_message: str) -> tuple[str, str]:
    """
    呼叫 Claude API，維護對話歷史。
    回傳 (reply_text, status)，status = OK / ERROR / ESCALATE
    """
    history = conversation_history[user_id]
    history.append({"role": "user", "content": user_message})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
        conversation_history[user_id] = history

    status = "OK"
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=get_dynamic_system_prompt(user_message),   # 動態注入時段
            messages=history,
        )
        reply_text = response.content[0].text
    except Exception as e:
        log.error("Claude API error: %s", e)
        reply_text = "目前系統有點忙，請稍後再試。"
        status = "ERROR"

    # ── 偵測 BOOK 標記（預約）────────────────────────────────────────────────
    book_match = re.search(
        r"BOOK:date:([^:]+):slot:([^:\n]+):name:([^:]+):contact:([^\s\n]+)",
        reply_text,
    )
    if book_match:
        b_date, b_slot, b_name, b_contact = [g.strip() for g in book_match.groups()]
        # 移除標記，用戶看不到
        reply_text = re.sub(r"\s*BOOK:date:[^\n]+", "", reply_text).strip()
        # 執行預約
        success, booking_msg = book_slot(user_id, b_date, b_slot, b_name, b_contact)
        if success:
            status = "BOOKED"
            # Push 通知業主
            if FOUNDER_LINE_USER_ID:
                line_push(FOUNDER_LINE_USER_ID,
                    f"新預約\n日期：{b_date} {b_slot}\n姓名：{b_name}\n聯絡：{b_contact}")
            log.info("BOOKED: %s %s → %s", b_date, b_slot, b_name)
        else:
            # 預約失敗（時段已被搶訂），覆蓋 AI 回覆
            status = "BOOK_FAIL"
            reply_text = booking_msg
            log.warning("BOOK_FAIL: %s %s", b_date, b_slot)

    # ── 偵測 ESCALATE 標記 ────────────────────────────────────────────────────
    escalate_match = re.search(r"ESCALATE:user_id:(U\w+)", reply_text)
    if escalate_match:
        status = "ESCALATE"
        reply_text = re.sub(r"\s*ESCALATE:user_id:U\w+", "", reply_text).strip()

    history.append({"role": "assistant", "content": reply_text})
    log_conversation(user_id, user_message, reply_text, status)
    return reply_text, status


def call_claude_analysis(prompt: str) -> str:
    """用 Claude 分析 Sheets 資料（不帶對話歷史，純分析用）"""
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        log.error("Claude analysis error: %s", e)
        return "分析時發生錯誤，請稍後再試。"


# ── 模組 7-4：對話分析 ────────────────────────────────────────────────────────

def fetch_conversations(days: int) -> list[list]:
    """從 Sheets 讀取最近 N 天的對話記錄"""
    sheet = get_sheet(SHEET_LOG)
    if sheet is None:
        return []
    try:
        rows = sheet.get_all_values()
        cutoff = datetime.now() - timedelta(days=days)
        result = []
        for row in rows[1:]:  # 跳過標題列
            if not row or not row[0]:
                continue
            try:
                ts = datetime.fromisoformat(row[0])
                if ts >= cutoff:
                    result.append(row)
            except ValueError:
                continue
        return result
    except Exception as e:
        log.error("讀取對話記錄失敗: %s", e)
        return []


def analyze_conversations(days: int) -> str:
    """讀取 Sheets 對話記錄 → Claude 分析 → 回傳報告文字"""
    rows = fetch_conversations(days)
    label = {1: "今日", 7: "本週", 30: "本月"}.get(days, f"最近{days}天")

    if not rows:
        return f"[{label}報告]\n\n此期間無對話記錄。"

    # 組合分析 prompt（避免超過 token limit，截取最多 200 筆）
    sample = rows[-200:]
    records_text = "\n".join(
        f"用戶:{r[1]} | 訊息:{r[2][:100]} | AI:{r[3][:100]} | 狀態:{r[4]}"
        for r in sample
        if len(r) >= 5
    )

    escalate_count = sum(1 for r in rows if len(r) >= 5 and r[4] == "ESCALATE")
    unique_users = len(set(r[1] for r in rows if r[1]))

    analysis_prompt = f"""以下是 LINE OA 客服系統{label}的對話記錄（共 {len(rows)} 筆，{unique_users} 位用戶，ESCALATE {escalate_count} 件）：

{records_text}

請分析並回覆以下內容（繁體中文，簡潔）：
1. 最常被問的問題 Top 5
2. AI 回答不佳或用戶反應不好的對話（若有）
3. 知識庫建議補充的項目
4. ESCALATE 事件摘要（若有）
5. 整體觀察與下週建議

格式：純文字，不要 markdown 標題符號，每段前面加數字即可。"""

    report = call_claude_analysis(analysis_prompt)
    header = f"[{label}報告] {datetime.now().strftime('%Y-%m-%d %H:%M')}\n對話：{len(rows)} 筆 / 用戶：{unique_users} 人 / ESCALATE：{escalate_count} 件\n\n"
    return header + report


# ── 模組 7-5：每日自動報告（APScheduler） ────────────────────────────────────

def send_daily_report() -> None:
    """APScheduler 每日定時呼叫，push 昨日報告給業主"""
    if not FOUNDER_LINE_USER_ID:
        log.warning("FOUNDER_LINE_USER_ID 未設定，無法發送每日報告")
        return
    log.info("每日報告觸發中...")
    report = analyze_conversations(days=1)
    line_push(FOUNDER_LINE_USER_ID, report)


def init_scheduler() -> None:
    """初始化 APScheduler（server 啟動時呼叫）"""
    if not APSCHEDULER_AVAILABLE:
        log.warning("APScheduler 未安裝，跳過每日報告排程")
        return

    settings = read_settings()
    report_time = settings.get("DAILY_REPORT_TIME", "09:00")
    try:
        hour, minute = map(int, report_time.split(":"))
    except ValueError:
        hour, minute = 9, 0

    scheduler = BackgroundScheduler(timezone="Asia/Taipei")
    scheduler.add_job(
        func=send_daily_report,
        trigger="cron",
        hour=hour,
        minute=minute,
        id="daily_report",
    )
    scheduler.start()
    log.info("每日報告排程已啟動，時間：%02d:%02d Asia/Taipei", hour, minute)


# ── 模組 7-3：Admin 指令處理 ──────────────────────────────────────────────────

ADMIN_HELP = """可用指令：
status — 系統狀態
report — 今日對話摘要
report 週 — 本週報告
report 月 — 本月報告
update prompt [內容] — 更新提示詞（有確認流程）
confirm — 確認提示詞更新
cancel — 取消提示詞更新
reload — 重新載入知識庫與設定
pause [user_id] — 暫停特定用戶的 AI 回應
resume [user_id] — 恢復特定用戶的 AI 回應
reply [user_id] [訊息] — 代傳訊息給用戶（relay 模式）
help — 顯示此說明"""


def handle_admin(user_id: str, text: str) -> str:
    """處理業主 admin 指令，回傳要推播給業主的回應文字"""
    text_lower = text.strip().lower()
    original = text.strip()

    # ── status ────────────────────────────────────────────────────────────────
    if text_lower == "status":
        active = len(conversation_history)
        paused = len(paused_users)
        pending = len(pending_prompt_update)
        sheet = get_sheet(SHEET_ESCALATE)
        unhandled = 0
        if sheet:
            try:
                rows = sheet.get_all_values()
                unhandled = sum(1 for r in rows[1:] if len(r) >= 4 and r[3] == "未處理")
            except Exception:
                pass
        return (
            f"系統狀態 {datetime.now().strftime('%m/%d %H:%M')}\n"
            f"活躍用戶：{active} 人\n"
            f"暫停中：{paused} 人\n"
            f"未處理 ESCALATE：{unhandled} 件\n"
            f"待確認提示詞更新：{pending} 件\n"
            f"SYSTEM_PROMPT 長度：{len(get_system_prompt())} 字"
        )

    # ── help ──────────────────────────────────────────────────────────────────
    if text_lower == "help":
        return ADMIN_HELP

    # ── reload ────────────────────────────────────────────────────────────────
    if text_lower == "reload":
        load_system_prompt()
        return f"已重新載入知識庫與設定。\nSYSTEM_PROMPT 長度：{len(get_system_prompt())} 字"

    # ── report ────────────────────────────────────────────────────────────────
    if text_lower == "report":
        return analyze_conversations(days=1)
    if text_lower == "report 週":
        return analyze_conversations(days=7)
    if text_lower == "report 月":
        return analyze_conversations(days=30)

    # ── confirm / cancel（提示詞更新確認流程）────────────────────────────────
    if text_lower == "confirm":
        if user_id not in pending_prompt_update:
            return "目前沒有待確認的提示詞更新。"
        new_prompt = pending_prompt_update.pop(user_id)
        if write_setting("SYSTEM_PROMPT_BASE", new_prompt):
            load_system_prompt()
            return "提示詞已更新並即時生效。"
        else:
            return "寫入 Sheets 失敗，請稍後再試。"

    if text_lower == "cancel":
        if user_id in pending_prompt_update:
            pending_prompt_update.pop(user_id)
            return "已取消，提示詞未變更。"
        return "目前沒有待取消的更新。"

    # ── update prompt [內容] ──────────────────────────────────────────────────
    if text_lower.startswith("update prompt "):
        addition = original[len("update prompt "):].strip()
        if not addition:
            return "請在 update prompt 後面加上要新增的內容。"
        current = get_system_prompt()
        new_prompt = current.rstrip() + "\n\n" + addition
        pending_prompt_update[user_id] = new_prompt
        preview = addition if len(addition) <= 200 else addition[:200] + "..."
        return (
            f"準備新增以下內容到提示詞末尾：\n\n「{preview}」\n\n"
            "確認請回覆：confirm\n取消請回覆：cancel"
        )

    # ── pause [user_id] ───────────────────────────────────────────────────────
    if text_lower.startswith("pause "):
        target = original[6:].strip()
        if not target.startswith("U"):
            return f"user_id 格式錯誤（應為 Uxxxxxx），收到：{target}"
        paused_users.add(target)
        return f"已暫停 {target} 的 AI 回應。\n恢復請傳：resume {target}"

    # ── resume [user_id] ──────────────────────────────────────────────────────
    if text_lower.startswith("resume "):
        target = original[7:].strip()
        paused_users.discard(target)
        return f"已恢復 {target} 的 AI 自動回應。"

    # ── reply [user_id] [訊息]（relay mode）─────────────────────────────────
    if text_lower.startswith("reply "):
        parts = original[6:].split(" ", 1)
        if len(parts) < 2:
            return "格式：reply [user_id] [要傳給用戶的訊息]"
        target_id, relay_msg = parts[0].strip(), parts[1].strip()
        if not target_id.startswith("U"):
            return f"user_id 格式錯誤（應為 Uxxxxxx），收到：{target_id}"
        line_push(target_id, relay_msg)
        log_conversation(target_id, "[業主 relay]", relay_msg, "RELAY")
        return f"已代傳給 {target_id}。"

    # ── 未知指令 ──────────────────────────────────────────────────────────────
    return f"不認識這個指令。輸入 help 查看可用指令。\n（收到：{original[:50]}）"


# ── 模組 7-7：ESCALATE 升級通知 ──────────────────────────────────────────────

def handle_escalate(escalate_user_id: str, trigger_text: str) -> None:
    """
    ESCALATE 升級流程：
    1. 暫停該用戶的 AI 回應
    2. 寫入 Sheets 例外記錄
    3. Push 通知業主
    """
    paused_users.add(escalate_user_id)
    log_escalate(escalate_user_id, trigger_text)

    if FOUNDER_LINE_USER_ID:
        notify = (
            f"有一位用戶需要人工介入\n"
            f"用戶ID：{escalate_user_id}\n"
            f"觸發訊息：{trigger_text[:100]}\n\n"
            f"該用戶 AI 回應已暫停，可用指令：\n"
            f"reply {escalate_user_id} [你的回覆]\n"
            f"resume {escalate_user_id}"
        )
        line_push(FOUNDER_LINE_USER_ID, notify)
    log.info("ESCALATE: user %s → paused + notified founder", escalate_user_id)


# ── Webhook 路由 ──────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data()

    if not verify_signature(body, signature):
        log.warning("Invalid signature")
        abort(400)

    try:
        events = json.loads(body.decode("utf-8")).get("events", [])
    except Exception:
        abort(400)

    for event in events:
        event_type = event.get("type")

        # ── 訊息事件 ──────────────────────────────────────────────────────────
        if event_type == "message":
            msg = event.get("message", {})
            if msg.get("type") != "text":
                continue  # 貼圖、圖片等非文字訊息靜默忽略

            user_id     = event["source"]["userId"]
            reply_token = event["replyToken"]
            user_text   = msg["text"].strip()

            log.info("MSG from %s: %s", user_id, user_text[:80])

            # ── Admin 模式（模組 7-3）────────────────────────────────────────
            if FOUNDER_LINE_USER_ID and user_id == FOUNDER_LINE_USER_ID:
                response = handle_admin(user_id, user_text)
                # Admin 回應用 Push（避免超過 5 秒 webhook timeout）
                line_push(user_id, response)
                continue

            # ── 暫停模式（模組 7-7）──────────────────────────────────────────
            if user_id in paused_users:
                # 暫停中的用戶不觸發 AI，靜默（或視需求回一句「請稍候」）
                log.info("User %s is paused, skipping AI response", user_id)
                # line_reply(reply_token, "請稍候，我們的團隊將盡快與您聯繫。")
                continue

            # ── 一般客服模式 ──────────────────────────────────────────────────
            reply_text, status = call_claude(user_id, user_text)
            line_reply(reply_token, reply_text)
            log.info("REPLY to %s [%s]: %s", user_id, status, reply_text[:80])

            # ── ESCALATE 處理（模組 7-7）─────────────────────────────────────
            if status == "ESCALATE":
                handle_escalate(user_id, user_text)

        # ── 加入好友事件 ──────────────────────────────────────────────────────
        elif event_type == "follow":
            user_id     = event["source"]["userId"]
            reply_token = event.get("replyToken", "")

            # 歡迎訊息從 Sheets 設定分頁讀取（鍵：WELCOME_MESSAGE）
            settings = read_settings()
            welcome = settings.get(
                "WELCOME_MESSAGE",
                "你好，很高興認識你。有什麼我可以幫你的嗎？",
            )
            if reply_token:
                line_reply(reply_token, welcome)
            else:
                line_push(user_id, welcome)
            log.info("New follower: %s", user_id)

        # ── 封鎖事件 ──────────────────────────────────────────────────────────
        elif event_type == "unfollow":
            user_id = event["source"]["userId"]
            conversation_history.pop(user_id, None)
            paused_users.discard(user_id)
            log.info("Unfollowed: %s", user_id)

    return "OK", 200


# ── 健康檢查 ──────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return {
        "status": "ok",
        "time": datetime.now().isoformat(),
        "active_users": len(conversation_history),
        "paused_users": len(paused_users),
        "system_prompt_length": len(get_system_prompt()),
    }, 200


# ── 進入點 ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    missing = []
    if not LINE_CHANNEL_SECRET:       missing.append("LINE_CHANNEL_SECRET")
    if not LINE_CHANNEL_ACCESS_TOKEN: missing.append("LINE_CHANNEL_ACCESS_TOKEN")
    if not ANTHROPIC_API_KEY:         missing.append("ANTHROPIC_API_KEY")
    if missing:
        log.warning("⚠️  以下環境變數尚未設定：%s", ", ".join(missing))

    # 初始化 SYSTEM_PROMPT（從 Sheets 載入）
    load_system_prompt()

    # 啟動每日報告排程
    init_scheduler()

    log.info("LINE OA AI Server 啟動中... http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
else:
    # gunicorn 啟動時也要初始化
    load_system_prompt()
    init_scheduler()
