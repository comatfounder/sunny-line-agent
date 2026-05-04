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
from datetime import datetime, timedelta, timezone

# 台灣時區（UTC+8）
TZ_TW = timezone(timedelta(hours=8))

def now_tw() -> datetime:
    """回傳當前台灣時間（UTC+8），不帶時區標記（避免與 Sheets 舊資料比較時 TypeError）"""
    return datetime.now(tz=TZ_TW).replace(tzinfo=None)

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

# Redis
try:
    import redis as redis_lib
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


# ── 環境變數 ──────────────────────────────────────────────────────────────────

_ENV_FILE = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=True)

LINE_CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
ANTHROPIC_API_KEY         = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CREDENTIALS_JSON   = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_SHEET_ID           = os.environ.get("GOOGLE_SHEET_ID", "")
FOUNDER_LINE_USER_ID      = os.environ.get("FOUNDER_LINE_USER_ID", "")
# 一次性密碼認領：業主在 LINE 傳 "admin [密碼]" 即可自助完成 admin 綁定
ADMIN_SETUP_PASSWORD      = os.environ.get("ADMIN_SETUP_PASSWORD", "")
REDIS_URL                 = os.environ.get("REDIS_URL", "")

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


# ── Redis 連線 ────────────────────────────────────────────────────────────────

_redis_client = None

def get_redis():
    """取得 Redis 連線，失敗回傳 None（降級為 in-memory）"""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not REDIS_AVAILABLE or not REDIS_URL:
        return None
    try:
        _redis_client = redis_lib.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
        _redis_client.ping()
        log.info("Redis 連線成功：%s", REDIS_URL.split("@")[-1])
    except Exception as e:
        log.warning("Redis 連線失敗，降級使用 in-memory：%s", e)
        _redis_client = None
    return _redis_client


# ── 對話歷史存取（Redis 優先，降級 in-memory） ────────────────────────────────

HISTORY_TTL = 60 * 60 * 24 * 7   # Redis key 保留 7 天
MAX_HISTORY  = 20                  # 每位用戶保留的最大對話輪數（條數）

_mem_history: dict[str, list[dict]] = defaultdict(list)   # in-memory 降級備用


def get_history(user_id: str) -> list[dict]:
    """讀取用戶對話歷史（Redis → in-memory 降級）"""
    r = get_redis()
    if r:
        try:
            raw = r.get(f"hist:{user_id}")
            return json.loads(raw) if raw else []
        except Exception as e:
            log.warning("Redis get_history 失敗：%s", e)
    return list(_mem_history[user_id])


def set_history(user_id: str, history: list[dict]) -> None:
    """寫入用戶對話歷史（Redis → in-memory 降級）"""
    r = get_redis()
    if r:
        try:
            r.setex(f"hist:{user_id}", HISTORY_TTL, json.dumps(history, ensure_ascii=False))
            return
        except Exception as e:
            log.warning("Redis set_history 失敗：%s", e)
    _mem_history[user_id] = history


def clear_history(user_id: str) -> None:
    """清除用戶對話歷史"""
    r = get_redis()
    if r:
        try:
            r.delete(f"hist:{user_id}")
        except Exception:
            pass
    _mem_history.pop(user_id, None)


# ── 暫停用戶存取（Redis 優先，降級 in-memory） ────────────────────────────────

_mem_paused: set[str] = set()


def is_paused(user_id: str) -> bool:
    r = get_redis()
    if r:
        try:
            return bool(r.sismember("paused_users", user_id))
        except Exception:
            pass
    return user_id in _mem_paused


def pause_user(user_id: str) -> None:
    r = get_redis()
    if r:
        try:
            r.sadd("paused_users", user_id)
            return
        except Exception:
            pass
    _mem_paused.add(user_id)


def resume_user(user_id: str) -> None:
    r = get_redis()
    if r:
        try:
            r.srem("paused_users", user_id)
            return
        except Exception:
            pass
    _mem_paused.discard(user_id)


def get_paused_count() -> int:
    r = get_redis()
    if r:
        try:
            return r.scard("paused_users")
        except Exception:
            pass
    return len(_mem_paused)


# ── 全域狀態（in-memory，不需要持久化的部分） ────────────────────────────────

# 等待業主 confirm/cancel 的提示詞更新（key = admin user_id）
pending_prompt_update: dict[str, str] = {}

# 向下相容：舊程式碼用的 paused_users（代理到新函數）
class _PausedProxy:
    def add(self, uid): pause_user(uid)
    def discard(self, uid): resume_user(uid)
    def __contains__(self, uid): return is_paused(uid)
    def __len__(self): return get_paused_count()

paused_users = _PausedProxy()

# ── 管理員清單（in-memory，啟動時從 env + Sheets 載入） ───────────────────────
# 支援多管理員：逗號分隔多個 user_id
_admin_ids: set[str] = set(
    uid.strip()
    for uid in FOUNDER_LINE_USER_ID.split(",")
    if uid.strip().startswith("U")
)


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


def load_admin_ids() -> None:
    """
    從 Sheets 設定分頁讀取 ADMIN_USER_IDS（逗號分隔），
    合併環境變數中的 FOUNDER_LINE_USER_ID，更新全域 _admin_ids。
    """
    global _admin_ids
    base = set(
        uid.strip()
        for uid in FOUNDER_LINE_USER_ID.split(",")
        if uid.strip().startswith("U")
    )
    settings = read_settings()
    sheets_ids = set(
        uid.strip()
        for uid in settings.get("ADMIN_USER_IDS", "").split(",")
        if uid.strip().startswith("U")
    )
    _admin_ids = base | sheets_ids
    log.info("管理員清單已載入：%d 人 → %s", len(_admin_ids), _admin_ids)


def is_admin(user_id: str) -> bool:
    """判斷是否為管理員"""
    return user_id in _admin_ids


def add_admin(user_id: str) -> bool:
    """
    將 user_id 加入管理員清單，並同步寫入 Sheets 設定分頁（ADMIN_USER_IDS）。
    回傳是否成功寫入 Sheets。
    """
    global _admin_ids
    _admin_ids.add(user_id)
    # 同步寫入 Sheets，排除環境變數已有的 id，只存 Sheets 額外追加的
    base = set(
        uid.strip()
        for uid in FOUNDER_LINE_USER_ID.split(",")
        if uid.strip().startswith("U")
    )
    extra = _admin_ids - base
    return write_setting("ADMIN_USER_IDS", ",".join(sorted(extra)))


def remove_admin(user_id: str) -> bool:
    """
    從管理員清單移除 user_id，同步更新 Sheets。
    注意：無法移除環境變數 FOUNDER_LINE_USER_ID 中的 id。
    """
    global _admin_ids
    base = set(
        uid.strip()
        for uid in FOUNDER_LINE_USER_ID.split(",")
        if uid.strip().startswith("U")
    )
    if user_id in base:
        return False  # 環境變數設定的 admin 不允許透過指令移除
    _admin_ids.discard(user_id)
    extra = _admin_ids - base
    return write_setting("ADMIN_USER_IDS", ",".join(sorted(extra)))


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
            now_tw().isoformat(),
            user_id,
            user_text,
            reply_text,
            status,
            len(get_history(user_id)),
        ])
    except Exception as e:
        log.error("Sheets 對話記錄寫入失敗: %s", e)


def classify_escalate(trigger_text: str) -> str:
    """
    根據觸發訊息快速分類 ESCALATE 原因（不呼叫 Claude，純關鍵字比對）。
    回傳分類字串，用於 Sheets 備註欄。
    """
    t = trigger_text.lower()
    if any(k in t for k in ["費用", "價格", "多少錢", "報價", "便宜", "折扣", "優惠"]):
        return "費用詢問"
    if any(k in t for k in ["客訴", "不滿", "差", "失望", "退款", "賠", "抱怨", "爛"]):
        return "客訴／不滿"
    if any(k in t for k in ["取消", "改期", "延期", "遲到", "沒來"]):
        return "預約異動"
    if any(k in t for k in ["真人", "人工", "老闆", "館主", "負責人"]):
        return "要求真人"
    if any(k in t for k in ["商業", "活動", "大型", "合作", "企業"]):
        return "商業／複雜需求"
    return "其他"


def log_escalate(user_id: str, trigger_text: str) -> None:
    """將 ESCALATE 事件寫入例外記錄分頁（含觸發分類）"""
    sheet = get_sheet(SHEET_ESCALATE)
    if sheet is None:
        return
    try:
        category = classify_escalate(trigger_text)
        sheet.append_row([
            now_tw().isoformat(),
            user_id,
            trigger_text,
            category,
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
                    "訂", "排", "檔期", "何時", "available", "book",
                    "取消", "改期", "換日", "變更", "改時間", "重新約"]

SHEET_SLOTS   = "可預約時段"
SHEET_BOOKING = "預約記錄"


def get_available_slots() -> list[dict]:
    """
    讀取 Sheets 可預約時段（格式：日期|開始時間|結束時間|狀態）
    自動過濾今天以前的時段，只回傳未來的「可預約」時段
    """
    sheet = get_sheet(SHEET_SLOTS)
    if sheet is None:
        return []
    try:
        rows = sheet.get_all_values()
        today = now_tw().date()
        available = []
        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 4:
                continue
            raw_date, start_t, end_t, status = (
                row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()
            )
            if status != "可預約":
                continue
            # 過濾過去日期
            try:
                slot_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError:
                log.warning("Sheets 日期格式錯誤（row %d）: %r", i, raw_date)
                continue
            if slot_date < today:
                continue
            available.append({
                "row": i,
                "date": raw_date,
                "start": start_t,
                "end": end_t,
                "label": f"{raw_date} {start_t}-{end_t}",
            })
        return available
    except Exception as e:
        log.error("讀取可預約時段失敗: %s", e)
        return []


def _time_to_minutes(t: str) -> int:
    """將 HH:MM 轉成分鐘數，方便比較"""
    h, m = map(int, t.split(":"))
    return h * 60 + m


def book_slot(user_id: str, date_str: str, start_t: str, end_t: str,
              name: str, contact: str,
              shoot_type: str = "", people: str = "", notes: str = "") -> tuple[bool, str]:
    """
    原子性預約操作（workers=1 保證無 race condition）：
    支援單一時段（09:00-10:00）與跨時段範圍（09:00-12:00）。

    流程：
    1. 驗證日期不是過去
    2. 找出 date_str 當天所有落在 [start_t, end_t) 範圍內的時段列
    3. 確認全部都是「可預約」
    4. 批次將這些列的 D 欄改為「已滿」
    5. 寫入預約記錄（一筆，含拍攝類型、人數、備註）
    回傳 (success, message)
    """
    # 驗證日期
    try:
        slot_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        if slot_date < now_tw().date():
            return False, "這個日期已經過去，請選擇未來的時段。"
    except ValueError:
        return False, "日期格式有誤，請重新選擇。"

    ws_slots   = get_sheet(SHEET_SLOTS)
    ws_records = get_sheet(SHEET_BOOKING)
    if ws_slots is None or ws_records is None:
        return False, "系統暫時無法完成預約，請稍後再試"
    try:
        rows = ws_slots.get_all_values()
        start_min = _time_to_minutes(start_t)
        end_min   = _time_to_minutes(end_t)

        # 找出當天所有落在 [start_t, end_t) 範圍內的時段
        matched_rows: list[tuple[int, str, str, str]] = []  # (row_idx, start, end, status)
        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 4:
                continue
            if row[0].strip() != date_str:
                continue
            s_min = _time_to_minutes(row[1].strip())
            e_min = _time_to_minutes(row[2].strip())
            # 時段起點 >= booking start 且 終點 <= booking end
            if s_min >= start_min and e_min <= end_min:
                matched_rows.append((i, row[1].strip(), row[2].strip(), row[3].strip()))

        if not matched_rows:
            return False, f"找不到 {date_str} {start_t}-{end_t} 的可用時段，請確認時間是否正確。"

        # 確認所有符合時段都是「可預約」
        unavailable = [f"{s}-{e}" for _, s, e, st in matched_rows if st != "可預約"]
        if unavailable:
            return False, f"很抱歉，以下時段已被預約：{', '.join(unavailable)}，請選擇其他時段。"

        # 批次標記為已滿
        for row_idx, _, _, _ in matched_rows:
            ws_slots.update_cell(row_idx, 4, "已滿")

        # 寫入預約記錄（完整資訊）
        hours = (end_min - start_min) // 60
        ws_records.append_row([
            now_tw().isoformat(),
            user_id,
            name,
            contact,
            f"{date_str} {start_t}-{end_t}",
            shoot_type or "未指定",
            people or "未填",
            f"{hours} 小時",
            notes or "",
            "已確認",
        ])

        slots_count = len(matched_rows)
        log.info("預約成功：%s %s-%s（%d 時段）→ %s (%s) [%s / %s]",
                 date_str, start_t, end_t, slots_count, name, contact, shoot_type, people)
        return True, f"已為 {name} 預約 {date_str} {start_t}-{end_t} 的拍攝時段"
    except Exception as e:
        log.error("預約操作失敗: %s", e)
        return False, "預約時發生錯誤，請稍後再試。"


def cancel_booking(user_id: str, date_str: str, start_t: str, end_t: str) -> tuple[bool, str]:
    """
    取消預約：
    1. 在預約記錄找到符合的那筆（user_id + 日期時段），確認狀態為「已確認」
    2. 將預約記錄狀態改為「已取消」
    3. 還原可預約時段回「可預約」
    回傳 (success, message)
    """
    ws_slots   = get_sheet(SHEET_SLOTS)
    ws_records = get_sheet(SHEET_BOOKING)
    if ws_slots is None or ws_records is None:
        return False, "系統暫時無法完成取消，請稍後再試。"
    try:
        slot_str = f"{date_str} {start_t}-{end_t}"
        # 找預約記錄
        records = ws_records.get_all_values()
        target_rec_row = None
        for i, row in enumerate(records[1:], start=2):
            if (len(row) >= 10
                    and row[1].strip() == user_id
                    and row[4].strip() == slot_str
                    and row[9].strip() == "已確認"):
                target_rec_row = i
                break
        if target_rec_row is None:
            return False, f"找不到 {slot_str} 的已確認預約記錄，請確認日期和時間是否正確。"

        # 更新預約記錄狀態
        ws_records.update_cell(target_rec_row, 10, "已取消")

        # 還原可預約時段
        start_min = _time_to_minutes(start_t)
        end_min   = _time_to_minutes(end_t)
        slots_rows = ws_slots.get_all_values()
        restored = 0
        for i, row in enumerate(slots_rows[1:], start=2):
            if len(row) < 4 or row[0].strip() != date_str:
                continue
            s_min = _time_to_minutes(row[1].strip())
            e_min = _time_to_minutes(row[2].strip())
            if s_min >= start_min and e_min <= end_min and row[3].strip() == "已滿":
                ws_slots.update_cell(i, 4, "可預約")
                restored += 1

        log.info("取消預約成功：%s → %d 時段還原", slot_str, restored)
        return True, f"已取消 {slot_str} 的預約，時段已重新開放。"
    except Exception as e:
        log.error("取消預約失敗: %s", e)
        return False, "取消時發生錯誤，請稍後再試。"


def change_booking(user_id: str,
                   old_date: str, old_start: str, old_end: str,
                   new_date: str, new_start: str, new_end: str,
                   name: str = "", contact: str = "",
                   shoot_type: str = "", people: str = "", notes: str = "") -> tuple[bool, str]:
    """
    變更預約（原子性）：
    1. 確認新時段全數可預約
    2. 取消舊預約（還原時段）
    3. 建立新預約（鎖定新時段）
    全部成功才 commit；任一失敗回傳錯誤，不留半途狀態。
    回傳 (success, message)
    """
    ws_slots   = get_sheet(SHEET_SLOTS)
    ws_records = get_sheet(SHEET_BOOKING)
    if ws_slots is None or ws_records is None:
        return False, "系統暫時無法完成變更，請稍後再試。"
    try:
        # ── Step 1：確認新時段全數可預約 ─────────────────────────────────────
        try:
            new_slot_date = datetime.strptime(new_date, "%Y-%m-%d").date()
            if new_slot_date < now_tw().date():
                return False, "新時段的日期已經過去，請選擇未來的時段。"
        except ValueError:
            return False, "新時段日期格式有誤。"

        new_start_min = _time_to_minutes(new_start)
        new_end_min   = _time_to_minutes(new_end)
        slots_rows = ws_slots.get_all_values()

        new_matched: list[tuple[int, str]] = []  # (row_idx, status)
        for i, row in enumerate(slots_rows[1:], start=2):
            if len(row) < 4 or row[0].strip() != new_date:
                continue
            s_min = _time_to_minutes(row[1].strip())
            e_min = _time_to_minutes(row[2].strip())
            if s_min >= new_start_min and e_min <= new_end_min:
                new_matched.append((i, row[3].strip()))

        if not new_matched:
            return False, f"找不到 {new_date} {new_start}-{new_end} 的時段，請確認是否正確。"

        unavailable = [i for i, st in new_matched if st != "可預約"]
        if unavailable:
            return False, f"很抱歉，{new_date} {new_start}-{new_end} 已有部分時段被預約，請選擇其他時段。"

        # ── Step 2：找並更新舊預約記錄 ───────────────────────────────────────
        old_slot_str = f"{old_date} {old_start}-{old_end}"
        records = ws_records.get_all_values()
        target_rec_row = None
        rec_name, rec_contact, rec_type, rec_people, rec_notes = name, contact, shoot_type, people, notes
        for i, row in enumerate(records[1:], start=2):
            if (len(row) >= 10
                    and row[1].strip() == user_id
                    and row[4].strip() == old_slot_str
                    and row[9].strip() == "已確認"):
                target_rec_row = i
                # 繼承舊預約的資訊（若呼叫端沒傳）
                if not rec_name:    rec_name    = row[2].strip()
                if not rec_contact: rec_contact = row[3].strip()
                if not rec_type:    rec_type    = row[5].strip()
                if not rec_people:  rec_people  = row[6].strip()
                if not rec_notes:   rec_notes   = row[8].strip()
                break
        if target_rec_row is None:
            return False, f"找不到 {old_slot_str} 的已確認預約記錄，請確認日期和時間是否正確。"

        # ── Step 3：還原舊時段 ────────────────────────────────────────────────
        old_start_min = _time_to_minutes(old_start)
        old_end_min   = _time_to_minutes(old_end)
        for i, row in enumerate(slots_rows[1:], start=2):
            if len(row) < 4 or row[0].strip() != old_date:
                continue
            s_min = _time_to_minutes(row[1].strip())
            e_min = _time_to_minutes(row[2].strip())
            if s_min >= old_start_min and e_min <= old_end_min and row[3].strip() == "已滿":
                ws_slots.update_cell(i, 4, "可預約")

        # ── Step 4：鎖定新時段 ────────────────────────────────────────────────
        for row_idx, _ in new_matched:
            ws_slots.update_cell(row_idx, 4, "已滿")

        # ── Step 5：更新預約記錄（舊筆改為已取消，新增一筆）──────────────────
        ws_records.update_cell(target_rec_row, 10, "已取消（已改期）")
        new_slot_str = f"{new_date} {new_start}-{new_end}"
        hours = (new_end_min - new_start_min) // 60
        ws_records.append_row([
            now_tw().isoformat(),
            user_id,
            rec_name,
            rec_contact,
            new_slot_str,
            rec_type or "未指定",
            rec_people or "未填",
            f"{hours} 小時",
            rec_notes or "",
            "已確認",
        ])

        log.info("變更預約成功：%s → %s [%s]", old_slot_str, new_slot_str, rec_name)
        return True, f"已將 {rec_name} 的預約從 {old_slot_str} 改為 {new_slot_str}。"
    except Exception as e:
        log.error("變更預約失敗: %s", e)
        return False, "變更時發生錯誤，請稍後再試。"


def is_booking_related(text: str) -> bool:
    return any(kw in text for kw in BOOKING_KEYWORDS)


def is_booking_context(user_id: str, user_message: str) -> bool:
    """
    判斷是否需要注入預約指示：
    - 當前訊息含預約關鍵字，或
    - 近 10 則對話歷史中有含預約關鍵字的 user 訊息
    （確保「傳電話號碼」這種最後一步也能取得預約指示）
    """
    if is_booking_related(user_message):
        return True
    history = get_history(user_id)
    recent = history[-10:] if len(history) > 10 else history
    for msg in recent:
        if msg.get("role") == "user" and is_booking_related(msg.get("content", "")):
            return True
    return False


def get_dynamic_system_prompt(user_id: str, user_message: str) -> str:
    """根據訊息內容動態組合 SYSTEM_PROMPT，注入 user_id 與預約時段"""
    base = get_system_prompt()

    # 永遠注入當前用戶的 LINE user_id，確保 AI 能正確生成 ESCALATE/NOTIFY tag
    user_ctx = (
        f"\n\n# 當前用戶資訊\n"
        f"當前對話的用戶 LINE ID：{user_id}\n\n"
        f"# 升級標記使用規則\n\n"
        f"## ESCALATE（硬升級）：用戶將被暫停 AI 回應，等待業主人工介入\n"
        f"觸發條件：客訴／強烈不滿、明確要求真人服務、費用議價、商業合作等需要業主直接處理的情況\n"
        f"格式（放在回覆末尾，用戶看不到）：ESCALATE:user_id:{user_id}\n\n"
        f"## NOTIFY（軟通知）：僅通知業主，用戶的 AI 回應正常繼續\n"
        f"觸發條件：AI 無法回答的知識空缺（如知識庫沒有的特殊服務、超出 AI 判斷範圍的細節問題）\n"
        f"格式（放在回覆末尾，用戶看不到）：NOTIFY:reason:[簡短說明原因，30字以內]\n"
        f"注意：NOTIFY 後用戶仍可繼續與 AI 對話，不需暫停\n\n"
        f"重要：兩種標記不可同時使用；優先判斷是否需要 ESCALATE，否則才考慮 NOTIFY"
    )
    base = base + user_ctx

    if not is_booking_context(user_id, user_message):
        return base

    slots = get_available_slots()
    if slots:
        # 只列出最近 30 筆避免 prompt 太長
        display = slots[:30]
        slots_text = "\n".join(
            f"- {s['label']}" for s in display
        )
        more = f"\n（還有 {len(slots)-30} 個時段，客人可指定日期查詢）" if len(slots) > 30 else ""
        booking_ctx = f"""

# 預約管理系統說明

## 目前可預約時段（即時資料，請以此為準）

{slots_text}{more}

上方每筆為一個小時單位。客人可選擇連續多個時段（例如選 09:00-12:00，系統會自動鎖定 09:00-10:00、10:00-11:00、11:00-12:00 三個時段）。

# 預約確認流程

引導客人完成以下三步驟後才執行預約：
步驟一：確認拍攝類型（親子、個人、全家福、情侶）
步驟二：確認拍攝時段——客人可選單一小時或連續多小時（start 到 end 必須都在上方清單內）
步驟三：請客人提供姓名和聯絡方式（電話或 LINE ID）

三項資訊齊全後，在回覆末尾加入預約標記（客人看不到此標記）：
BOOK:date:[YYYY-MM-DD]:start:[HH:MM]:end:[HH:MM]:name:[姓名]:contact:[聯絡方式]:type:[拍攝類型]:people:[人數描述]:notes:[其他備註]

範例（親子寫真）：BOOK:date:2026-05-10:start:09:00:end:12:00:name:王小明:contact:0912345678:type:親子寫真:people:2大人3小孩:notes:希望在公園拍
範例（個人寫真）：BOOK:date:2026-05-10:start:14:00:end:15:00:name:李小花:contact:0987654321:type:個人寫真:people:1人:notes:

注意：
- 嚴格使用 YYYY-MM-DD 格式填寫日期，HH:MM 格式填寫時間
- start 和 end 必須完全落在上方可預約清單的邊界上
- 不可選超出清單範圍的時間（例如 08:00 或 19:00）
- type 填對話中提到的拍攝類型（親子寫真／個人寫真／全家福／情侶寫真）
- people 填人數描述（例：2大人5小孩、1人、2人）
- notes 填對話中其他有用資訊（特殊需求、備註等），無則留空
- 確認姓名、聯絡方式、時段三項齊全後才輸出 BOOK 標記，不可提前輸出
- 輸出 BOOK 標記時，同時用文字向客人確認完整預約細節

## 取消預約流程

當客人要求取消預約時：
1. 請客人確認要取消的日期和時段
2. 確認後在回覆末尾加入取消標記：
CANCEL:date:[YYYY-MM-DD]:start:[HH:MM]:end:[HH:MM]

範例：CANCEL:date:2026-05-11:start:09:00:end:12:00

注意：
- 只能取消「已確認」狀態的預約
- 取消後時段自動還原為可預約

## 變更（改期）預約流程

當客人要求改期時：
1. 確認原本的預約日期時段
2. 從可預約時段中選擇新時段（與新預約流程相同，需確認新時段存在）
3. 確認後在回覆末尾加入變更標記：
CHANGE:old_date:[YYYY-MM-DD]:old_start:[HH:MM]:old_end:[HH:MM]:new_date:[YYYY-MM-DD]:new_start:[HH:MM]:new_end:[HH:MM]

範例：CHANGE:old_date:2026-05-11:old_start:09:00:old_end:12:00:new_date:2026-05-15:new_start:14:00:new_end:17:00

注意：
- 原時段自動還原，新時段自動鎖定，一次完成
- 新時段必須從上方可預約清單中選取"""
        return base + booking_ctx
    else:
        return base + """

# 目前可預約時段

目前所有時段均已預約。
請告知客人目前無可用時段，並詢問是否要留下姓名和聯絡方式，有空檔時主動通知。"""


# ── Claude API ────────────────────────────────────────────────────────────────

def call_claude(user_id: str, user_message: str) -> tuple[str, str]:
    """
    呼叫 Claude API，維護對話歷史（Redis 持久化，重啟不遺失）。
    回傳 (reply_text, status)，status = OK / ERROR / ESCALATE / NOTIFY
    """
    history = get_history(user_id)
    history.append({"role": "user", "content": user_message})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]

    status = "OK"
    try:
        response = claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=get_dynamic_system_prompt(user_id, user_message),   # 動態注入時段（含歷史判斷）
            messages=history,
        )
        reply_text = response.content[0].text
    except Exception as e:
        log.error("Claude API error: %s", e)
        reply_text = "目前系統有點忙，請稍後再試。"
        status = "ERROR"

    # ── 偵測 BOOK 標記 ────────────────────────────────────────────────────────
    book_match = re.search(
        r"BOOK:date:(\d{4}-\d{2}-\d{2}):start:(\d{2}:\d{2}):end:(\d{2}:\d{2})"
        r":name:([^:]+):contact:([^:\s\n]+)"
        r"(?::type:([^:]*?))?(?::people:([^:]*?))?(?::notes:([^\n]*))?(?:\s|$)",
        reply_text,
    )
    if book_match:
        g = [x.strip() if x else "" for x in book_match.groups()]
        b_date, b_start, b_end, b_name, b_contact = g[0], g[1], g[2], g[3], g[4]
        b_type, b_people, b_notes = g[5], g[6], g[7]
        # 移除標記，用戶看不到
        reply_text = re.sub(r"\s*BOOK:date:[^\n]+", "", reply_text).strip()
        # 執行預約
        success, booking_msg = book_slot(
            user_id, b_date, b_start, b_end, b_name, b_contact,
            shoot_type=b_type, people=b_people, notes=b_notes,
        )
        if success:
            status = "BOOKED"
            for admin_id in _admin_ids:
                notify_lines = [
                    f"📸 新預約通知",
                    f"姓名：{b_name}",
                    f"聯絡：{b_contact}",
                    f"日期：{b_date} {b_start}-{b_end}",
                    f"類型：{b_type or '未指定'}",
                    f"人數：{b_people or '未填'}",
                ]
                if b_notes:
                    notify_lines.append(f"備註：{b_notes}")
                notify_lines.append(f"用戶ID：{user_id}")
                line_push(admin_id, "\n".join(notify_lines))
            log.info("BOOKED: %s %s-%s → %s [%s/%s]", b_date, b_start, b_end, b_name, b_type, b_people)
        else:
            status = "BOOK_FAIL"
            reply_text = booking_msg
            log.warning("BOOK_FAIL: %s %s-%s", b_date, b_start, b_end)

    # ── 偵測 CANCEL 標記 ─────────────────────────────────────────────────────
    cancel_match = re.search(
        r"CANCEL:date:(\d{4}-\d{2}-\d{2}):start:(\d{2}:\d{2}):end:(\d{2}:\d{2})",
        reply_text,
    )
    if cancel_match and not book_match:  # BOOK 優先，避免衝突
        c_date, c_start, c_end = [g.strip() for g in cancel_match.groups()]
        reply_text = re.sub(r"\s*CANCEL:date:[^\n]+", "", reply_text).strip()
        success, cancel_msg = cancel_booking(user_id, c_date, c_start, c_end)
        if success:
            status = "CANCELLED"
            for admin_id in _admin_ids:
                line_push(admin_id,
                    f"🗓 預約已取消\n日期：{c_date} {c_start}-{c_end}\n用戶ID：{user_id}")
            log.info("CANCELLED: %s %s-%s by %s", c_date, c_start, c_end, user_id)
        else:
            status = "CANCEL_FAIL"
            reply_text = cancel_msg
            log.warning("CANCEL_FAIL: %s %s-%s", c_date, c_start, c_end)

    # ── 偵測 CHANGE 標記 ─────────────────────────────────────────────────────
    change_match = re.search(
        r"CHANGE:old_date:(\d{4}-\d{2}-\d{2}):old_start:(\d{2}:\d{2}):old_end:(\d{2}:\d{2})"
        r":new_date:(\d{4}-\d{2}-\d{2}):new_start:(\d{2}:\d{2}):new_end:(\d{2}:\d{2})",
        reply_text,
    )
    if change_match and not book_match and not cancel_match:
        g = [x.strip() for x in change_match.groups()]
        c_old_date, c_old_start, c_old_end = g[0], g[1], g[2]
        c_new_date, c_new_start, c_new_end = g[3], g[4], g[5]
        reply_text = re.sub(r"\s*CHANGE:old_date:[^\n]+", "", reply_text).strip()
        success, change_msg = change_booking(
            user_id, c_old_date, c_old_start, c_old_end,
            c_new_date, c_new_start, c_new_end,
        )
        if success:
            status = "CHANGED"
            for admin_id in _admin_ids:
                line_push(admin_id,
                    f"🔄 預約已變更\n原：{c_old_date} {c_old_start}-{c_old_end}\n"
                    f"新：{c_new_date} {c_new_start}-{c_new_end}\n用戶ID：{user_id}")
            log.info("CHANGED: %s %s-%s → %s %s-%s",
                     c_old_date, c_old_start, c_old_end, c_new_date, c_new_start, c_new_end)
        else:
            status = "CHANGE_FAIL"
            reply_text = change_msg
            log.warning("CHANGE_FAIL: %s → %s", f"{c_old_date} {c_old_start}-{c_old_end}",
                        f"{c_new_date} {c_new_start}-{c_new_end}")

    # ── 偵測 NOTIFY 標記（軟通知，不暫停用戶）────────────────────────────────
    notify_match = re.search(r"NOTIFY:reason:([^\n]{1,60})", reply_text)
    if notify_match and status == "OK":
        status = "NOTIFY"
        _notify_reason = notify_match.group(1).strip()
        reply_text = re.sub(r"\s*NOTIFY:reason:[^\n]+", "", reply_text).strip()
        # handle_notify 在 webhook handler 中呼叫（需要 trigger_text）
        # 這裡先把原因存進 thread-safe 方式：用 return 值帶出
        # 做法：在 status 欄位帶入 reason，格式 "NOTIFY:[reason]"
        status = f"NOTIFY:{_notify_reason}"

    # ── 偵測 ESCALATE 標記 ────────────────────────────────────────────────────
    # 主要格式：ESCALATE:user_id:Uxxxxxx（AI 從注入的 user_id 填入）
    # 降級格式：任何含 ESCALATE:user_id: 的輸出（AI 格式錯誤時也能捕捉）
    escalate_match = re.search(r"ESCALATE:user_id:(\S+)", reply_text)
    if escalate_match:
        status = "ESCALATE"
        # 不論 AI 填了什麼 user_id，一律用 webhook 傳入的真實 user_id
        reply_text = re.sub(r"\s*ESCALATE:user_id:\S+", "", reply_text).strip()

    history.append({"role": "assistant", "content": reply_text})
    set_history(user_id, history)   # 寫回 Redis（持久化）
    # 對話記錄 Sheets 的狀態欄只寫主類型（NOTIFY:[reason] → NOTIFY）
    log_status = status.split(":")[0] if status.startswith("NOTIFY:") else status
    log_conversation(user_id, user_message, reply_text, log_status)
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
    """從 Sheets 讀取最近 N 天的對話記錄（以自然日 00:00 為邊界）"""
    sheet = get_sheet(SHEET_LOG)
    if sheet is None:
        return []
    try:
        rows = sheet.get_all_values()
        # 自然日邊界：從 N-1 天前的 00:00:00 起（days=1 → 今天 00:00；days=7 → 6 天前 00:00）
        today_midnight = now_tw().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = today_midnight - timedelta(days=days - 1)
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
    # 顯示實際資料起始日期
    today_midnight = now_tw().replace(hour=0, minute=0, second=0, microsecond=0)
    data_start = today_midnight - timedelta(days=days - 1)
    header = f"[{label}報告] {data_start.strftime('%m/%d')}–{now_tw().strftime('%m/%d %H:%M')}\n對話：{len(rows)} 筆 / 用戶：{unique_users} 人 / ESCALATE：{escalate_count} 件\n\n"
    return header + report


# ── 模組 7-5：每日自動報告（APScheduler） ────────────────────────────────────

def send_daily_report() -> None:
    """APScheduler 每日定時呼叫，push 今日報告給所有管理員"""
    targets = list(_admin_ids) or ([FOUNDER_LINE_USER_ID] if FOUNDER_LINE_USER_ID else [])
    if not targets:
        log.warning("沒有設定管理員，無法發送每日報告")
        return
    log.info("每日報告觸發中，推送給 %d 位管理員...", len(targets))
    report = analyze_conversations(days=1)
    for admin_id in targets:
        line_push(admin_id, report)


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
【時段管理】
slots [日期] — 查看某天時段（例：slots 2026-05-10）
block [日期] [開始]-[結束] — 封鎖時段（例：block 2026-05-10 14:00-17:00）
unblock [日期] [開始]-[結束] — 還原時段（例：unblock 2026-05-10 14:00-17:00）

【系統管理】
status — 系統狀態
reload — 重新載入知識庫與設定
report — 今日對話摘要
report 週 — 本週報告
report 月 — 本月報告

【AI 設定】
update prompt [內容] — 更新提示詞（有確認流程）
confirm — 確認提示詞更新
cancel — 取消提示詞更新

【客服操作】
pause [user_id] — 暫停特定用戶的 AI 回應
resume [user_id] — 恢復特定用戶的 AI 回應
reply [user_id] [訊息] — 代傳訊息給用戶

【管理員】
listadmin — 查看所有管理員
addadmin [user_id] — 新增管理員
removeadmin [user_id] — 移除管理員
testnotify — 測試推播通知（確認所有管理員都能收到）
help — 顯示此說明"""


def handle_admin(user_id: str, text: str) -> str:
    """處理業主 admin 指令，回傳要推播給業主的回應文字"""
    text_lower = text.strip().lower()
    original = text.strip()

    # ── status ────────────────────────────────────────────────────────────────
    if text_lower == "status":
        r = get_redis()
        active = r.dbsize() if r else "N/A（in-memory）"
        paused = get_paused_count()
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
            f"系統狀態 {now_tw().strftime('%m/%d %H:%M')}\n"
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
        load_admin_ids()
        return f"已重新載入知識庫、設定與管理員清單。\nSYSTEM_PROMPT 長度：{len(get_system_prompt())} 字\n管理員：{len(_admin_ids)} 人"

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

    # ── listadmin ─────────────────────────────────────────────────────────────
    if text_lower == "listadmin":
        ids = sorted(_admin_ids)
        base = set(
            uid.strip()
            for uid in FOUNDER_LINE_USER_ID.split(",")
            if uid.strip().startswith("U")
        )
        lines = []
        for uid in ids:
            tag = "（環境變數）" if uid in base else "（Sheets）"
            lines.append(f"{uid} {tag}")
        return f"目前管理員（{len(ids)} 人）：\n" + "\n".join(lines) if lines else "尚無管理員設定。"

    # ── addadmin [user_id] ────────────────────────────────────────────────────
    if text_lower.startswith("addadmin "):
        target = original[9:].strip()
        if not target.startswith("U"):
            return f"user_id 格式錯誤（應為 Uxxxxxx），收到：{target}"
        if target in _admin_ids:
            return f"{target} 已經是管理員。"
        ok = add_admin(target)
        return f"已新增管理員：{target}" + ("" if ok else "\n（注意：Sheets 同步失敗，重啟後將失效，請手動更新 Sheets ADMIN_USER_IDS）")

    # ── removeadmin [user_id] ─────────────────────────────────────────────────
    if text_lower.startswith("removeadmin "):
        target = original[12:].strip()
        if not target.startswith("U"):
            return f"user_id 格式錯誤（應為 Uxxxxxx），收到：{target}"
        result = remove_admin(target)
        if result is False:
            return f"{target} 是透過環境變數設定的管理員，無法透過指令移除。請在 Railway 修改 FOUNDER_LINE_USER_ID。"
        return f"已移除管理員：{target}"

    # ── testnotify — 測試推播給所有管理員 ───────────────────────────────────
    if text_lower == "testnotify":
        targets = sorted(_admin_ids)
        if not targets:
            return "⚠️ 管理員清單為空！請先設定 FOUNDER_LINE_USER_ID 或用 addadmin 新增。"
        test_msg = (
            f"✅ 推播測試成功\n"
            f"時間：{now_tw().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"管理員數：{len(targets)} 人\n"
            f"NOTIFY/ESCALATE 通知正常運作中。"
        )
        success, fail = 0, 0
        for tid in targets:
            try:
                line_push(tid, test_msg)
                success += 1
            except Exception:
                fail += 1
        return f"測試推播完成：{success} 成功 / {fail} 失敗\n目標：{', '.join(targets)}"

    # ── slots [日期] — 查看某天時段狀態 ─────────────────────────────────────
    if text_lower.startswith("slots"):
        parts = original.split(None, 1)
        date_arg = parts[1].strip() if len(parts) > 1 else ""
        if not date_arg:
            return "格式：slots [日期]\n範例：slots 2026-05-10"
        ws = get_sheet(SHEET_SLOTS)
        if ws is None:
            return "無法讀取時段資料，請稍後再試。"
        try:
            rows = ws.get_all_values()
            day_rows = [r for r in rows[1:] if len(r) >= 4 and r[0].strip() == date_arg]
            if not day_rows:
                return f"{date_arg} 沒有任何時段資料。"
            lines = [f"📅 {date_arg} 時段狀態："]
            for r in day_rows:
                icon = "✅" if r[3].strip() == "可預約" else "🔴"
                lines.append(f"{icon} {r[1]}-{r[2]}  {r[3]}")
            avail = sum(1 for r in day_rows if r[3].strip() == "可預約")
            lines.append(f"\n可預約：{avail} 筆 / 共 {len(day_rows)} 筆")
            return "\n".join(lines)
        except Exception as e:
            log.error("slots 指令失敗: %s", e)
            return "讀取時段失敗，請稍後再試。"

    # ── block [日期] [開始]-[結束] — 封鎖時段 ──────────────────────────────
    if text_lower.startswith("block "):
        parts = original[6:].split(None, 1)
        if len(parts) < 2 or "-" not in parts[1]:
            return "格式：block [日期] [開始]-[結束]\n範例：block 2026-05-10 14:00-17:00"
        date_arg = parts[0].strip()
        times = parts[1].strip().split("-", 1)
        if len(times) != 2:
            return "時間格式錯誤，請用 HH:MM-HH:MM"
        start_arg, end_arg = times[0].strip(), times[1].strip()
        ws = get_sheet(SHEET_SLOTS)
        if ws is None:
            return "無法存取時段資料。"
        try:
            rows = ws.get_all_values()
            start_min = _time_to_minutes(start_arg)
            end_min   = _time_to_minutes(end_arg)
            blocked = []
            already = []
            for i, row in enumerate(rows[1:], start=2):
                if len(row) < 4 or row[0].strip() != date_arg:
                    continue
                s_min = _time_to_minutes(row[1].strip())
                e_min = _time_to_minutes(row[2].strip())
                if s_min >= start_min and e_min <= end_min:
                    if row[3].strip() == "可預約":
                        ws.update_cell(i, 4, "已滿")
                        blocked.append(f"{row[1]}-{row[2]}")
                    else:
                        already.append(f"{row[1]}-{row[2]}")
            if not blocked and not already:
                return f"找不到 {date_arg} {start_arg}-{end_arg} 範圍內的時段，請確認日期和時間。"
            msg = f"🔴 已封鎖 {date_arg} {start_arg}-{end_arg}：\n" + "\n".join(f"  • {t}" for t in blocked)
            if already:
                msg += f"\n（已滿，略過：{', '.join(already)}）"
            log.info("Admin BLOCK: %s %s-%s → %d 筆", date_arg, start_arg, end_arg, len(blocked))
            return msg
        except Exception as e:
            log.error("block 指令失敗: %s", e)
            return "封鎖失敗，請稍後再試。"

    # ── unblock [日期] [開始]-[結束] — 還原時段 ────────────────────────────
    if text_lower.startswith("unblock "):
        parts = original[8:].split(None, 1)
        if len(parts) < 2 or "-" not in parts[1]:
            return "格式：unblock [日期] [開始]-[結束]\n範例：unblock 2026-05-10 14:00-17:00"
        date_arg = parts[0].strip()
        times = parts[1].strip().split("-", 1)
        if len(times) != 2:
            return "時間格式錯誤，請用 HH:MM-HH:MM"
        start_arg, end_arg = times[0].strip(), times[1].strip()
        ws = get_sheet(SHEET_SLOTS)
        if ws is None:
            return "無法存取時段資料。"
        try:
            rows = ws.get_all_values()
            start_min = _time_to_minutes(start_arg)
            end_min   = _time_to_minutes(end_arg)
            restored = []
            skipped  = []
            for i, row in enumerate(rows[1:], start=2):
                if len(row) < 4 or row[0].strip() != date_arg:
                    continue
                s_min = _time_to_minutes(row[1].strip())
                e_min = _time_to_minutes(row[2].strip())
                if s_min >= start_min and e_min <= end_min:
                    if row[3].strip() == "已滿":
                        ws.update_cell(i, 4, "可預約")
                        restored.append(f"{row[1]}-{row[2]}")
                    else:
                        skipped.append(f"{row[1]}-{row[2]}")
            if not restored and not skipped:
                return f"找不到 {date_arg} {start_arg}-{end_arg} 範圍內的時段。"
            msg = f"✅ 已還原 {date_arg} {start_arg}-{end_arg}：\n" + "\n".join(f"  • {t}" for t in restored)
            if skipped:
                msg += f"\n（已是可預約，略過：{', '.join(skipped)}）"
            log.info("Admin UNBLOCK: %s %s-%s → %d 筆", date_arg, start_arg, end_arg, len(restored))
            return msg
        except Exception as e:
            log.error("unblock 指令失敗: %s", e)
            return "還原失敗，請稍後再試。"

    # ── 未知指令 ──────────────────────────────────────────────────────────────
    return f"不認識這個指令。輸入 help 查看可用指令。\n（收到：{original[:50]}）"


# ── 模組 7-7：升級通知（NOTIFY 軟通知 + ESCALATE 硬升級）────────────────────

def handle_notify(notify_user_id: str, trigger_text: str, reason: str) -> None:
    """
    NOTIFY 軟通知流程：
    1. 寫入 Sheets 例外記錄（觸發分類：知識空缺）
    2. Push 通知所有管理員（提示 AI 無法回答，供補充知識庫）
    3. 用戶的 AI 回應正常送出，不暫停
    """
    sheet = get_sheet(SHEET_ESCALATE)
    if sheet:
        try:
            sheet.append_row([
                now_tw().isoformat(),
                notify_user_id,
                trigger_text[:200],
                "知識空缺",
                "未處理",
                "",
            ])
        except Exception as e:
            log.error("NOTIFY 寫入例外記錄失敗: %s", e)

    notify_msg = (
        f"💬 知識空缺通知\n"
        f"AI 無法完整回答此問題，建議補充知識庫\n"
        f"原因：{reason}\n"
        f"觸發訊息：{trigger_text[:80]}\n"
        f"用戶ID：{notify_user_id}\n\n"
        f"（用戶 AI 回應已正常送出，無需人工介入）\n"
        f"若需回覆可用：reply {notify_user_id} [你的補充]"
    )
    for admin_id in _admin_ids:
        line_push(admin_id, notify_msg)
    log.info("NOTIFY: user %s [知識空缺] reason=%s → notified %d admin(s)",
             notify_user_id, reason[:40], len(_admin_ids))


def handle_escalate(escalate_user_id: str, trigger_text: str) -> None:
    """
    ESCALATE 升級流程：
    1. 暫停該用戶的 AI 回應
    2. 寫入 Sheets 例外記錄（含觸發分類）
    3. Push 通知所有管理員
    """
    paused_users.add(escalate_user_id)
    category = classify_escalate(trigger_text)
    log_escalate(escalate_user_id, trigger_text)

    notify = (
        f"⚠️ 需要人工介入\n"
        f"分類：{category}\n"
        f"觸發訊息：{trigger_text[:80]}\n"
        f"用戶ID：{escalate_user_id}\n\n"
        f"AI 回應已暫停，可用指令：\n"
        f"reply {escalate_user_id} [你的回覆]\n"
        f"resume {escalate_user_id}"
    )
    for admin_id in _admin_ids:
        line_push(admin_id, notify)
    log.info("ESCALATE: user %s [%s] → paused + notified %d admin(s)",
             escalate_user_id, category, len(_admin_ids))


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

            # ── 一次性密碼認領（任何人皆可觸發，成功後加入管理員）────────────
            if (ADMIN_SETUP_PASSWORD
                    and user_text.strip().lower().startswith("admin ")
                    and not is_admin(user_id)):
                pw_input = user_text.strip()[6:].strip()
                if pw_input == ADMIN_SETUP_PASSWORD:
                    add_admin(user_id)
                    line_reply(reply_token,
                        "管理員身份已確認，歡迎使用管理模式。\n傳送 help 查看可用指令。")
                    log.info("新管理員透過密碼認領：%s", user_id)
                else:
                    line_reply(reply_token, "密碼錯誤。")
                    log.warning("密碼認領失敗（錯誤密碼）：%s", user_id)
                # 密碼訊息不寫入 Sheets，直接 continue
                continue

            # ── Admin 模式（模組 7-3）────────────────────────────────────────
            if is_admin(user_id):
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

            # ── NOTIFY 處理（模組 7-7，軟通知）─────────────────────────────────
            if status.startswith("NOTIFY:"):
                _reason = status[len("NOTIFY:"):]
                handle_notify(user_id, user_text, _reason)

            # ── ESCALATE 處理（模組 7-7，硬升級）────────────────────────────────
            elif status == "ESCALATE":
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
            clear_history(user_id)
            resume_user(user_id)
            log.info("Unfollowed: %s", user_id)

    return "OK", 200


# ── 健康檢查 ──────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    r = get_redis()
    redis_ok = False
    if r:
        try:
            r.ping()
            redis_ok = True
        except Exception:
            pass
    return {
        "status": "ok",
        "time": now_tw().isoformat(),
        "redis": "connected" if redis_ok else "in-memory fallback",
        "paused_users": get_paused_count(),
        "system_prompt_length": len(get_system_prompt()),
    }, 200


# ── 管理 API（需帶 token 驗證）────────────────────────────────────────────────

@app.route("/admin/resume/<user_id>", methods=["POST"])
def admin_resume(user_id: str):
    """解鎖被暫停的用戶，供緊急狀況使用"""
    token = request.headers.get("X-Admin-Token", "")
    expected = ADMIN_SETUP_PASSWORD or LINE_CHANNEL_SECRET[:16]
    if not token or token != expected:
        return {"error": "unauthorized"}, 401
    paused_users.discard(user_id)
    log.info("HTTP admin resume: %s", user_id)
    return {"resumed": user_id, "paused_count": len(paused_users)}, 200


@app.route("/admin/status", methods=["GET"])
def admin_status():
    """查看目前暫停用戶清單（需 token）"""
    token = request.headers.get("X-Admin-Token", "")
    expected = ADMIN_SETUP_PASSWORD or LINE_CHANNEL_SECRET[:16]
    if not token or token != expected:
        return {"error": "unauthorized"}, 401
    r = get_redis()
    paused_list = list(r.smembers("paused_users")) if r else list(_mem_paused)
    return {
        "paused_users": paused_list,
        "admin_ids": list(_admin_ids),
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

    # 載入管理員清單（env + Sheets）
    load_admin_ids()

    # 啟動每日報告排程
    init_scheduler()

    log.info("LINE OA AI Server 啟動中... http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
else:
    # gunicorn 啟動時也要初始化
    load_system_prompt()
    load_admin_ids()
    init_scheduler()
