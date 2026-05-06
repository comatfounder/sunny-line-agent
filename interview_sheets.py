"""
Interview Sheets — Google Sheets 訪談記錄操作模組
LINE OA AI 通用模組

負責：
  1. 初始化訪談用的 Sheets 分頁（訪談記錄、問題庫、行銷活動、視覺素材）
  2. 逐題寫入業主答案
  3. 寫入生成的知識庫初稿

分頁設計：
  - 訪談記錄：Q# / 模組 / 題目 / 業主原始回答 / AI 整理摘要 / 回答時間 / 狀態
  - 問題庫：   Q# / 模組代號 / 模組名稱 / 題目 / 是否必答 / 狀態
  - 知識庫：   Intent類型 / 顧客問題 / 標準答案 / 最後更新（訪談生成後自動填入）

版本：v1.0
建立日期：2026-05-06
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

TZ_TW = timezone(timedelta(hours=8))


def now_tw_str() -> str:
    return datetime.now(tz=TZ_TW).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M")


# ── 分頁名稱常數 ──────────────────────────────────────────────────────────────

SHEET_INTERVIEW_LOG  = "訪談記錄"
SHEET_QUESTION_BANK  = "問題庫"
SHEET_KB             = "知識庫"
SHEET_MARKETING      = "行銷活動"
SHEET_MEDIA          = "視覺素材"


# ── 公開函數 ──────────────────────────────────────────────────────────────────

def init_interview_sheets(
    spreadsheet,          # gspread Spreadsheet 物件
    questions: List[Dict],
) -> None:
    """
    初始化所有訪談相關分頁。
    - 若分頁已存在，跳過（不覆蓋現有資料）。
    - 若分頁不存在，建立並寫入標題列與問題庫。
    """
    _ensure_sheet(spreadsheet, SHEET_INTERVIEW_LOG, _interview_log_headers())
    _ensure_sheet(spreadsheet, SHEET_MARKETING,     _marketing_headers())
    _ensure_sheet(spreadsheet, SHEET_MEDIA,         _media_headers())
    _populate_question_bank(spreadsheet, questions)
    log.info("訪談分頁初始化完成")


def write_answer(
    spreadsheet,
    question:   Dict,
    raw_answer: str,
    summary:    str,
) -> None:
    """
    將單題業主答案寫入「訪談記錄」分頁，並更新「問題庫」的狀態欄。
    """
    try:
        ws = spreadsheet.worksheet(SHEET_INTERVIEW_LOG)
        ws.append_row([
            question["id"],                    # Q#
            question["module"],                # 模組代號
            question["module_name"],           # 模組名稱
            question["question"],              # 題目
            raw_answer,                        # 業主原始回答
            summary,                           # AI 整理摘要
            now_tw_str(),                      # 回答時間
            "✅ 完成" if raw_answer != "(跳過)" else "⏭ 跳過",  # 狀態
        ], value_input_option="USER_ENTERED")

        # 同步更新問題庫的狀態
        _update_question_status(spreadsheet, question["id"], "✅" if raw_answer != "(跳過)" else "⏭")
    except Exception as e:
        log.error("write_answer 失敗：%s", e)


def write_kb(
    spreadsheet,
    kb_items: List[Dict],
) -> None:
    """
    將知識庫條目寫入「知識庫」分頁（追加，不覆蓋）。
    如果知識庫分頁只有標題列，則直接追加；
    如果已有內容，則在尾端追加並加上「—— 訪談生成 {日期} ——」分隔行。
    """
    try:
        ws = _ensure_sheet(spreadsheet, SHEET_KB, _kb_headers())
        all_rows = ws.get_all_values()

        if len(all_rows) > 1:
            # 已有資料，加分隔行
            ws.append_row(
                [f"—— 訪談生成 {now_tw_str()} ——", "", "", ""],
                value_input_option="USER_ENTERED",
            )

        for item in kb_items:
            ws.append_row([
                item.get("intent", ""),
                item.get("question", ""),
                item.get("answer", ""),
                now_tw_str(),
            ], value_input_option="USER_ENTERED")

        log.info("寫入知識庫 %d 條", len(kb_items))
    except Exception as e:
        log.error("write_kb 失敗：%s", e)


# ── 內部輔助函數 ──────────────────────────────────────────────────────────────

def _ensure_sheet(spreadsheet, title: str, headers: List[str]):
    """若分頁不存在則建立並寫入標題列；若已存在則回傳現有分頁。"""
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(title=title, rows=500, cols=len(headers))
        ws.append_row(headers, value_input_option="USER_ENTERED")
        _format_header(ws)
        log.info("建立分頁：%s", title)
        return ws


def _populate_question_bank(spreadsheet, questions: List[Dict]) -> None:
    """建立問題庫分頁並填入所有題目（每題一列，狀態初始為「待回答」）。"""
    try:
        # 若問題庫已有題目，跳過
        try:
            ws = spreadsheet.worksheet(SHEET_QUESTION_BANK)
            if len(ws.get_all_values()) > 1:
                return
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_QUESTION_BANK, rows=200, cols=7)
            ws.append_row(_question_bank_headers(), value_input_option="USER_ENTERED")
            _format_header(ws)

        rows = []
        for q in questions:
            rows.append([
                q["id"],
                q["module"],
                q["module_name"],
                q["question"],
                q.get("hint", ""),
                "必答" if q.get("required", True) else "選填",
                "⬜ 待回答",
            ])

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        log.info("問題庫填入 %d 題", len(rows))
    except Exception as e:
        log.error("_populate_question_bank 失敗：%s", e)


def _update_question_status(spreadsheet, q_id: str, status: str) -> None:
    """更新問題庫分頁中指定題目的狀態欄（第 7 欄）。"""
    try:
        ws = spreadsheet.worksheet(SHEET_QUESTION_BANK)
        cell_list = ws.findall(q_id, in_column=1)
        for cell in cell_list:
            ws.update_cell(cell.row, 7, status)
    except Exception as e:
        log.error("_update_question_status 失敗：%s", e)


def _format_header(ws) -> None:
    """簡易格式化：凍結標題列（僅 gspread >= 5.x 支援）。"""
    try:
        ws.freeze(rows=1)
    except Exception:
        pass


# ── 欄位定義 ──────────────────────────────────────────────────────────────────

def _interview_log_headers() -> List[str]:
    return ["Q#", "模組代號", "模組名稱", "題目", "業主原始回答", "AI 整理摘要", "回答時間", "狀態"]


def _question_bank_headers() -> List[str]:
    return ["Q#", "模組代號", "模組名稱", "題目", "提示說明", "是否必答", "狀態"]


def _kb_headers() -> List[str]:
    return ["Intent 類型", "顧客問題（觸發詞）", "標準答案", "最後更新"]


def _marketing_headers() -> List[str]:
    return ["活動名稱", "內容說明", "適用服務", "開始日期", "結束日期", "狀態", "觸發關鍵字"]


def _media_headers() -> List[str]:
    return ["素材名稱", "用途說明", "觸發情境（關鍵字）", "圖片 URL / Google Drive 連結", "狀態"]
