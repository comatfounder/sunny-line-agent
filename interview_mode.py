"""
Interview Mode — 知識庫建立訪談系統
LINE OA AI 通用模組

整合方式（在 line_server.py 的 admin 處理段加入）：
    from interview_mode import InterviewMode

    # 初始化（server 啟動時）
    interview = InterviewMode(
        redis_client=redis_client,
        sheets_client=sheets_client,
        anthropic_client=anthropic_client,
        brand_name=BRAND_NAME,
        industry=INDUSTRY,
    )

    # 在 admin 訊息路由中
    result = interview.handle(user_id, message_type, message_content)
    if result is not None:
        return result   # 直接回傳，不走一般 AI 流程

管理員指令：
    開始建立知識庫          從第 1 題開始（或提示已有進度）
    繼續建立知識庫          從上次中斷的題目繼續
    下一題 / next / 繼續     整合 buffer → 萃取 → 寫入 → 下一題
    跳過                    跳過當前題目，繼續下一題
    重新問第N題             回到指定題號重新收集答案（e.g. 重新問第5題）
    今天先到這裡             儲存進度，回報完成 N 題
    知識庫建好了嗎           顯示完成進度
    生成知識庫              所有題目完成後，Claude 綜合生成知識庫

版本：v1.0
建立日期：2026-05-06
維護者：LINE OA AI BU
"""

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

TZ_TW = timezone(timedelta(hours=8))


def now_tw() -> datetime:
    return datetime.now(tz=TZ_TW).replace(tzinfo=None)


# ── 管理員觸發指令 ─────────────────────────────────────────────────────────────

CMD_START    = ["開始建立知識庫", "建立知識庫", "開始訪談"]
CMD_RESUME   = ["繼續建立知識庫", "繼續訪談"]
CMD_NEXT     = ["下一題", "next", "繼續", "ok", "好的", "好"]
CMD_SKIP     = ["跳過", "skip", "略過"]
CMD_PAUSE    = ["今天先到這裡", "先到這裡", "暫停", "pause"]
CMD_STATUS   = ["知識庫建好了嗎", "進度", "完成了嗎", "status"]
CMD_GENERATE = ["生成知識庫", "生成知識庫初稿", "產生知識庫"]
CMD_RETRY    = r"^重新問第\s*(\d+)\s*題$"  # regex


class InterviewMode:
    """
    知識庫建立訪談 — 狀態機。

    Redis 結構（key: interview:{user_id}）：
    {
        "active":        bool,         # 訪談進行中
        "brand_name":    str,
        "industry":      str,
        "questions":     list[dict],   # 完整問題清單（含動態展開的 M2）
        "q_index":       int,          # 當前題號 index（0-based）
        "buffer":        list[str],    # 暫存業主訊息
        "answers":       dict,         # {q_id: {"raw": str, "summary": str}}
        "services":      list[str],    # M2 服務清單（special: service_inventory）
        "m2_expanded":   bool,         # M2 追問是否已展開
        "started_at":    str,
        "updated_at":    str,
    }
    """

    REDIS_PREFIX = "interview:"
    REDIS_TTL    = 86400 * 30  # 30 天有效期

    def __init__(
        self,
        redis_client,
        sheets_client,
        anthropic_client,
        brand_name: str,
        industry:   str,
    ):
        self.redis     = redis_client
        self.sheets    = sheets_client
        self.anthropic = anthropic_client
        self.brand     = brand_name
        self.industry  = industry

    # ── 公開入口 ──────────────────────────────────────────────────────────────

    def handle(
        self,
        user_id:         str,
        message_type:    str,     # "text" | "image" | "file"
        message_content: str,     # 文字內容或圖片 URL
    ) -> Optional[str]:
        """
        處理管理員訊息。
        - 如果是 Interview 指令或訪談進行中，回傳回應字串。
        - 如果與 Interview 無關，回傳 None（由上層繼續正常 admin 流程）。
        """
        txt = message_content.strip() if message_type == "text" else ""

        # ① 啟動
        if txt in CMD_START:
            return self._cmd_start(user_id)

        # ② 恢復
        if txt in CMD_RESUME:
            return self._cmd_resume(user_id)

        state = self._load(user_id)

        # ③ 訪談未啟動，與 Interview 無關
        if not state or not state.get("active"):
            return None

        # ④ 暫停
        if txt in CMD_PAUSE:
            return self._cmd_pause(user_id, state)

        # ⑤ 進度查詢
        if txt in CMD_STATUS:
            return self._cmd_status(user_id, state)

        # ⑥ 生成知識庫
        if txt in CMD_GENERATE:
            return self._cmd_generate(user_id, state)

        # ⑦ 下一題
        if txt in CMD_NEXT:
            return self._cmd_next(user_id, state)

        # ⑧ 跳過
        if txt in CMD_SKIP:
            return self._cmd_skip(user_id, state)

        # ⑨ 重新問第 N 題
        m = re.match(CMD_RETRY, txt)
        if m:
            return self._cmd_retry(user_id, state, int(m.group(1)))

        # ⑩ 訪談進行中 → 任何其他訊息（文字/圖片）加入 buffer
        return self._buffer_add(user_id, state, message_type, message_content)

    # ── 指令處理 ──────────────────────────────────────────────────────────────

    def _cmd_start(self, user_id: str) -> str:
        state = self._load(user_id)

        if state and state.get("active"):
            q_index   = state["q_index"]
            total     = len(state["questions"])
            done      = q_index
            q_display = q_index + 1
            return (
                f"你已經有一個進行中的訪談（進度：{done}/{total} 題）。\n\n"
                f"輸入「繼續建立知識庫」從第 {q_display} 題繼續，\n"
                f"或輸入「今天先到這裡」先儲存進度。"
            )

        # 生成問題清單
        from question_bank import generate_questions
        questions = generate_questions(self.brand, self.industry)

        state = {
            "active":      True,
            "brand_name":  self.brand,
            "industry":    self.industry,
            "questions":   questions,
            "q_index":     0,
            "buffer":      [],
            "answers":     {},
            "services":    [],
            "m2_expanded": False,
            "started_at":  now_tw().isoformat(),
            "updated_at":  now_tw().isoformat(),
        }
        self._save(user_id, state)

        # 初始化 Sheets（訪談記錄 / 問題庫 兩個分頁）
        self._init_sheets(questions)

        modules  = self._get_modules(questions)
        mod_list = "\n".join(
            f"  {self._module_emoji(i)} {m['name']}"
            for i, m in enumerate(modules)
        )
        first_q   = questions[0]
        mod_intro = self._format_module_intro(modules[0], 0, questions)

        return (
            f"好的！現在開始為【{self.brand}】建立知識庫 📋\n\n"
            f"我會分 {len(modules)} 個主題問你，不需要一次做完。\n"
            f"說「今天先到這裡」隨時暫停，下次「繼續建立知識庫」接著做。\n"
            f"每題回答完後輸入「下一題」繼續。\n\n"
            f"主題一覽：\n{mod_list}\n\n"
            f"{mod_intro}\n"
            f"{self._format_question(first_q, questions)}"
        )

    def _cmd_resume(self, user_id: str) -> str:
        state = self._load(user_id)
        if not state:
            return "找不到進行中的訪談，請輸入「開始建立知識庫」重新開始。"

        state["active"] = True
        self._save(user_id, state)

        q_index = state["q_index"]
        questions = state["questions"]
        total = len(questions)

        if q_index >= total:
            return (
                "所有題目都已回答完畢！✅\n"
                "輸入「生成知識庫」讓我產出初稿。"
            )

        current_q = questions[q_index]
        modules   = self._get_modules(questions)
        cur_mod   = {"id": current_q["module"], "name": current_q["module_name"]}
        mod_idx   = next((i for i, m in enumerate(modules) if m["id"] == cur_mod["id"]), 0)
        mod_intro = self._format_module_intro(cur_mod, mod_idx, questions)
        done_pct  = int(q_index / len(questions) * 100)
        return (
            f"繼續！整體進度 {done_pct}%。\n\n"
            f"{mod_intro}\n"
            f"{self._format_question(current_q, questions)}"
        )

    def _cmd_next(self, user_id: str, state: dict) -> str:
        questions = state["questions"]
        q_index   = state["q_index"]
        buffer    = state.get("buffer", [])
        total     = len(questions)

        if q_index >= total:
            return "所有題目都已完成！輸入「生成知識庫」產出初稿。"

        current_q = questions[q_index]

        # 若 buffer 為空，提醒業主先回答
        if not buffer:
            return (
                f"你還沒有回答這題喔！\n\n"
                f"{self._format_question(current_q, questions)}\n\n"
                f"回答完後輸入「下一題」繼續。"
            )

        # 整合 buffer → Claude 萃取結構化摘要
        raw_answer = "\n".join(buffer)
        summary    = self._extract_summary(current_q, raw_answer)

        # 寫入 Sheets（訪談記錄）
        self._write_answer_to_sheets(current_q, raw_answer, summary)

        # 儲存答案
        state["answers"][current_q["id"]] = {
            "raw":     raw_answer,
            "summary": summary,
        }
        state["buffer"]   = []
        state["q_index"]  = q_index + 1
        state["updated_at"] = now_tw().isoformat()

        # 處理 special: service_inventory（M2 展開）
        if current_q.get("special") == "service_inventory" and not state.get("m2_expanded"):
            services = self._parse_service_list(raw_answer)
            state["services"]    = services
            state["m2_expanded"] = True

            if services:
                from question_bank import get_service_followup_questions
                next_id = int(state["questions"][-1]["id"]) + 1
                followup = []
                for svc in services:
                    followup.extend(
                        get_service_followup_questions(svc, self.brand, next_id)
                    )
                    next_id += len([q for q in get_service_followup_questions(svc, self.brand, 1)])

                # 插入追問題目到 M3 之前
                m3_start = next(
                    (i for i, q in enumerate(state["questions"]) if q["module"] == "M3"),
                    len(state["questions"]),
                )
                state["questions"] = (
                    state["questions"][:m3_start]
                    + followup
                    + state["questions"][m3_start:]
                )
                # 重新對問題 id 排號
                for idx, q in enumerate(state["questions"]):
                    q["id"] = str(idx + 1)

                svc_list = "、".join(services)
                self._save(user_id, state)
                next_q    = state["questions"][state["q_index"]]
                modules   = self._get_modules(state["questions"])
                mod_idx   = next((i for i, m in enumerate(modules) if m["id"] == next_q["module"]), 0)
                mod_intro = self._format_module_intro(
                    {"id": next_q["module"], "name": next_q["module_name"]},
                    mod_idx, state["questions"]
                )
                return (
                    f"✅ 收到！我記下了這些服務品項：\n{svc_list}\n\n"
                    f"接下來我會逐一詢問每個服務的詳細資訊（每項 5 題）。\n\n"
                    f"{mod_intro}\n"
                    f"{self._format_question(next_q, state['questions'])}"
                )

        self._save(user_id, state)

        # 下一題
        new_index = state["q_index"]
        questions = state["questions"]   # 可能因 M2 展開而更新
        new_total = len(questions)

        if new_index >= new_total:
            return (
                f"✅ 完成！\n\n"
                f"🎉 所有主題都回答完了！\n"
                f"輸入「生成知識庫」讓我把你的答案整理成完整知識庫初稿。"
            )

        next_q        = questions[new_index]
        modules       = self._get_modules(questions)
        prev_mod_id   = current_q["module"]
        next_mod_id   = next_q["module"]

        # 判斷是否跨模組
        if prev_mod_id != next_mod_id:
            # 跨模組：顯示完成慶賀 + 新模組開場
            prev_mod_idx = next((i for i, m in enumerate(modules) if m["id"] == prev_mod_id), 0)
            next_mod_idx = next((i for i, m in enumerate(modules) if m["id"] == next_mod_id), 0)
            completion   = self._module_completion_msg(
                {"id": prev_mod_id, "name": current_q["module_name"]},
                prev_mod_idx, modules
            )
            mod_intro = self._format_module_intro(
                {"id": next_mod_id, "name": next_q["module_name"]},
                next_mod_idx, questions
            )
            return (
                f"{completion}\n"
                f"{mod_intro}\n"
                f"{self._format_question(next_q, questions)}"
            )
        else:
            # 同模組，繼續下一題
            return (
                f"✅ 收到！\n\n"
                f"{self._format_question(next_q, questions)}"
            )

    def _cmd_skip(self, user_id: str, state: dict) -> str:
        questions = state["questions"]
        q_index   = state["q_index"]
        total     = len(questions)

        if q_index >= total:
            return "所有題目都已完成！"

        skipped_q = questions[q_index]
        state["answers"][skipped_q["id"]] = {
            "raw":     "(跳過)",
            "summary": "(業主跳過)",
        }
        self._write_answer_to_sheets(skipped_q, "(跳過)", "(業主跳過)")

        state["buffer"]   = []
        state["q_index"]  = q_index + 1
        state["updated_at"] = now_tw().isoformat()
        self._save(user_id, state)

        new_index = state["q_index"]
        questions = state["questions"]
        if new_index >= len(questions):
            return "⏭ 已跳過。所有主題都完成了！輸入「生成知識庫」。"

        next_q = questions[new_index]
        return (
            f"⏭ 已跳過。\n\n"
            f"{self._format_question(next_q, questions)}"
        )

    def _cmd_pause(self, user_id: str, state: dict) -> str:
        questions = state["questions"]
        q_index   = state["q_index"]
        modules   = self._get_modules(questions)
        done_pct  = int(q_index / max(len(questions), 1) * 100)

        state["active"]     = False
        state["updated_at"] = now_tw().isoformat()
        self._save(user_id, state)

        # 找到目前停在哪個主題
        if q_index < len(questions):
            cur_q     = questions[q_index]
            mod_idx   = next((i for i, m in enumerate(modules) if m["id"] == cur_q["module"]), 0)
            cur_emoji = self._module_emoji(mod_idx)
            cur_name  = cur_q["module_name"]
            cur_info  = f"停在第 {cur_emoji} 主題【{cur_name}】"
        else:
            cur_info = "全部完成"

        return (
            f"好的！進度已儲存 💾\n\n"
            f"整體進度：{done_pct}%\n"
            f"{cur_info}\n\n"
            f"下次輸入「繼續建立知識庫」接著做。"
        )

    def _cmd_status(self, user_id: str, state: dict) -> str:
        questions = state["questions"]
        q_index   = state["q_index"]
        modules   = self._get_modules(questions)

        skipped  = sum(1 for v in state.get("answers", {}).values() if v.get("raw") == "(跳過)")
        answered = q_index - skipped
        done_pct = int(q_index / max(len(questions), 1) * 100)

        # 已完成的模組 vs 尚未完成的模組
        completed_mods = []
        remaining_mods = []
        for i, m in enumerate(modules):
            mod_qs    = [q for q in questions if q["module"] == m["id"]]
            first_idx = questions.index(mod_qs[0])
            last_idx  = questions.index(mod_qs[-1])
            if last_idx < q_index:
                completed_mods.append(f"  ✅ {self._module_emoji(i)} {m['name']}")
            elif first_idx <= q_index:
                remaining_mods.append(f"  🔄 {self._module_emoji(i)} {m['name']}（進行中）")
            else:
                remaining_mods.append(f"  ⬜ {self._module_emoji(i)} {m['name']}")

        if q_index >= len(questions):
            return (
                f"📋 訪談進度：全部完成！✅\n"
                f"回答 {answered} 題，跳過 {skipped} 題。\n\n"
                f"輸入「生成知識庫」產出初稿。"
            )

        mod_status = "\n".join(completed_mods + remaining_mods)
        return (
            f"📋 訪談進度：{done_pct}%\n"
            f"已回答：{answered} 題  |  已跳過：{skipped} 題\n\n"
            f"主題進度：\n{mod_status}\n\n"
            f"輸入「下一題」繼續，或「今天先到這裡」儲存進度。"
        )

    def _cmd_retry(self, user_id: str, state: dict, q_num: int) -> str:
        questions = state["questions"]
        total     = len(questions)

        if q_num < 1 or q_num > total:
            return f"題號不正確，請輸入 1 到 {total} 之間的數字。"

        state["q_index"]    = q_num - 1
        state["buffer"]     = []
        state["updated_at"] = now_tw().isoformat()
        self._save(user_id, state)

        target_q = questions[q_num - 1]
        return (
            f"↩️ 重新作答：\n\n"
            f"{self._format_question(target_q, questions)}\n\n"
            f"（回答完後輸入「下一題」繼續）"
        )

    def _cmd_generate(self, user_id: str, state: dict) -> str:
        questions = state["questions"]
        total     = len(questions)
        done      = state["q_index"]

        if done < total:
            missing = total - done
            return (
                f"還有 {missing} 題尚未完成。\n\n"
                f"輸入「今天先到這裡」儲存進度，之後再繼續；\n"
                f"或繼續回答剩餘題目後再生成知識庫。"
            )

        # 收集所有答案
        answers = state.get("answers", {})
        qa_text = "\n\n".join(
            f"Q{q['id']}. [{q['module_name']}] {q['question']}\n答：{answers.get(q['id'], {}).get('summary', '(未回答)')}"
            for q in questions
        )

        kb = self._generate_kb(qa_text)

        # 寫入 Sheets 知識庫分頁
        self._write_kb_to_sheets(kb)

        state["active"]     = False
        state["updated_at"] = now_tw().isoformat()
        self._save(user_id, state)

        return (
            f"🎉 知識庫初稿已生成並寫入 Google Sheets！\n\n"
            f"請到 Google Sheets 的「知識庫」分頁確認內容。\n"
            f"你可以直接在 Sheets 修改任何答案，修改後 AI 會在下次重新載入時生效。\n\n"
            f"如果要重新生成，輸入「生成知識庫」即可。"
        )

    # ── Buffer 管理 ───────────────────────────────────────────────────────────

    def _buffer_add(
        self,
        user_id: str,
        state:   dict,
        msg_type: str,
        content:  str,
    ) -> str:
        q_index   = state["q_index"]
        questions = state["questions"]
        total     = len(questions)

        if msg_type == "image":
            state["buffer"].append(f"[圖片：{content}]")
        else:
            state["buffer"].append(content)

        state["updated_at"] = now_tw().isoformat()
        self._save(user_id, state)

        current_q = questions[q_index]
        buf_len   = len(state["buffer"])

        if buf_len == 1:
            return (
                f"收到！如果還有要補充的，繼續傳給我。\n"
                f"準備好後輸入「下一題」，我會整理你的回答。"
            )
        else:
            return f"已收到第 {buf_len} 條補充 👌 輸入「下一題」繼續。"

    # ── Claude 萃取 ───────────────────────────────────────────────────────────

    def _extract_summary(self, question: dict, raw_answer: str) -> str:
        """呼叫 Claude 將業主原始回答萃取成結構化摘要。"""
        try:
            prompt = (
                f"你是一個知識庫建立助理。\n"
                f"以下是業主對某個問題的回答（可能包含多條訊息、口語化表達）。\n"
                f"請將回答整理成 1-3 句清晰的繁體中文摘要，供 AI 客服使用。\n"
                f"保留關鍵數字、條件、政策。不要添加沒有提到的資訊。\n\n"
                f"問題：{question['question']}\n\n"
                f"業主回答：\n{raw_answer}\n\n"
                f"摘要："
            )
            resp = self.anthropic.messages.create(
                model="claude-haiku-3-5",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.content[0].text.strip()
        except Exception as e:
            log.error("Claude 萃取摘要失敗：%s", e)
            return raw_answer[:200]  # fallback：截斷原文

    def _parse_service_list(self, raw_answer: str) -> list:
        """從業主的服務品項回答中解析出品項清單。"""
        try:
            prompt = (
                f"以下是業主列出的服務品項（可能用頓號、換行、逗號、數字等分隔）。\n"
                f"請回傳一個 JSON array，每個元素是一個服務品項名稱（繁體中文字串）。\n"
                f"只回傳 JSON array，不要其他文字。\n\n"
                f"業主回答：\n{raw_answer}"
            )
            resp = self.anthropic.messages.create(
                model="claude-haiku-3-5",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            return json.loads(text)
        except Exception as e:
            log.error("解析服務清單失敗：%s", e)
            # fallback：用換行/頓號分割
            return [s.strip() for s in re.split(r"[、,，\n\d\.。\-]", raw_answer) if s.strip()]

    def _generate_kb(self, qa_text: str) -> list:
        """
        呼叫 Claude 將所有 Q&A 整合成知識庫條目。
        回傳格式：[{"intent": "...", "question": "...", "answer": "..."}, ...]
        """
        try:
            prompt = (
                f"你是一個知識庫建立助理。\n"
                f"以下是業主在訪談中回答的所有問題與答案。\n"
                f"請整理成 AI 客服可以直接使用的知識庫條目，格式為 JSON array：\n"
                f'[{{"intent": "諮詢型", "question": "顧客會怎麼問", "answer": "AI 的標準回答"}}, ...]\n\n'
                f"要求：\n"
                f"- 每個條目對應一個顧客可能問到的問題\n"
                f"- 使用業主的語氣和用詞風格\n"
                f"- 答案要完整但不冗長\n"
                f"- 只回傳 JSON array，不要其他文字\n\n"
                f"訪談記錄：\n{qa_text}"
            )
            resp = self.anthropic.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            return json.loads(text)
        except Exception as e:
            log.error("生成知識庫失敗：%s", e)
            return []

    # ── Sheets 操作 ───────────────────────────────────────────────────────────

    def _init_sheets(self, questions: list) -> None:
        """初始化 Google Sheets 的訪談記錄與問題庫分頁。"""
        try:
            from interview_sheets import init_interview_sheets
            init_interview_sheets(self.sheets, questions)
        except Exception as e:
            log.error("初始化 Sheets 失敗：%s", e)

    def _write_answer_to_sheets(
        self, question: dict, raw: str, summary: str
    ) -> None:
        """寫入單題答案到訪談記錄分頁。"""
        try:
            from interview_sheets import write_answer
            write_answer(self.sheets, question, raw, summary)
        except Exception as e:
            log.error("寫入答案到 Sheets 失敗：%s", e)

    def _write_kb_to_sheets(self, kb_items: list) -> None:
        """將知識庫條目寫入知識庫分頁。"""
        try:
            from interview_sheets import write_kb
            write_kb(self.sheets, kb_items)
        except Exception as e:
            log.error("寫入知識庫到 Sheets 失敗：%s", e)

    # ── Redis 操作 ────────────────────────────────────────────────────────────

    def _key(self, user_id: str) -> str:
        return f"{self.REDIS_PREFIX}{user_id}"

    def _load(self, user_id: str) -> Optional[dict]:
        if not self.redis:
            return None
        try:
            raw = self.redis.get(self._key(user_id))
            return json.loads(raw) if raw else None
        except Exception as e:
            log.error("Redis load 失敗：%s", e)
            return None

    def _save(self, user_id: str, state: dict) -> None:
        if not self.redis:
            return
        try:
            self.redis.set(
                self._key(user_id),
                json.dumps(state, ensure_ascii=False),
                ex=self.REDIS_TTL,
            )
        except Exception as e:
            log.error("Redis save 失敗：%s", e)

    # ── 格式化 ────────────────────────────────────────────────────────────────

    @staticmethod
    def _module_progress(q: dict, all_questions: list) -> tuple:
        """
        計算題目在當前模組中的位置。
        回傳 (module_q_num, module_total) — 例如 (3, 7) 表示此模組第 3/7 題。
        """
        module_id = q["module"]
        module_qs = [qq for qq in all_questions if qq["module"] == module_id]
        try:
            pos = module_qs.index(q) + 1
        except ValueError:
            pos = 1
        return pos, len(module_qs)

    @staticmethod
    def _get_modules(questions: list) -> list:
        """
        從問題清單中提取模組順序（去重，保留順序）。
        回傳 [{"id": "M1", "name": "品牌基本資訊"}, ...]
        """
        seen = {}
        modules = []
        for q in questions:
            mid = q["module"]
            if mid not in seen:
                seen[mid] = True
                modules.append({"id": mid, "name": q["module_name"]})
        return modules

    @staticmethod
    def _module_emoji(module_index: int) -> str:
        """把模組 index（0-based）轉成圓圈數字 emoji。"""
        circles = ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩"]
        return circles[module_index] if module_index < len(circles) else f"({module_index + 1})"

    def _format_question(self, q: dict, all_questions: list) -> str:
        """
        格式化單題問題顯示。
        使用模組內進度（例：3/7），不顯示總題數。
        """
        mod_num, mod_total = self._module_progress(q, all_questions)
        required_tag = "（選填）" if not q.get("required", True) else ""
        hint      = f"\n💡 {q['hint']}" if q.get("hint") else ""
        media_tag = "\n📎 可以直接傳圖給我" if q.get("type") == "media" else ""

        return (
            f"【{q['module_name']}】{mod_num}/{mod_total} {required_tag}\n"
            f"{q['question']}"
            f"{hint}"
            f"{media_tag}"
        )

    def _format_module_intro(self, module: dict, module_index: int, all_questions: list) -> str:
        """模組開場白，進入新模組時顯示。"""
        emoji    = self._module_emoji(module_index)
        mid      = module["id"]
        mod_qs   = [q for q in all_questions if q["module"] == mid]
        count    = len(mod_qs)
        return f"─────────────────\n第 {emoji} 主題：{module['name']}（{count} 題）\n"

    def _module_completion_msg(self, module: dict, module_index: int, modules: list) -> str:
        """模組完成時的慶賀訊息。"""
        emoji    = self._module_emoji(module_index)
        done_pct = int((module_index + 1) / len(modules) * 100)
        return f"✅ 第 {emoji} 主題【{module['name']}】完成！（整體進度 {done_pct}%）\n"
