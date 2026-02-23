from __future__ import annotations

import asyncio
import html
import logging
from collections import defaultdict
from typing import Awaitable, Callable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.types.bot_command import BotCommand
from aiogram.types.bot_command_scope_all_group_chats import BotCommandScopeAllGroupChats
from aiogram.types.bot_command_scope_all_private_chats import BotCommandScopeAllPrivateChats

from app.config import Settings
from app.services import GameService

logger = logging.getLogger(__name__)


class BotApp:
    def __init__(self, settings: Settings, game: GameService) -> None:
        self.settings = settings
        self.game = game
        self.bot = Bot(token=settings.bot_token)
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        self.chat_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.scheduled_next: dict[int, asyncio.Task] = {}
        self._bot_username: str | None = None

        self.router.message.register(self.cmd_start, Command("start"))
        self.router.message.register(self.cmd_next, Command("next"))
        self.router.message.register(self.cmd_stop, Command("stop"))
        self.router.message.register(self.on_command_fallback, F.text.startswith("/"))
        self.router.message.register(self.on_text_message, F.text)
        self.router.channel_post.register(self.on_channel_post_text, F.text)

    async def _with_chat_lock(self, chat_id: int, fn: Callable[[], Awaitable[None]]) -> None:
        async with self.chat_locks[chat_id]:
            await fn()

    def _cancel_scheduled(self, chat_id: int) -> None:
        task = self.scheduled_next.get(chat_id)
        if task and not task.done():
            task.cancel()
        self.scheduled_next.pop(chat_id, None)

    def _format_question(self, question) -> str:
        lines = [
            f"<b>Вопрос #{question.number_in_pack}</b>",
            html.escape(question.text or ""),
        ]
        if question.razdatka_text:
            lines.extend(
                [
                    "",
                    "<b>Раздатка:</b>",
                    f"<pre>{html.escape(question.razdatka_text)}</pre>",
                ]
            )
        lines.extend(
            [
                "",
                f"👍 {question.likes} | 👎 {question.dislikes if question.dislikes is not None else 'н/д'}",
            ]
        )
        if question.pack_complexity_primary is not None or question.pack_complexity_secondary is not None:
            lines.append(
                "Сложность пака: "
                f"{question.pack_complexity_primary if question.pack_complexity_primary is not None else '-'}"
                f" · {question.pack_complexity_secondary if question.pack_complexity_secondary is not None else '-'}"
            )
        if question.source_url:
            lines.append(f"Источник вопроса: {html.escape(question.source_url)}")
        return "\n".join(lines)

    def _format_answer(self, question) -> str:
        lines = [
            f"<b>Ответ:</b> {html.escape(question.answer or '—')}",
        ]
        if question.zachet:
            lines.append(f"<b>Зачет:</b> {html.escape(question.zachet)}")
        if question.comment:
            lines.append(f"<b>Комментарий:</b> {html.escape(question.comment)}")
        if question.sources:
            lines.append(f"<b>Источники:</b> {html.escape(question.sources)}")
        if question.take_num and question.take_den:
            percent = question.take_percent or 0.0
            lines.append(f"<b>Взяли:</b> {question.take_num}/{question.take_den} · {percent:.2f}%")
        return "\n".join(lines)

    async def _send_question_to_chat(self, chat_id: int, question) -> None:
        text = self._format_question(question)
        if question.razdatka_pic_url:
            url = question.razdatka_pic_url
            if url.startswith("/"):
                url = f"https://gotquestions.online{url}"
            try:
                sent = await self.bot.send_photo(chat_id=chat_id, photo=url, caption=text, parse_mode="HTML")
            except Exception:
                logger.exception("Failed to send photo for question_id=%s; fallback to text", question.question_id)
                sent = await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        else:
            sent = await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        self.game.set_current_message_id(chat_id, sent.message_id)

    async def cmd_start(self, message: Message) -> None:
        async def _run() -> None:
            try:
                selected_difficulty = self._parse_start_difficulty(message.text or "")
                if selected_difficulty == -1:
                    await message.answer("Использование: /start [сложность 1-10]. Пример: /start 6")
                    return
                status, q = await self.game.start_game(message.chat.id, None if selected_difficulty == 0 else selected_difficulty)
                if status == "already_running":
                    await message.answer("Игра уже запущена. Используй /next или /stop.")
                    return
                if status == "no_questions" or q is None:
                    await message.answer("Нет подходящих вопросов в пуле. Попробуй чуть позже.")
                    return
                await self._send_question_to_chat(message.chat.id, q)
            except Exception:
                logger.exception("cmd_start failed for chat_id=%s", message.chat.id)
                await message.answer("Ошибка при запуске игры. Попробуй еще раз через несколько секунд.")

        await self._with_chat_lock(message.chat.id, _run)

    def _parse_start_difficulty(self, text: str) -> int:
        parts = text.strip().split()
        if len(parts) == 1:
            return 0
        if len(parts) != 2:
            return -1
        raw = parts[1].strip()
        if not raw.isdigit():
            return -1
        value = int(raw)
        if value < 1 or value > 10:
            return -1
        return value

    async def _schedule_next_send_for_chat(self, chat_id: int) -> None:
        self._cancel_scheduled(chat_id)

        async def _task() -> None:
            try:
                await asyncio.sleep(self.settings.next_delay_sec)
                await self._with_chat_lock(chat_id, lambda: self._send_current_active_question(chat_id))
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Scheduled next failed for chat_id=%s", chat_id)

        self.scheduled_next[chat_id] = asyncio.create_task(_task())

    async def _send_current_active_question(self, chat_id: int) -> None:
        question = self.game.get_active_question(chat_id)
        if question is None:
            return
        await self._send_question_to_chat(chat_id, question)

    async def _reveal_and_send_next(self, message: Message) -> None:
        status, current, next_q = await self.game.reveal_and_prepare_next(message.chat.id)
        if status == "no_active":
            await message.answer("Сейчас нет активного вопроса. Используй /start.")
            return
        if current is not None:
            await message.answer(self._format_answer(current), parse_mode="HTML")
        if status == "no_questions" or next_q is None:
            await message.answer("Подходящие вопросы закончились. Игра остановлена.")
            return
        await self._schedule_next_send_for_chat(message.chat.id)

    async def cmd_next(self, message: Message) -> None:
        async def _run() -> None:
            self._cancel_scheduled(message.chat.id)
            await self._reveal_and_send_next(message)

        await self._with_chat_lock(message.chat.id, _run)

    async def cmd_stop(self, message: Message) -> None:
        async def _run() -> None:
            self._cancel_scheduled(message.chat.id)
            stats = self.game.stop_game(message.chat.id)
            c1 = f"{stats.complexity_primary_avg:.2f}" if stats.complexity_primary_avg is not None else "-"
            c2 = f"{stats.complexity_secondary_avg:.2f}" if stats.complexity_secondary_avg is not None else "-"
            await message.answer(
                "Игра остановлена.\n"
                f"Сыграно: {stats.asked}\n"
                f"Взято (автораспознанных): {stats.taken}\n"
                f"Средняя сложность: {c1} · {c2}"
            )

        await self._with_chat_lock(message.chat.id, _run)

    async def on_command_fallback(self, message: Message) -> None:
        if message.text is None:
            return
        text = message.text.strip()
        cmd = text.split()[0].lower()
        cmd_name, mention = (cmd.split("@", 1) + [""])[:2]

        if mention:
            if self._bot_username is None:
                me = await self.bot.get_me()
                self._bot_username = (me.username or "").lower()
            if mention.lower() != (self._bot_username or ""):
                return

        if message.chat.type in {"group", "supergroup"} and cmd_name in {"/start", "/next", "/stop"}:
            await message.answer("Команда получена, обрабатываю...")

        if cmd_name == "/start":
            await self.cmd_start(message)
            return
        if cmd_name == "/next":
            await self.cmd_next(message)
            return
        if cmd_name == "/stop":
            await self.cmd_stop(message)
            return

    async def on_text_message(self, message: Message) -> None:
        if message.text is None or message.text.startswith("/"):
            return
        # Ignore bot-authored messages, but allow anonymous chat sender mode.
        # In anonymous mode Telegram may send from_user=GroupAnonymousBot (is_bot=true)
        # together with sender_chat - these answers must be processed.
        if message.from_user is not None and message.from_user.is_bot and message.sender_chat is None:
            return
        await self._process_answer_message(message)

    async def on_channel_post_text(self, message: Message) -> None:
        if message.text is None or message.text.startswith("/"):
            return
        await self._process_answer_message(message)

    async def _process_answer_message(self, message: Message) -> None:
        async def _run() -> None:
            sender_chat_id = message.sender_chat.id if message.sender_chat is not None else None
            status, question, target_chat_id = self.game.check_answer_with_candidates(
                message.chat.id, sender_chat_id, message.text or ""
            )
            if status != "correct" or question is None:
                return

            if message.from_user is not None:
                name = message.from_user.full_name or "Игрок"
            elif message.sender_chat is not None:
                name = message.sender_chat.title or "Игрок"
            else:
                name = "Игрок"
            await self.bot.send_message(chat_id=target_chat_id, text=f"✅ {html.escape(name)}, правильный ответ!")
            await self.bot.send_message(chat_id=target_chat_id, text=self._format_answer(question), parse_mode="HTML")
            prep_status, next_question = await self.game.prepare_next_after_correct(target_chat_id)
            if prep_status == "no_questions" or next_question is None:
                await self.bot.send_message(chat_id=target_chat_id, text="Подходящие вопросы закончились. Игра остановлена.")
                return
            await self._schedule_next_send_for_chat(target_chat_id)

        await self._with_chat_lock(message.chat.id, _run)

    async def run_polling(self) -> None:
        await self.setup_commands_menu()
        await self.dp.start_polling(self.bot)

    async def setup_commands_menu(self) -> None:
        commands = [
            BotCommand(command="start", description="Старт игры: /start [сложность 1-10]"),
            BotCommand(command="next", description="Показать ответ и следующий вопрос"),
            BotCommand(command="stop", description="Остановить игру и показать статистику"),
        ]
        await self.bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
        await self.bot.set_my_commands(commands, scope=BotCommandScopeAllGroupChats())
