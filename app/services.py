from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.difficulty import difficulty_bucket
from app.matcher import is_correct_answer
from app.models import ChatSession, Question
from app.parser import GotQuestionsParser

logger = logging.getLogger(__name__)


@dataclass
class SessionStats:
    asked: int
    taken: int
    complexity_primary_avg: float | None
    complexity_secondary_avg: float | None


class PoolService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.parser = GotQuestionsParser(settings)
        self._replenish_lock = asyncio.Lock()
        self._replenish_task: asyncio.Task | None = None
        self._background_task: asyncio.Task | None = None

    async def ensure_pool_if_needed(self) -> None:
        if self._replenish_lock.locked():
            return
        async with self._replenish_lock:
            await asyncio.to_thread(self._replenish_sync)

    def trigger_background_replenish(self) -> None:
        if self._replenish_task and not self._replenish_task.done():
            return
        self._replenish_task = asyncio.create_task(self.ensure_pool_if_needed())

    def start_background_loop(self, interval_sec: int = 30) -> None:
        if self._background_task and not self._background_task.done():
            return

        async def _loop() -> None:
            while True:
                self.trigger_background_replenish()
                await asyncio.sleep(interval_sec)

        self._background_task = asyncio.create_task(_loop())

    async def stop_background_loop(self) -> None:
        if self._background_task and not self._background_task.done():
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
        self._background_task = None

    def _replenish_sync(self) -> None:
        with self.session_factory() as db:
            result = self.parser.replenish_if_needed(db)
            logger.info(
                "Pool replenish: added=%s ready=%s pages=%s",
                result.added_questions,
                result.ready_count,
                result.pages_scanned,
            )


class GameService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session], pool: PoolService) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.pool = pool

    def get_or_create_session(self, db: Session, chat_id: int) -> ChatSession:
        row = db.execute(select(ChatSession).where(ChatSession.chat_id == chat_id)).scalar_one_or_none()
        if row is None:
            row = ChatSession(chat_id=chat_id, state="IDLE", selected_difficulty=None, updated_at=datetime.utcnow())
            db.add(row)
            db.flush()
        return row

    def _question_bucket(self, q: Question) -> int | None:
        return difficulty_bucket(q.pack_complexity_primary, q.pack_complexity_secondary)

    def _difficulty_score_expr(self):
        return case(
            (
                and_(
                    Question.pack_complexity_primary.is_not(None),
                    Question.pack_complexity_secondary.is_not(None),
                ),
                (Question.pack_complexity_primary + Question.pack_complexity_secondary) / 2.0,
            ),
            (Question.pack_complexity_primary.is_not(None), Question.pack_complexity_primary),
            else_=Question.pack_complexity_secondary,
        )

    def _next_question(self, db: Session, selected_difficulty: int | None) -> Question | None:
        query = select(Question).where(Question.is_used.is_(False))
        if selected_difficulty is not None:
            score = self._difficulty_score_expr()
            lower = selected_difficulty - 0.5
            upper = selected_difficulty + 0.5
            if selected_difficulty >= 10:
                query = query.where(and_(score.is_not(None), score >= lower, score <= upper))
            else:
                query = query.where(and_(score.is_not(None), score >= lower, score < upper))
        return db.execute(query.order_by(func.random()).limit(1)).scalar_one_or_none()

    async def start_game(self, chat_id: int, selected_difficulty: int | None) -> tuple[str, Question | None]:
        self.pool.trigger_background_replenish()
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if session.state != "IDLE":
                return "already_running", None

            question = self._next_question(db, selected_difficulty)
            if question is not None:
                question.is_used = True
                question.updated_at = datetime.utcnow()
                session.state = "QUESTION_ACTIVE"
                session.selected_difficulty = selected_difficulty
                session.current_question_id = question.question_id
                session.current_question_message_id = None
                session.session_asked_count = 1
                session.session_taken_count = 0
                session.session_complexity_primary_sum = float(question.pack_complexity_primary or 0.0)
                session.session_complexity_secondary_sum = float(question.pack_complexity_secondary or 0.0)
                session.session_complexity_count = 1 if (question.pack_complexity_primary is not None or question.pack_complexity_secondary is not None) else 0
                session.updated_at = datetime.utcnow()
                db.commit()
                return "ok", question

        return "no_questions", None

    def set_current_message_id(self, chat_id: int, message_id: int) -> None:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            session.current_question_message_id = message_id
            session.updated_at = datetime.utcnow()
            db.commit()

    async def reveal_and_prepare_next(self, chat_id: int) -> tuple[str, Question | None, Question | None]:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if session.state != "QUESTION_ACTIVE" or not session.current_question_id:
                return "no_active", None, None

            current = db.get(Question, session.current_question_id)
            if current is None:
                session.state = "IDLE"
                session.current_question_id = None
                db.commit()
                return "no_active", None, None

            session.state = "ANSWER_PENDING_NEXT"
            db.commit()

        return await self._prepare_next_for_chat(chat_id)

    async def prepare_next_after_correct(self, chat_id: int) -> tuple[str, Question | None]:
        return await self._prepare_next_for_chat(chat_id, return_current=False)

    async def _prepare_next_for_chat(
        self, chat_id: int, return_current: bool = True
    ) -> tuple[str, Question | None, Question | None] | tuple[str, Question | None]:
        self.pool.trigger_background_replenish()
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            current = db.get(Question, session.current_question_id) if session.current_question_id else None
            if session.state != "ANSWER_PENDING_NEXT":
                if return_current:
                    return "no_active", current, None
                return "no_active", None
            next_q = self._next_question(db, session.selected_difficulty)
            if next_q is not None:
                next_q.is_used = True
                next_q.updated_at = datetime.utcnow()
                session.current_question_id = next_q.question_id
                session.current_question_message_id = None
                session.state = "QUESTION_ACTIVE"
                session.session_asked_count += 1
                if next_q.pack_complexity_primary is not None or next_q.pack_complexity_secondary is not None:
                    session.session_complexity_count += 1
                    session.session_complexity_primary_sum += float(next_q.pack_complexity_primary or 0.0)
                    session.session_complexity_secondary_sum += float(next_q.pack_complexity_secondary or 0.0)
                session.updated_at = datetime.utcnow()
                db.commit()
                if return_current:
                    return "ok", current, next_q
                return "ok", next_q

        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            current = db.get(Question, session.current_question_id) if session.current_question_id else None
            session.state = "IDLE"
            session.selected_difficulty = None
            session.current_question_id = None
            session.current_question_message_id = None
            db.commit()
            if return_current:
                return "no_questions", current, None
            return "no_questions", None

    def check_answer(self, chat_id: int, user_text: str) -> tuple[str, Question | None]:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if session.state != "QUESTION_ACTIVE" or not session.current_question_id:
                return "no_active", None
            q = db.get(Question, session.current_question_id)
            if q is None:
                return "no_active", None
            if is_correct_answer(user_text, q.answer, q.zachet):
                session.session_taken_count += 1
                session.state = "ANSWER_PENDING_NEXT"
                session.updated_at = datetime.utcnow()
                db.commit()
                return "correct", q
            return "wrong", q

    def check_answer_with_candidates(
        self, chat_id: int, sender_chat_id: Optional[int], user_text: str
    ) -> tuple[str, Question | None, int]:
        status, question = self.check_answer(chat_id, user_text)
        if status != "no_active":
            return status, question, chat_id

        if sender_chat_id is not None and sender_chat_id != chat_id:
            status2, question2 = self.check_answer(sender_chat_id, user_text)
            return status2, question2, sender_chat_id

        return status, question, chat_id

    def stop_game(self, chat_id: int) -> SessionStats:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            count = session.session_complexity_count
            primary_avg = (session.session_complexity_primary_sum / count) if count else None
            secondary_avg = (session.session_complexity_secondary_sum / count) if count else None
            stats = SessionStats(
                asked=session.session_asked_count,
                taken=session.session_taken_count,
                complexity_primary_avg=primary_avg,
                complexity_secondary_avg=secondary_avg,
            )
            session.state = "IDLE"
            session.selected_difficulty = None
            session.current_question_id = None
            session.current_question_message_id = None
            session.scheduled_next_at = None
            session.lock_version += 1
            session.updated_at = datetime.utcnow()
            db.commit()
            return stats

    def get_active_question(self, chat_id: int) -> Question | None:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if not session.current_question_id:
                return None
            return db.get(Question, session.current_question_id)
