from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import and_, case, exists, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.matcher import is_correct_answer
from app.models import ChatQuestionUsage, ChatSession, GameSessionLog, Question
from app.parser import GotQuestionsParser, ReplenishResult

logger = logging.getLogger(__name__)


@dataclass
class SessionStats:
    asked: int
    taken: int
    complexity_primary_avg: float | None
    complexity_secondary_avg: float | None


@dataclass
class UsageStats:
    started_sessions_24h: int
    active_chats_24h: int
    window_from_utc: datetime
    window_to_utc: datetime


class PoolService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.parser = GotQuestionsParser(settings)
        self._replenish_lock = asyncio.Lock()

    def is_running(self) -> bool:
        return self._replenish_lock.locked()

    async def replenish_to_target(self) -> ReplenishResult:
        if self._replenish_lock.locked():
            async with self._replenish_lock:
                return ReplenishResult(added_questions=0, ready_count=0, pages_scanned=0)
        async with self._replenish_lock:
            return await asyncio.to_thread(self._replenish_sync)

    async def run_manual_batch(self, cursor_start: int | None = None, max_batches: int = 1) -> ReplenishResult:
        if self._replenish_lock.locked():
            async with self._replenish_lock:
                return ReplenishResult(added_questions=0, ready_count=0, pages_scanned=0)
        async with self._replenish_lock:
            return await asyncio.to_thread(lambda: self._manual_batch_sync(cursor_start, max_batches))

    def _replenish_sync(self) -> ReplenishResult:
        with self.session_factory() as db:
            result = self.parser.replenish_cursor_batches(
                db,
                target_per_level=self.settings.replenish_target_per_level,
                batch_size=self.settings.parser_batch_size,
                max_batches=self.settings.parser_max_batches_per_run,
            )
            logger.info(
                "Pool replenish: added=%s ready=%s batches=%s packs_checked=%s cursor=%s->%s",
                result.added_questions,
                result.ready_count,
                result.pages_scanned,
                result.packs_checked,
                result.cursor_before,
                result.cursor_after,
            )
            return result

    def _manual_batch_sync(self, cursor_start: int | None, max_batches: int) -> ReplenishResult:
        with self.session_factory() as db:
            if cursor_start is not None:
                self.parser.set_cursor(db, cursor_start)
            result = self.parser.replenish_cursor_batches(
                db,
                target_per_level=self.settings.replenish_target_per_level,
                batch_size=self.settings.parser_batch_size,
                max_batches=max_batches,
            )
            logger.info(
                "Manual parser batch: added=%s checked=%s found=%s cursor=%s->%s retries=%s net_errors=%s parser_errors=%s",
                result.added_questions,
                result.packs_checked,
                result.packs_found,
                result.cursor_before,
                result.cursor_after,
                result.network_retries,
                result.network_errors,
                result.parser_errors,
            )
            return result


class GameService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session], pool: PoolService) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.pool = pool

    def get_or_create_session(self, db: Session, chat_id: int) -> ChatSession:
        row = db.execute(select(ChatSession).where(ChatSession.chat_id == chat_id)).scalar_one_or_none()
        if row is None:
            row = ChatSession(
                chat_id=chat_id,
                state="IDLE",
                selected_difficulty=None,
                selected_min_likes=1,
                selected_min_take_percent=20.0,
                updated_at=datetime.utcnow(),
            )
            db.add(row)
            db.flush()
        return row

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

    def _build_questions_query(
        self,
        chat_id: int,
        selected_difficulty: int | None,
        selected_min_likes: int,
        selected_min_take_percent: float,
    ):
        used_subquery = (
            select(ChatQuestionUsage.id)
            .where(
                ChatQuestionUsage.chat_id == chat_id,
                ChatQuestionUsage.question_id == Question.question_id,
            )
            .limit(1)
        )
        query = select(Question).where(
            ~exists(used_subquery),
            Question.likes >= selected_min_likes,
            Question.take_percent.is_not(None),
            Question.take_percent >= selected_min_take_percent,
        )

        if selected_difficulty is not None:
            score = self._difficulty_score_expr()
            lower = selected_difficulty - 0.5
            upper = selected_difficulty + 0.5
            if selected_difficulty >= 10:
                query = query.where(and_(score.is_not(None), score >= lower, score <= upper))
            else:
                query = query.where(and_(score.is_not(None), score >= lower, score < upper))

        return query

    def _next_question(
        self,
        db: Session,
        chat_id: int,
        selected_difficulty: int | None,
        selected_min_likes: int,
        selected_min_take_percent: float,
    ) -> Question | None:
        query = self._build_questions_query(
            chat_id=chat_id,
            selected_difficulty=selected_difficulty,
            selected_min_likes=selected_min_likes,
            selected_min_take_percent=selected_min_take_percent,
        )
        return db.execute(query.order_by(func.random()).limit(1)).scalar_one_or_none()

    def count_selection(
        self,
        chat_id: int,
        selected_difficulty: int | None,
        selected_min_likes: int,
        selected_min_take_percent: float,
    ) -> tuple[int, int]:
        with self.session_factory() as db:
            used_subquery = (
                select(ChatQuestionUsage.id)
                .where(
                    ChatQuestionUsage.chat_id == chat_id,
                    ChatQuestionUsage.question_id == Question.question_id,
                )
                .limit(1)
            )
            total = db.execute(select(func.count()).select_from(Question).where(~exists(used_subquery))).scalar_one()
            filtered_query = self._build_questions_query(
                chat_id=chat_id,
                selected_difficulty=selected_difficulty,
                selected_min_likes=selected_min_likes,
                selected_min_take_percent=selected_min_take_percent,
            )
            filtered = db.execute(
                filtered_query.with_only_columns(func.count()).order_by(None)
            ).scalar_one()
            return int(filtered or 0), int(total or 0)

    async def start_game(
        self,
        chat_id: int,
        selected_difficulty: int | None,
        selected_min_likes: int,
        selected_min_take_percent: float,
    ) -> tuple[str, Question | None]:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if session.state == "WAITING_REPLENISH":
                return "waiting_replenish", None
            if session.state != "IDLE":
                return "already_running", None

            session.state = "QUESTION_ACTIVE"
            session.selected_difficulty = selected_difficulty
            session.selected_min_likes = selected_min_likes
            session.selected_min_take_percent = selected_min_take_percent
            session.current_question_message_id = None
            session.session_asked_count = 0
            session.session_taken_count = 0
            session.session_complexity_primary_sum = 0.0
            session.session_complexity_secondary_sum = 0.0
            session.session_complexity_count = 0

            question = self._next_question(
                db,
                chat_id,
                selected_difficulty,
                selected_min_likes,
                selected_min_take_percent,
            )
            if question is None:
                session.state = "WAITING_REPLENISH"
                session.current_question_id = None
                session.updated_at = datetime.utcnow()
                db.commit()
                return "need_replenish", None

            session.current_question_id = question.question_id
            session.updated_at = datetime.utcnow()
            db.commit()
            return "ok", question

    def mark_question_published(self, chat_id: int, question_id: int) -> bool:
        with self.session_factory() as db:
            now = datetime.utcnow()
            usage = ChatQuestionUsage(chat_id=chat_id, question_id=question_id, used_at=now)
            db.add(usage)
            inserted = True
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                inserted = False

            session = self.get_or_create_session(db, chat_id)
            if inserted:
                is_first_question_in_session = session.session_asked_count == 0
                if is_first_question_in_session:
                    db.add(
                        GameSessionLog(
                            chat_id=chat_id,
                            started_at=now,
                            selected_difficulty=session.selected_difficulty,
                            selected_min_likes=int(session.selected_min_likes or 1),
                            selected_min_take_percent=float(session.selected_min_take_percent or 20.0),
                        )
                    )
                question = db.get(Question, question_id)
                session.session_asked_count += 1
                if question and (question.pack_complexity_primary is not None or question.pack_complexity_secondary is not None):
                    session.session_complexity_count += 1
                    session.session_complexity_primary_sum += float(question.pack_complexity_primary or 0.0)
                    session.session_complexity_secondary_sum += float(question.pack_complexity_secondary or 0.0)
            session.updated_at = now
            db.commit()
            return inserted

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
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            current = db.get(Question, session.current_question_id) if session.current_question_id else None
            if session.state != "ANSWER_PENDING_NEXT":
                if return_current:
                    return "no_active", current, None
                return "no_active", None

            next_q = self._next_question(
                db,
                chat_id,
                session.selected_difficulty,
                int(session.selected_min_likes or 1),
                float(session.selected_min_take_percent or 20.0),
            )
            if next_q is None:
                session.state = "WAITING_REPLENISH"
                session.current_question_id = None
                session.current_question_message_id = None
                session.updated_at = datetime.utcnow()
                db.commit()
                if return_current:
                    return "need_replenish", current, None
                return "need_replenish", None

            session.current_question_id = next_q.question_id
            session.current_question_message_id = None
            session.state = "QUESTION_ACTIVE"
            session.updated_at = datetime.utcnow()
            db.commit()
            if return_current:
                return "ok", current, next_q
            return "ok", next_q

    def resume_after_replenish(self, chat_id: int) -> tuple[str, Question | None]:
        with self.session_factory() as db:
            session = self.get_or_create_session(db, chat_id)
            if session.state != "WAITING_REPLENISH":
                return "not_waiting", None

            next_q = self._next_question(
                db,
                chat_id,
                session.selected_difficulty,
                int(session.selected_min_likes or 1),
                float(session.selected_min_take_percent or 20.0),
            )
            if next_q is None:
                return "still_empty", None

            session.current_question_id = next_q.question_id
            session.current_question_message_id = None
            session.state = "QUESTION_ACTIVE"
            session.updated_at = datetime.utcnow()
            db.commit()
            return "ok", next_q

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
            session.selected_min_likes = 1
            session.selected_min_take_percent = 20.0
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
            if session.state != "QUESTION_ACTIVE" or not session.current_question_id:
                return None
            return db.get(Question, session.current_question_id)

    def usage_stats_last_24h(self) -> UsageStats:
        now = datetime.utcnow()
        window_from = now - timedelta(hours=24)
        with self.session_factory() as db:
            started_sessions = db.execute(
                select(func.count()).select_from(GameSessionLog).where(GameSessionLog.started_at >= window_from)
            ).scalar_one()
            active_chats = db.execute(
                select(func.count(func.distinct(GameSessionLog.chat_id)))
                .select_from(GameSessionLog)
                .where(GameSessionLog.started_at >= window_from)
            ).scalar_one()
        return UsageStats(
            started_sessions_24h=int(started_sessions or 0),
            active_chats_24h=int(active_chats or 0),
            window_from_utc=window_from,
            window_to_utc=now,
        )
