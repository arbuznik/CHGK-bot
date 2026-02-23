from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Pack(Base):
    __tablename__ = "packs"

    pack_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(1000), default="")
    pub_date: Mapped[str] = mapped_column(String(128), default="")
    complexity_primary: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    complexity_secondary: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source_url: Mapped[str] = mapped_column(String(1024), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Question(Base):
    __tablename__ = "questions"

    question_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pack_id: Mapped[int] = mapped_column(ForeignKey("packs.pack_id"), index=True)
    number_in_pack: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text)
    source_url: Mapped[str] = mapped_column(String(1024), default="")
    razdatka_pic_url: Mapped[str] = mapped_column(String(1024), default="")
    answer: Mapped[str] = mapped_column(Text, default="")
    zachet: Mapped[str] = mapped_column(Text, default="")
    comment: Mapped[str] = mapped_column(Text, default="")
    sources: Mapped[str] = mapped_column(Text, default="")
    likes: Mapped[int] = mapped_column(Integer, default=0)
    dislikes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    take_num: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    take_den: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    take_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pack_complexity_primary: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pack_complexity_secondary: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_used: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class ChatSession(Base):
    __tablename__ = "chat_sessions"
    __table_args__ = (UniqueConstraint("chat_id", name="uq_chat_sessions_chat_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(Integer, index=True)
    state: Mapped[str] = mapped_column(String(64), default="IDLE")
    current_question_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    current_question_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    scheduled_next_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    lock_version: Mapped[int] = mapped_column(Integer, default=0)
    session_asked_count: Mapped[int] = mapped_column(Integer, default=0)
    session_taken_count: Mapped[int] = mapped_column(Integer, default=0)
    session_complexity_primary_sum: Mapped[float] = mapped_column(Float, default=0.0)
    session_complexity_secondary_sum: Mapped[float] = mapped_column(Float, default=0.0)
    session_complexity_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
