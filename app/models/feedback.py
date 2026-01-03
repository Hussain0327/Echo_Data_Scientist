import enum

from sqlalchemy import JSON, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.sql import func

from app.core.database import Base


class InteractionType(str, enum.Enum):
    REPORT = "report"
    CHAT = "chat"
    METRIC = "metric"


class AccuracyRating(str, enum.Enum):
    CORRECT = "correct"
    INCORRECT = "incorrect"
    PARTIALLY_CORRECT = "partially_correct"
    NOT_RATED = "not_rated"


class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="default")
    session_id = Column(String, nullable=True)
    report_id = Column(String, ForeignKey("reports.id"), nullable=True)
    usage_metric_id = Column(String, ForeignKey("usage_metrics.id"), nullable=True)

    interaction_type = Column(SQLEnum(InteractionType), nullable=False)
    rating = Column(Integer, nullable=True)
    feedback_text = Column(Text, nullable=True)

    accuracy_rating = Column(SQLEnum(AccuracyRating), default=AccuracyRating.NOT_RATED)
    accuracy_notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    metadata_ = Column("metadata", JSON)
