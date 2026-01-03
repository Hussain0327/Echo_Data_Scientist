import enum

from sqlalchemy import JSON, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Float, String
from sqlalchemy.sql import func

from app.core.database import Base


class TaskType(str, enum.Enum):
    REPORT_GENERATION = "report_generation"
    CHAT_INTERACTION = "chat_interaction"
    METRIC_CALCULATION = "metric_calculation"
    DATA_UPLOAD = "data_upload"
    GENERAL_ANALYSIS = "general_analysis"


class UsageMetric(Base):
    __tablename__ = "usage_metrics"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="default")
    session_id = Column(String, nullable=True)
    task_type = Column(SQLEnum(TaskType), nullable=False)

    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Float, nullable=True)

    baseline_time_seconds = Column(Float, nullable=True)
    time_saved_seconds = Column(Float, nullable=True)

    metadata_ = Column("metadata", JSON)
