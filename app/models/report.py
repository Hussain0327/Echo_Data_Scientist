import enum

from sqlalchemy import JSON, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.sql import func

from app.core.database import Base


class ReportType(str, enum.Enum):
    REVENUE_HEALTH = "revenue_health"
    MARKETING_FUNNEL = "marketing_funnel"
    FINANCIAL_OVERVIEW = "financial_overview"


class ReportStatus(str, enum.Enum):
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


class Report(Base):
    __tablename__ = "reports"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="default")
    report_type = Column(SQLEnum(ReportType), nullable=False)
    data_source_id = Column(String, ForeignKey("data_sources.id"), nullable=True)

    status = Column(SQLEnum(ReportStatus), default=ReportStatus.GENERATING)
    generated_at = Column(DateTime(timezone=True), server_default=func.now())

    metrics = Column(JSON)
    narratives = Column(JSON)

    version = Column(Integer, default=1)
    metadata_ = Column("metadata", JSON)

    error_message = Column(String, nullable=True)
