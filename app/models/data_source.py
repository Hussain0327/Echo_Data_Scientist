import enum

from sqlalchemy import JSON, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Integer, String
from sqlalchemy.sql import func

from app.core.database import Base


class SourceType(str, enum.Enum):
    CSV = "csv"
    EXCEL = "excel"
    STRIPE = "stripe"
    HUBSPOT = "hubspot"


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="default")
    source_type = Column(SQLEnum(SourceType), nullable=False)
    file_name = Column(String)
    file_size = Column(Integer)
    upload_timestamp = Column(DateTime(timezone=True), server_default=func.now())
    schema_info = Column(JSON)
    validation_status = Column(String, default="pending")
    validation_errors = Column(JSON)
    row_count = Column(Integer)
    metadata_ = Column("metadata", JSON)
