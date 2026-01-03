import enum

from sqlalchemy import JSON, Column, DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class ExperimentStatus(str, enum.Enum):
    """Status of an experiment lifecycle."""

    DRAFT = "draft"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"


class ExperimentDecision(str, enum.Enum):
    """Decision outcome after statistical analysis."""

    SHIP_VARIANT = "ship_variant"
    KEEP_CONTROL = "keep_control"
    INCONCLUSIVE = "inconclusive"
    PENDING = "pending"


class Experiment(Base):
    """
    Represents an A/B experiment definition.

    Tracks the experiment hypothesis, primary metric, timeline,
    and links to variant results for statistical analysis.
    """

    __tablename__ = "experiments"

    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="default")

    # Experiment definition
    name = Column(String, nullable=False)
    hypothesis = Column(Text, nullable=False)
    description = Column(Text)

    # Primary metric to measure (e.g., "signup_conversion", "activation_rate")
    primary_metric = Column(String, nullable=False)

    # Optional secondary metrics for monitoring
    secondary_metrics = Column(JSON)  # List of metric names

    # Funnel stage being tested (e.g., "signup", "activation", "retention")
    funnel_stage = Column(String)

    # Statistical parameters
    significance_level = Column(Float, default=0.05)  # Alpha
    minimum_detectable_effect = Column(Float)  # MDE in percentage points

    # Timeline
    start_date = Column(DateTime(timezone=True))
    end_date = Column(DateTime(timezone=True))

    # Status and decision
    status = Column(SQLEnum(ExperimentStatus), default=ExperimentStatus.DRAFT)
    decision = Column(SQLEnum(ExperimentDecision), default=ExperimentDecision.PENDING)
    decision_rationale = Column(Text)

    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    metadata_ = Column("metadata", JSON)

    # Relationship to variant results
    variants = relationship(
        "VariantResult", back_populates="experiment", cascade="all, delete-orphan"
    )


class VariantResult(Base):
    """
    Aggregated results for a single variant in an experiment.

    Stores the sample size and conversions for each variant,
    enabling lift and statistical significance calculations.
    """

    __tablename__ = "variant_results"

    id = Column(String, primary_key=True)
    experiment_id = Column(String, ForeignKey("experiments.id"), nullable=False)

    # Variant identification
    variant_name = Column(String, nullable=False)  # e.g., "control", "variant_a", "variant_b"
    is_control = Column(Integer, default=0)  # 1 for control group, 0 for treatment

    # Core metrics (aggregated, not per-user)
    users = Column(Integer, nullable=False)  # Total users in variant
    conversions = Column(Integer, nullable=False)  # Users who converted

    # Computed metrics (stored for quick access)
    conversion_rate = Column(Float)

    # Optional: additional metrics for deeper analysis
    revenue = Column(Float)  # Total revenue from variant
    avg_order_value = Column(Float)

    # Funnel breakdown (optional)
    funnel_metrics = Column(JSON)  # {"signup": 1000, "activation": 450, "retention": 200}

    # Timestamps
    recorded_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationship
    experiment = relationship("Experiment", back_populates="variants")
