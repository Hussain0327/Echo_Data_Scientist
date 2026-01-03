from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SourceTypeEnum(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    STRIPE = "stripe"
    HUBSPOT = "hubspot"


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool
    sample_values: List[Any]
    null_count: int
    unique_count: int


class SchemaInfo(BaseModel):
    columns: Dict[str, ColumnInfo]
    total_rows: int
    total_columns: int


class ValidationError(BaseModel):
    severity: str
    field: str
    message: str
    suggestion: str


class UploadResponse(BaseModel):
    id: str
    source_type: SourceTypeEnum
    file_name: str
    status: str
    message: str
    schema_info: Optional[SchemaInfo] = None
    validation_errors: Optional[List[ValidationError]] = None


class DataSourceResponse(BaseModel):
    id: str
    user_id: str
    source_type: SourceTypeEnum
    file_name: Optional[str]
    upload_timestamp: datetime
    validation_status: str
    row_count: Optional[int]
    schema_info: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class TaskTypeEnum(str, Enum):
    REPORT_GENERATION = "report_generation"
    CHAT_INTERACTION = "chat_interaction"
    METRIC_CALCULATION = "metric_calculation"
    DATA_UPLOAD = "data_upload"
    GENERAL_ANALYSIS = "general_analysis"


class InteractionTypeEnum(str, Enum):
    REPORT = "report"
    CHAT = "chat"
    METRIC = "metric"


class AccuracyRatingEnum(str, Enum):
    CORRECT = "correct"
    INCORRECT = "incorrect"
    PARTIALLY_CORRECT = "partially_correct"
    NOT_RATED = "not_rated"


class StartSessionRequest(BaseModel):
    task_type: TaskTypeEnum
    baseline_time_seconds: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


class EndSessionRequest(BaseModel):
    session_id: str


class SessionResponse(BaseModel):
    id: str
    user_id: str
    session_id: Optional[str]
    task_type: TaskTypeEnum
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[float]
    time_saved_seconds: Optional[float]

    class Config:
        from_attributes = True


class SubmitFeedbackRequest(BaseModel):
    interaction_type: InteractionTypeEnum
    session_id: Optional[str] = None
    report_id: Optional[str] = None
    usage_metric_id: Optional[str] = None
    rating: Optional[int] = Field(None, ge=1, le=5)
    feedback_text: Optional[str] = None
    accuracy_rating: Optional[AccuracyRatingEnum] = None
    accuracy_notes: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class FeedbackResponse(BaseModel):
    id: str
    user_id: str
    interaction_type: InteractionTypeEnum
    rating: Optional[int]
    feedback_text: Optional[str]
    accuracy_rating: AccuracyRatingEnum
    created_at: datetime

    class Config:
        from_attributes = True


class TimeSavingsStats(BaseModel):
    total_sessions: int
    total_time_saved_hours: float
    avg_time_saved_hours: float
    avg_duration_minutes: float
    sessions_by_task_type: Dict[str, int]


class SatisfactionStats(BaseModel):
    total_ratings: int
    avg_rating: float
    rating_distribution: Dict[int, int]
    ratings_by_interaction_type: Dict[str, float]


class AccuracyStats(BaseModel):
    total_ratings: int
    accuracy_rate: float
    accuracy_distribution: Dict[str, int]


class UsageStats(BaseModel):
    total_sessions: int
    total_reports: int
    total_chats: int
    most_used_metrics: List[str]
    sessions_per_day: Dict[str, int]


class AnalyticsOverview(BaseModel):
    time_savings: TimeSavingsStats
    satisfaction: SatisfactionStats
    accuracy: AccuracyStats
    usage: UsageStats


class PortfolioStats(BaseModel):
    total_sessions: int
    total_time_saved_hours: float
    avg_time_saved_hours: float
    avg_satisfaction_rating: float
    accuracy_rate: float
    total_insights_generated: int
    headline_metrics: Dict[str, str]


class ExperimentStatusEnum(str, Enum):
    DRAFT = "draft"
    RUNNING = "running"
    COMPLETED = "completed"
    STOPPED = "stopped"


class ExperimentDecisionEnum(str, Enum):
    SHIP_VARIANT = "ship_variant"
    KEEP_CONTROL = "keep_control"
    INCONCLUSIVE = "inconclusive"
    PENDING = "pending"


class CreateExperimentRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    hypothesis: str = Field(..., min_length=10, description="The hypothesis being tested")
    description: Optional[str] = None
    primary_metric: str = Field(
        ..., description="Primary KPI to measure (e.g., 'signup_conversion')"
    )
    secondary_metrics: Optional[List[str]] = None
    funnel_stage: Optional[str] = Field(
        None, description="Funnel stage being tested (signup, activation, retention)"
    )
    significance_level: float = Field(
        0.05, ge=0.01, le=0.20, description="Alpha level for statistical significance"
    )
    minimum_detectable_effect: Optional[float] = Field(None, description="MDE in percentage points")
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class UpdateExperimentRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    hypothesis: Optional[str] = None
    description: Optional[str] = None
    status: Optional[ExperimentStatusEnum] = None
    end_date: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class VariantResultRequest(BaseModel):
    variant_name: str = Field(..., description="Name of the variant (e.g., 'control', 'variant_a')")
    is_control: bool = Field(False, description="Whether this is the control group")
    users: int = Field(..., ge=1, description="Total users in this variant")
    conversions: int = Field(..., ge=0, description="Number of users who converted")
    revenue: Optional[float] = Field(None, ge=0, description="Total revenue from variant")
    avg_order_value: Optional[float] = Field(None, ge=0)
    funnel_metrics: Optional[Dict[str, int]] = Field(None, description="Funnel breakdown by stage")


class SubmitVariantResultsRequest(BaseModel):
    variants: List[VariantResultRequest] = Field(
        ..., min_length=2, description="Results for each variant (min 2)"
    )


class VariantResultResponse(BaseModel):
    id: str
    variant_name: str
    is_control: bool
    users: int
    conversions: int
    conversion_rate: float
    revenue: Optional[float] = None
    avg_order_value: Optional[float] = None
    funnel_metrics: Optional[Dict[str, int]] = None
    recorded_at: datetime

    class Config:
        from_attributes = True


class StatisticalResult(BaseModel):
    control_conversion_rate: float
    variant_conversion_rate: float
    absolute_lift: float  # Percentage points difference
    relative_lift: float  # Percentage improvement
    confidence_interval_lower: float
    confidence_interval_upper: float
    z_score: float
    p_value: float
    is_significant: bool
    sample_size_adequate: bool
    power: Optional[float] = None


class ExperimentSummary(BaseModel):
    id: str
    name: str
    hypothesis: str
    description: Optional[str]
    primary_metric: str
    funnel_stage: Optional[str]
    status: ExperimentStatusEnum
    significance_level: float

    # Variant results
    control: Optional[VariantResultResponse] = None
    variant: Optional[VariantResultResponse] = None

    # Statistical analysis
    statistics: Optional[StatisticalResult] = None

    # Decision
    decision: ExperimentDecisionEnum
    decision_rationale: Optional[str]

    # Timestamps
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    created_at: datetime


class ExperimentResponse(BaseModel):
    id: str
    name: str
    hypothesis: str
    description: Optional[str]
    primary_metric: str
    secondary_metrics: Optional[List[str]]
    funnel_stage: Optional[str]
    significance_level: float
    minimum_detectable_effect: Optional[float]
    status: ExperimentStatusEnum
    decision: ExperimentDecisionEnum
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    variants: List[VariantResultResponse] = []

    class Config:
        from_attributes = True


class ExperimentListResponse(BaseModel):
    experiments: List[ExperimentResponse]
    total: int


class ExperimentExplanation(BaseModel):
    experiment_id: str
    summary: str  # One-paragraph executive summary
    key_findings: List[str]  # Bullet points
    recommendation: str  # Clear recommendation
    caveats: List[str]  # Important limitations or considerations
    next_steps: List[str]  # Suggested follow-up actions
