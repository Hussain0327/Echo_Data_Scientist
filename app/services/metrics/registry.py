import pandas as pd

from app.services.metrics.engine import MetricsEngine
from app.services.metrics.financial import (
    CAC,
    LTV,
    BurnRate,
    GrossMargin,
    LTVCACRatio,
    Runway,
)
from app.services.metrics.marketing import (
    ROAS,
    CampaignPerformance,
    ChannelPerformance,
    ConversionRate,
    CostPerLead,
    FunnelAnalysis,
    LeadVelocity,
)
from app.services.metrics.revenue import (
    ARR,
    MRR,
    AverageOrderValue,
    RevenueByPeriod,
    RevenueByProduct,
    RevenueGrowth,
    TotalRevenue,
)

ALL_METRICS = [
    TotalRevenue,
    RevenueByPeriod,
    RevenueGrowth,
    MRR,
    ARR,
    AverageOrderValue,
    RevenueByProduct,
    CAC,
    LTV,
    LTVCACRatio,
    GrossMargin,
    BurnRate,
    Runway,
    ConversionRate,
    ChannelPerformance,
    CampaignPerformance,
    CostPerLead,
    ROAS,
    LeadVelocity,
    FunnelAnalysis,
]

REVENUE_METRICS = [
    TotalRevenue,
    RevenueByPeriod,
    RevenueGrowth,
    MRR,
    ARR,
    AverageOrderValue,
    RevenueByProduct,
]

FINANCIAL_METRICS = [
    CAC,
    LTV,
    LTVCACRatio,
    GrossMargin,
    BurnRate,
    Runway,
]

MARKETING_METRICS = [
    ConversionRate,
    ChannelPerformance,
    CampaignPerformance,
    CostPerLead,
    ROAS,
    LeadVelocity,
    FunnelAnalysis,
]


def create_metrics_engine(df: pd.DataFrame) -> MetricsEngine:
    engine = MetricsEngine(df)
    for metric_class in ALL_METRICS:
        engine.register(metric_class)
    return engine


def create_revenue_engine(df: pd.DataFrame) -> MetricsEngine:
    engine = MetricsEngine(df)
    for metric_class in REVENUE_METRICS:
        engine.register(metric_class)
    return engine


def create_financial_engine(df: pd.DataFrame) -> MetricsEngine:
    engine = MetricsEngine(df)
    for metric_class in FINANCIAL_METRICS:
        engine.register(metric_class)
    return engine


def create_marketing_engine(df: pd.DataFrame) -> MetricsEngine:
    engine = MetricsEngine(df)
    for metric_class in MARKETING_METRICS:
        engine.register(metric_class)
    return engine


def get_available_metrics() -> dict:
    return {
        "revenue": [m.__name__ for m in REVENUE_METRICS],
        "financial": [m.__name__ for m in FINANCIAL_METRICS],
        "marketing": [m.__name__ for m in MARKETING_METRICS],
    }
