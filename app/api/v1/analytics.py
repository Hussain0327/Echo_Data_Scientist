from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.schemas import (
    AccuracyStats,
    AnalyticsOverview,
    EndSessionRequest,
    PortfolioStats,
    SatisfactionStats,
    SessionResponse,
    StartSessionRequest,
    TimeSavingsStats,
    UsageStats,
)
from app.services.analytics import AnalyticsAggregator, TrackingService

router = APIRouter()


@router.post("/session/start", response_model=SessionResponse)
async def start_session(
    request: StartSessionRequest, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = TrackingService(db)
    metric = await service.start_session(
        task_type=request.task_type,
        user_id=user_id,
        baseline_time_seconds=request.baseline_time_seconds,
        metadata=request.metadata,
    )
    return metric


@router.post("/session/end", response_model=SessionResponse)
async def end_session(
    request: EndSessionRequest, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = TrackingService(db)
    metric = await service.end_session(request.session_id, user_id)

    if not metric:
        raise HTTPException(status_code=404, detail="Session not found")

    return metric


@router.get("/session/{session_id}", response_model=SessionResponse)
async def get_session(
    session_id: str, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = TrackingService(db)
    metric = await service.get_session(session_id, user_id)

    if not metric:
        raise HTTPException(status_code=404, detail="Session not found")

    return metric


@router.get("/sessions", response_model=list[SessionResponse])
async def get_sessions(
    db: AsyncSession = Depends(get_db), user_id: str = "default", limit: int = 100
):
    service = TrackingService(db)
    return await service.get_user_sessions(user_id, limit)


@router.get("/time-savings", response_model=TimeSavingsStats)
async def get_time_savings(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_time_savings_stats(user_id)


@router.get("/satisfaction", response_model=SatisfactionStats)
async def get_satisfaction(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_satisfaction_stats(user_id)


@router.get("/accuracy", response_model=AccuracyStats)
async def get_accuracy(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_accuracy_stats(user_id)


@router.get("/usage", response_model=UsageStats)
async def get_usage(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_usage_stats(user_id)


@router.get("/overview", response_model=AnalyticsOverview)
async def get_overview(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_overview(user_id)


@router.get("/portfolio", response_model=PortfolioStats)
async def get_portfolio_stats(db: AsyncSession = Depends(get_db), user_id: str = "default"):
    aggregator = AnalyticsAggregator(db)
    return await aggregator.get_portfolio_stats(user_id)
