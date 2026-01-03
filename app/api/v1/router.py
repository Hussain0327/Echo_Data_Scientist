from fastapi import APIRouter

from app.api.v1 import analytics, chat, experiments, feedback, health, ingestion, metrics, reports

api_router = APIRouter()

api_router.include_router(health.router, tags=["health"])
api_router.include_router(ingestion.router, prefix="/ingestion", tags=["ingestion"])
api_router.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
api_router.include_router(chat.router, prefix="/chat", tags=["chat"])
api_router.include_router(reports.router, prefix="/reports", tags=["reports"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(feedback.router, prefix="/feedback", tags=["feedback"])
api_router.include_router(experiments.router, prefix="/experiments", tags=["experiments"])
