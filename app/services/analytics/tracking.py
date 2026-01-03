import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.usage_metric import TaskType, UsageMetric


class TrackingService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def start_session(
        self,
        task_type: TaskType,
        user_id: str = "default",
        session_id: Optional[str] = None,
        baseline_time_seconds: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> UsageMetric:
        metric = UsageMetric(
            id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=session_id,
            task_type=task_type,
            baseline_time_seconds=baseline_time_seconds,
            metadata_=metadata or {},
        )

        self.db.add(metric)
        await self.db.commit()
        await self.db.refresh(metric)

        return metric

    async def end_session(self, metric_id: str, user_id: str = "default") -> Optional[UsageMetric]:
        stmt = select(UsageMetric).where(
            UsageMetric.id == metric_id, UsageMetric.user_id == user_id
        )
        result = await self.db.execute(stmt)
        metric = result.scalar_one_or_none()

        if not metric:
            return None

        metric.end_time = datetime.now(timezone.utc)

        if metric.start_time and metric.end_time:
            duration = (metric.end_time - metric.start_time).total_seconds()
            metric.duration_seconds = duration

            if metric.baseline_time_seconds:
                metric.time_saved_seconds = metric.baseline_time_seconds - duration

        await self.db.commit()
        await self.db.refresh(metric)

        return metric

    async def get_session(self, metric_id: str, user_id: str = "default") -> Optional[UsageMetric]:
        stmt = select(UsageMetric).where(
            UsageMetric.id == metric_id, UsageMetric.user_id == user_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_user_sessions(
        self, user_id: str = "default", limit: int = 100
    ) -> list[UsageMetric]:
        stmt = (
            select(UsageMetric)
            .where(UsageMetric.user_id == user_id)
            .order_by(UsageMetric.start_time.desc())
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
