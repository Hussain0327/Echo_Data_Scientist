import uuid
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.feedback import AccuracyRating, Feedback, InteractionType


class FeedbackService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def submit_feedback(
        self,
        interaction_type: InteractionType,
        user_id: str = "default",
        session_id: Optional[str] = None,
        report_id: Optional[str] = None,
        usage_metric_id: Optional[str] = None,
        rating: Optional[int] = None,
        feedback_text: Optional[str] = None,
        accuracy_rating: Optional[AccuracyRating] = None,
        accuracy_notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Feedback:
        if rating is not None and (rating < 1 or rating > 5):
            raise ValueError("Rating must be between 1 and 5")

        feedback = Feedback(
            id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=session_id,
            report_id=report_id,
            usage_metric_id=usage_metric_id,
            interaction_type=interaction_type,
            rating=rating,
            feedback_text=feedback_text,
            accuracy_rating=accuracy_rating or AccuracyRating.NOT_RATED,
            accuracy_notes=accuracy_notes,
            metadata_=metadata or {},
        )

        self.db.add(feedback)
        await self.db.commit()
        await self.db.refresh(feedback)

        return feedback

    async def get_feedback(self, feedback_id: str, user_id: str = "default") -> Optional[Feedback]:
        stmt = select(Feedback).where(Feedback.id == feedback_id, Feedback.user_id == user_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_user_feedback(self, user_id: str = "default", limit: int = 100) -> list[Feedback]:
        stmt = (
            select(Feedback)
            .where(Feedback.user_id == user_id)
            .order_by(Feedback.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_report_feedback(self, report_id: str, user_id: str = "default") -> list[Feedback]:
        stmt = (
            select(Feedback)
            .where(Feedback.report_id == report_id, Feedback.user_id == user_id)
            .order_by(Feedback.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
