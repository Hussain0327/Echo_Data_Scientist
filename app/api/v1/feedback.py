from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.schemas import FeedbackResponse, SubmitFeedbackRequest
from app.services.analytics import FeedbackService

router = APIRouter()


@router.post("", response_model=FeedbackResponse)
async def submit_feedback(
    request: SubmitFeedbackRequest, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = FeedbackService(db)

    try:
        feedback = await service.submit_feedback(
            interaction_type=request.interaction_type,
            user_id=user_id,
            session_id=request.session_id,
            report_id=request.report_id,
            usage_metric_id=request.usage_metric_id,
            rating=request.rating,
            feedback_text=request.feedback_text,
            accuracy_rating=request.accuracy_rating,
            accuracy_notes=request.accuracy_notes,
            metadata=request.metadata,
        )
        return feedback
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{feedback_id}", response_model=FeedbackResponse)
async def get_feedback(
    feedback_id: str, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = FeedbackService(db)
    feedback = await service.get_feedback(feedback_id, user_id)

    if not feedback:
        raise HTTPException(status_code=404, detail="Feedback not found")

    return feedback


@router.get("", response_model=list[FeedbackResponse])
async def list_feedback(
    db: AsyncSession = Depends(get_db), user_id: str = "default", limit: int = 100
):
    service = FeedbackService(db)
    return await service.get_user_feedback(user_id, limit)


@router.get("/report/{report_id}", response_model=list[FeedbackResponse])
async def get_report_feedback(
    report_id: str, db: AsyncSession = Depends(get_db), user_id: str = "default"
):
    service = FeedbackService(db)
    return await service.get_report_feedback(report_id, user_id)
