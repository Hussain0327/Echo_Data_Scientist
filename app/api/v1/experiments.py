from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.schemas import (
    CreateExperimentRequest,
    ExperimentExplanation,
    ExperimentListResponse,
    ExperimentResponse,
    ExperimentStatusEnum,
    ExperimentSummary,
    SubmitVariantResultsRequest,
    UpdateExperimentRequest,
)
from app.services.experiments.service import ExperimentService

router = APIRouter()


@router.post("", response_model=ExperimentResponse, status_code=201)
async def create_experiment(request: CreateExperimentRequest, db: AsyncSession = Depends(get_db)):
    service = ExperimentService(db)
    experiment = await service.create_experiment(request)
    # variants_loaded=False because newly created experiments have no variants yet
    return service.to_response(experiment, variants_loaded=False)


@router.get("", response_model=ExperimentListResponse)
async def list_experiments(
    status: Optional[ExperimentStatusEnum] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    service = ExperimentService(db)
    experiments = await service.list_experiments(status=status, limit=limit, offset=offset)

    return ExperimentListResponse(
        experiments=[service.to_response(e) for e in experiments], total=len(experiments)
    )


@router.get("/{experiment_id}", response_model=ExperimentResponse)
async def get_experiment(experiment_id: str, db: AsyncSession = Depends(get_db)):
    service = ExperimentService(db)
    experiment = await service.get_experiment(experiment_id)

    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return service.to_response(experiment)


@router.get("/{experiment_id}/summary", response_model=ExperimentSummary)
async def get_experiment_summary(experiment_id: str, db: AsyncSession = Depends(get_db)):
    service = ExperimentService(db)
    summary = await service.get_experiment_summary(experiment_id)

    if not summary:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return summary


@router.patch("/{experiment_id}", response_model=ExperimentResponse)
async def update_experiment(
    experiment_id: str, request: UpdateExperimentRequest, db: AsyncSession = Depends(get_db)
):
    service = ExperimentService(db)
    experiment = await service.update_experiment(experiment_id, request)

    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return service.to_response(experiment)


@router.post("/{experiment_id}/results", response_model=ExperimentSummary)
async def submit_variant_results(
    experiment_id: str, request: SubmitVariantResultsRequest, db: AsyncSession = Depends(get_db)
):
    service = ExperimentService(db)
    experiment = await service.submit_variant_results(experiment_id, request)

    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    # Return the full summary with analysis
    summary = await service.get_experiment_summary(experiment_id)
    return summary


@router.delete("/{experiment_id}", status_code=204)
async def delete_experiment(experiment_id: str, db: AsyncSession = Depends(get_db)):
    service = ExperimentService(db)
    deleted = await service.delete_experiment(experiment_id)

    if not deleted:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return None


@router.post("/{experiment_id}/explain", response_model=ExperimentExplanation)
async def explain_experiment(experiment_id: str, db: AsyncSession = Depends(get_db)):
    service = ExperimentService(db)
    summary = await service.get_experiment_summary(experiment_id)

    if not summary:
        raise HTTPException(status_code=404, detail="Experiment not found")

    if not summary.statistics:
        raise HTTPException(
            status_code=400,
            detail="Experiment has no results to explain. Submit variant results first.",
        )

    # Import LLM service here to avoid circular imports
    from app.services.experiments.explainer import ExperimentExplainer

    explainer = ExperimentExplainer()
    explanation = await explainer.explain(summary)

    return explanation
