import uuid
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.experiment import Experiment, ExperimentDecision, ExperimentStatus, VariantResult
from app.models.schemas import (
    CreateExperimentRequest,
    ExperimentDecisionEnum,
    ExperimentResponse,
    ExperimentStatusEnum,
    ExperimentSummary,
    StatisticalResult,
    SubmitVariantResultsRequest,
    UpdateExperimentRequest,
    VariantResultResponse,
)
from app.services.experiments.stats import (
    VariantData,
    analyze_experiment,
)


class ExperimentService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_experiment(
        self, request: CreateExperimentRequest, user_id: str = "default"
    ) -> Experiment:
        experiment = Experiment(
            id=str(uuid.uuid4()),
            user_id=user_id,
            name=request.name,
            hypothesis=request.hypothesis,
            description=request.description,
            primary_metric=request.primary_metric,
            secondary_metrics=request.secondary_metrics,
            funnel_stage=request.funnel_stage,
            significance_level=request.significance_level,
            minimum_detectable_effect=request.minimum_detectable_effect,
            start_date=request.start_date or datetime.now(timezone.utc),
            end_date=request.end_date,
            status=ExperimentStatus.DRAFT,
            decision=ExperimentDecision.PENDING,
            metadata_=request.metadata,
        )

        self.db.add(experiment)
        await self.db.commit()
        await self.db.refresh(experiment)

        return experiment

    async def get_experiment(self, experiment_id: str) -> Optional[Experiment]:
        result = await self.db.execute(
            select(Experiment)
            .options(selectinload(Experiment.variants))
            .where(Experiment.id == experiment_id)
        )
        return result.scalar_one_or_none()

    async def list_experiments(
        self,
        user_id: str = "default",
        status: Optional[ExperimentStatusEnum] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Experiment]:
        query = (
            select(Experiment)
            .options(selectinload(Experiment.variants))
            .where(Experiment.user_id == user_id)
            .order_by(Experiment.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        if status:
            query = query.where(Experiment.status == status.value)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_experiment(
        self, experiment_id: str, request: UpdateExperimentRequest
    ) -> Optional[Experiment]:
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return None

        if request.name is not None:
            experiment.name = request.name
        if request.hypothesis is not None:
            experiment.hypothesis = request.hypothesis
        if request.description is not None:
            experiment.description = request.description
        if request.status is not None:
            experiment.status = ExperimentStatus(request.status.value)
        if request.end_date is not None:
            experiment.end_date = request.end_date
        if request.metadata is not None:
            experiment.metadata_ = request.metadata

        await self.db.commit()
        await self.db.refresh(experiment)

        return experiment

    async def submit_variant_results(
        self, experiment_id: str, request: SubmitVariantResultsRequest
    ) -> Optional[Experiment]:
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return None

        # Clear existing variant results
        for variant in experiment.variants:
            await self.db.delete(variant)

        # Add new variant results
        for variant_request in request.variants:
            variant_result = VariantResult(
                id=str(uuid.uuid4()),
                experiment_id=experiment_id,
                variant_name=variant_request.variant_name,
                is_control=1 if variant_request.is_control else 0,
                users=variant_request.users,
                conversions=variant_request.conversions,
                conversion_rate=variant_request.conversions / variant_request.users
                if variant_request.users > 0
                else 0,
                revenue=variant_request.revenue,
                avg_order_value=variant_request.avg_order_value,
                funnel_metrics=variant_request.funnel_metrics,
            )
            self.db.add(variant_result)

        # Update experiment status to running if it was draft
        if experiment.status == ExperimentStatus.DRAFT:
            experiment.status = ExperimentStatus.RUNNING

        await self.db.commit()

        # Refresh to get the new variants
        experiment = await self.get_experiment(experiment_id)

        # Run statistical analysis and update decision
        await self._update_experiment_analysis(experiment)

        # Refresh again to get the updated decision
        experiment = await self.get_experiment(experiment_id)

        return experiment

    async def _update_experiment_analysis(self, experiment: Experiment) -> None:
        if len(experiment.variants) < 2:
            return

        # Find control and variant
        control_result = None
        variant_result = None

        for v in experiment.variants:
            if v.is_control:
                control_result = v
            else:
                variant_result = v

        # If no explicit control, use first as control
        if not control_result:
            control_result = experiment.variants[0]
            variant_result = experiment.variants[1]

        if not control_result or not variant_result:
            return

        # Build VariantData objects
        control = VariantData(
            name=control_result.variant_name,
            users=control_result.users,
            conversions=control_result.conversions,
            is_control=True,
        )

        variant = VariantData(
            name=variant_result.variant_name,
            users=variant_result.users,
            conversions=variant_result.conversions,
            is_control=False,
        )

        # Run analysis
        analysis = analyze_experiment(
            control=control, variant=variant, alpha=experiment.significance_level
        )

        # Update experiment decision
        experiment.decision = ExperimentDecision(analysis.decision)
        experiment.decision_rationale = analysis.decision_rationale

        await self.db.commit()

    async def get_experiment_summary(self, experiment_id: str) -> Optional[ExperimentSummary]:
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return None

        # Build variant responses
        control_response = None
        variant_response = None
        statistics = None

        for v in experiment.variants:
            vr = VariantResultResponse(
                id=v.id,
                variant_name=v.variant_name,
                is_control=bool(v.is_control),
                users=v.users,
                conversions=v.conversions,
                conversion_rate=v.conversion_rate * 100 if v.conversion_rate else 0,
                revenue=v.revenue,
                avg_order_value=v.avg_order_value,
                funnel_metrics=v.funnel_metrics,
                recorded_at=v.recorded_at,
            )

            if v.is_control:
                control_response = vr
            else:
                variant_response = vr

        # If we have both variants, compute statistics
        if len(experiment.variants) >= 2:
            control_result = None
            variant_result = None

            for v in experiment.variants:
                if v.is_control:
                    control_result = v
                else:
                    variant_result = v

            if not control_result:
                control_result = experiment.variants[0]
                variant_result = experiment.variants[1]
                control_response = VariantResultResponse(
                    id=control_result.id,
                    variant_name=control_result.variant_name,
                    is_control=True,
                    users=control_result.users,
                    conversions=control_result.conversions,
                    conversion_rate=control_result.conversion_rate * 100
                    if control_result.conversion_rate
                    else 0,
                    revenue=control_result.revenue,
                    avg_order_value=control_result.avg_order_value,
                    funnel_metrics=control_result.funnel_metrics,
                    recorded_at=control_result.recorded_at,
                )
                variant_response = VariantResultResponse(
                    id=variant_result.id,
                    variant_name=variant_result.variant_name,
                    is_control=False,
                    users=variant_result.users,
                    conversions=variant_result.conversions,
                    conversion_rate=variant_result.conversion_rate * 100
                    if variant_result.conversion_rate
                    else 0,
                    revenue=variant_result.revenue,
                    avg_order_value=variant_result.avg_order_value,
                    funnel_metrics=variant_result.funnel_metrics,
                    recorded_at=variant_result.recorded_at,
                )

            if control_result and variant_result:
                control = VariantData(
                    name=control_result.variant_name,
                    users=control_result.users,
                    conversions=control_result.conversions,
                    is_control=True,
                )

                variant = VariantData(
                    name=variant_result.variant_name,
                    users=variant_result.users,
                    conversions=variant_result.conversions,
                    is_control=False,
                )

                analysis = analyze_experiment(
                    control=control, variant=variant, alpha=experiment.significance_level
                )

                statistics = StatisticalResult(
                    control_conversion_rate=analysis.control_conversion_rate,
                    variant_conversion_rate=analysis.variant_conversion_rate,
                    absolute_lift=analysis.absolute_lift,
                    relative_lift=analysis.relative_lift,
                    confidence_interval_lower=analysis.confidence_interval_lower,
                    confidence_interval_upper=analysis.confidence_interval_upper,
                    z_score=analysis.z_score,
                    p_value=analysis.p_value,
                    is_significant=analysis.is_significant,
                    sample_size_adequate=analysis.sample_size_adequate,
                    power=analysis.power,
                )

        return ExperimentSummary(
            id=experiment.id,
            name=experiment.name,
            hypothesis=experiment.hypothesis,
            description=experiment.description,
            primary_metric=experiment.primary_metric,
            funnel_stage=experiment.funnel_stage,
            status=ExperimentStatusEnum(experiment.status.value),
            significance_level=experiment.significance_level,
            control=control_response,
            variant=variant_response,
            statistics=statistics,
            decision=ExperimentDecisionEnum(experiment.decision.value),
            decision_rationale=experiment.decision_rationale,
            start_date=experiment.start_date,
            end_date=experiment.end_date,
            created_at=experiment.created_at,
        )

    async def delete_experiment(self, experiment_id: str) -> bool:
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return False

        await self.db.delete(experiment)
        await self.db.commit()

        return True

    def to_response(
        self, experiment: Experiment, variants_loaded: bool = True
    ) -> ExperimentResponse:
        variants = []
        if variants_loaded:
            variants = [
                VariantResultResponse(
                    id=v.id,
                    variant_name=v.variant_name,
                    is_control=bool(v.is_control),
                    users=v.users,
                    conversions=v.conversions,
                    conversion_rate=v.conversion_rate * 100 if v.conversion_rate else 0,
                    revenue=v.revenue,
                    avg_order_value=v.avg_order_value,
                    funnel_metrics=v.funnel_metrics,
                    recorded_at=v.recorded_at,
                )
                for v in experiment.variants
            ]

        return ExperimentResponse(
            id=experiment.id,
            name=experiment.name,
            hypothesis=experiment.hypothesis,
            description=experiment.description,
            primary_metric=experiment.primary_metric,
            secondary_metrics=experiment.secondary_metrics,
            funnel_stage=experiment.funnel_stage,
            significance_level=experiment.significance_level,
            minimum_detectable_effect=experiment.minimum_detectable_effect,
            status=ExperimentStatusEnum(experiment.status.value),
            decision=ExperimentDecisionEnum(experiment.decision.value),
            start_date=experiment.start_date,
            end_date=experiment.end_date,
            created_at=experiment.created_at,
            updated_at=experiment.updated_at,
            variants=variants,
        )
