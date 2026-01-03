import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel

from app.services.llm.context_builder import DataContextBuilder
from app.services.llm.conversation import ConversationService
from app.services.metrics.engine import MetricsEngine
from app.services.metrics.registry import ALL_METRICS
from app.services.reports.templates import ReportTemplate, TemplateSection, get_template


class NarrativeSection(BaseModel):
    section_type: str
    content: str
    generated_at: datetime = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.generated_at is None:
            self.generated_at = datetime.now()


class GeneratedReport(BaseModel):
    report_id: str
    template_type: str
    metrics: Dict[str, Any]
    narratives: Dict[str, str]
    generated_at: datetime
    metadata: Dict[str, Any] = {}


class ReportGenerator:
    def __init__(self):
        self.conversation_service = ConversationService()

    def _validate_data(self, df: pd.DataFrame, template: ReportTemplate) -> None:
        missing_cols = [col for col in template.required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Missing required columns for {template.template_type}: {missing_cols}"
            )

    def _calculate_metrics(self, df: pd.DataFrame, template: ReportTemplate) -> Dict[str, Any]:
        engine = MetricsEngine(df)

        for metric_class in ALL_METRICS:
            engine.register(metric_class)

        metrics_dict = {}
        available_metrics = engine.available_metrics()

        for metric_name in template.required_metrics:
            if metric_name not in available_metrics:
                raise ValueError(
                    f"Required metric '{metric_name}' not available for data. "
                    f"Available: {available_metrics}"
                )

            try:
                result = engine.calculate(metric_name)
                metrics_dict[metric_name] = {
                    "value": result.value,
                    "unit": result.unit,
                    "period": result.period,
                    "metadata": result.metadata,
                }
            except Exception as e:
                raise ValueError(f"Failed to calculate metric '{metric_name}': {str(e)}")

        for metric_name in template.optional_metrics:
            if metric_name in available_metrics:
                try:
                    result = engine.calculate(metric_name)
                    metrics_dict[metric_name] = {
                        "value": result.value,
                        "unit": result.unit,
                        "period": result.period,
                        "metadata": result.metadata,
                    }
                except Exception:
                    pass

        return metrics_dict

    def _format_metrics_for_llm(self, metrics: Dict[str, Any]) -> str:
        lines = ["CALCULATED METRICS:"]
        lines.append("=" * 50)
        lines.append("")

        for metric_name, data in metrics.items():
            display_name = metric_name.replace("_", " ").title()
            value = data["value"]
            unit = data["unit"]
            period = data.get("period", "all_time")

            if unit == "$":
                formatted = f"${value:,.2f}"
            elif unit == "%":
                formatted = f"{value:.2f}%"
            elif unit == "ratio":
                formatted = f"{value:.2f}x"
            elif unit == "months":
                formatted = f"{value:.1f} months"
            else:
                formatted = f"{value:,.2f} {unit}".strip()

            lines.append(f"{display_name}: {formatted}")

            if period != "all_time":
                lines.append(f"  Period: {period}")

            metadata = data.get("metadata", {})
            if metadata and isinstance(metadata, dict):
                for key, val in metadata.items():
                    if key not in ["calculated_at", "metric_name"]:
                        if isinstance(val, (int, float)):
                            lines.append(f"  {key}: {val:,.2f}")
                        elif isinstance(val, str):
                            lines.append(f"  {key}: {val}")

            lines.append("")

        return "\n".join(lines)

    async def _generate_narrative(
        self, section_type: TemplateSection, prompt: str, metrics_formatted: str, data_summary: str
    ) -> str:
        session_id = f"report_{uuid.uuid4().hex[:8]}"

        context_prompt = f"""
You are generating the {section_type.value} section of a business report.

{data_summary}

{metrics_formatted}

TASK: {prompt}

IMPORTANT:
- Use ONLY the metrics provided above
- Do NOT make up or estimate any numbers
- Be specific and data-driven
- Write in a professional but conversational tone
- Keep it concise and actionable
"""

        response = await self.conversation_service.chat(
            session_id=session_id, user_message=context_prompt
        )

        self.conversation_service.clear_session(session_id)

        return response.message

    async def generate(
        self, df: pd.DataFrame, template_type: str, user_id: str = "default"
    ) -> GeneratedReport:
        template = get_template(template_type)

        self._validate_data(df, template)

        metrics = self._calculate_metrics(df, template)

        data_summary = DataContextBuilder.build_data_summary(df)
        metrics_formatted = self._format_metrics_for_llm(metrics)

        narratives = {}
        for section in template.sections:
            prompt = template.narrative_prompts.get(section.value, "")
            if prompt:
                narrative = await self._generate_narrative(
                    section, prompt, metrics_formatted, data_summary
                )
                narratives[section.value] = narrative

        report_id = str(uuid.uuid4())

        return GeneratedReport(
            report_id=report_id,
            template_type=template_type,
            metrics=metrics,
            narratives=narratives,
            generated_at=datetime.now(),
            metadata={
                "user_id": user_id,
                "row_count": len(df),
                "column_count": len(df.columns),
            },
        )


_generator: Optional[ReportGenerator] = None


def get_report_generator() -> ReportGenerator:
    global _generator
    if _generator is None:
        _generator = ReportGenerator()
    return _generator
