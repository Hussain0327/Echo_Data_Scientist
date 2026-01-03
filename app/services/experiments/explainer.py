from openai import AsyncOpenAI

from app.config import get_settings
from app.models.schemas import ExperimentExplanation, ExperimentSummary

EXPLAINER_SYSTEM_PROMPT = """You are Echo, a data science consultant explaining A/B test results to business stakeholders.

Your role is to translate statistical results into clear, actionable business insights.

CRITICAL RULES:
1. You MUST use ONLY the exact numbers provided in the experiment data
2. You MUST NOT perform any calculations or make up numbers
3. All statistics have been pre-computed deterministically - just explain them
4. Be direct and concise - stakeholders are busy
5. Focus on business impact, not statistical jargon
6. Always include important caveats and limitations

Your response format should include:
- A one-paragraph executive summary
- 3-5 key findings as bullet points
- A clear recommendation
- Important caveats to consider
- Suggested next steps
"""


def build_experiment_context(summary: ExperimentSummary) -> str:
    stats = summary.statistics

    context = f"""
EXPERIMENT: {summary.name}
HYPOTHESIS: {summary.hypothesis}
PRIMARY METRIC: {summary.primary_metric}
FUNNEL STAGE: {summary.funnel_stage or "Not specified"}

RESULTS:
- Control Group: {summary.control.users:,} users, {summary.control.conversions:,} conversions, {stats.control_conversion_rate:.2f}% conversion rate
- Variant Group: {summary.variant.users:,} users, {summary.variant.conversions:,} conversions, {stats.variant_conversion_rate:.2f}% conversion rate

STATISTICAL ANALYSIS:
- Absolute Lift: {stats.absolute_lift:+.2f} percentage points
- Relative Lift: {stats.relative_lift:+.1f}%
- 95% Confidence Interval: [{stats.confidence_interval_lower:+.2f}, {stats.confidence_interval_upper:+.2f}] pp
- Z-Score: {stats.z_score:.3f}
- P-Value: {stats.p_value:.4f}
- Statistically Significant: {"Yes" if stats.is_significant else "No"} (at α = {summary.significance_level})
- Sample Size Adequate: {"Yes" if stats.sample_size_adequate else "No"}
- Statistical Power: {stats.power:.1%} if stats.power else "Not calculated"

AUTOMATED DECISION: {summary.decision.value.upper().replace("_", " ")}
RATIONALE: {summary.decision_rationale}
"""

    return context


class ExperimentExplainer:
    def __init__(self):
        settings = get_settings()

        self.client = AsyncOpenAI(
            api_key=settings.DEEPSEEK_API_KEY or settings.OPENAI_API_KEY,
            base_url=settings.DEEPSEEK_BASE_URL if settings.DEEPSEEK_API_KEY else None,
        )
        self.model = settings.DEEPSEEK_MODEL if settings.DEEPSEEK_API_KEY else "gpt-4-turbo-preview"

    async def explain(self, summary: ExperimentSummary) -> ExperimentExplanation:
        context = build_experiment_context(summary)

        user_prompt = f"""Based on the following A/B test results, provide a business-friendly explanation.

{context}

Please provide your response in this exact format:

EXECUTIVE SUMMARY:
[One paragraph summarizing the experiment and its outcome]

KEY FINDINGS:
- [Finding 1]
- [Finding 2]
- [Finding 3]

RECOMMENDATION:
[Clear recommendation on what action to take]

CAVEATS:
- [Caveat 1]
- [Caveat 2]

NEXT STEPS:
- [Next step 1]
- [Next step 2]
"""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": EXPLAINER_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=1500,
            temperature=0.4,  # Lower temperature for more consistent output
        )

        content = response.choices[0].message.content

        # Parse the response into structured sections
        explanation = self._parse_explanation(summary.id, content)

        return explanation

    def _parse_explanation(self, experiment_id: str, content: str) -> ExperimentExplanation:
        # Default values in case parsing fails
        summary_text = ""
        key_findings = []
        recommendation = ""
        caveats = []
        next_steps = []

        current_section = None

        for line in content.split("\n"):
            line = line.strip()

            if line.startswith("EXECUTIVE SUMMARY:"):
                current_section = "summary"
                continue
            elif line.startswith("KEY FINDINGS:"):
                current_section = "findings"
                continue
            elif line.startswith("RECOMMENDATION:"):
                current_section = "recommendation"
                continue
            elif line.startswith("CAVEATS:"):
                current_section = "caveats"
                continue
            elif line.startswith("NEXT STEPS:"):
                current_section = "next_steps"
                continue

            if not line:
                continue

            if current_section == "summary":
                summary_text += " " + line
            elif current_section == "findings":
                if line.startswith("- "):
                    key_findings.append(line[2:])
                elif line.startswith("* "):
                    key_findings.append(line[2:])
            elif current_section == "recommendation":
                recommendation += " " + line
            elif current_section == "caveats":
                if line.startswith("- "):
                    caveats.append(line[2:])
                elif line.startswith("* "):
                    caveats.append(line[2:])
            elif current_section == "next_steps":
                if line.startswith("- "):
                    next_steps.append(line[2:])
                elif line.startswith("* "):
                    next_steps.append(line[2:])

        return ExperimentExplanation(
            experiment_id=experiment_id,
            summary=summary_text.strip() or "Analysis complete. See key findings below.",
            key_findings=key_findings or ["Results analyzed successfully."],
            recommendation=recommendation.strip()
            or "Review the statistical analysis for decision.",
            caveats=caveats or ["Consider external factors that may have influenced results."],
            next_steps=next_steps or ["Monitor post-launch metrics if shipping variant."],
        )
