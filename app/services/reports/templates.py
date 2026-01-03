from enum import Enum
from typing import Dict, List

from pydantic import BaseModel


class TemplateSection(str, Enum):
    EXECUTIVE_SUMMARY = "executive_summary"
    KEY_FINDINGS = "key_findings"
    RECOMMENDATIONS = "recommendations"
    DETAILED_ANALYSIS = "detailed_analysis"


class ReportTemplate(BaseModel):
    template_type: str
    display_name: str
    description: str
    required_metrics: List[str]
    optional_metrics: List[str]
    required_columns: List[str]
    sections: List[TemplateSection]
    narrative_prompts: Dict[str, str]


REVENUE_HEALTH_TEMPLATE = ReportTemplate(
    template_type="revenue_health",
    display_name="Weekly Revenue Health",
    description="Comprehensive revenue analysis with growth trends and recurring revenue metrics",
    required_metrics=[
        "total_revenue",
        "revenue_by_period",
        "revenue_growth",
    ],
    optional_metrics=[
        "mrr",
        "arr",
        "average_order_value",
        "revenue_by_product",
    ],
    required_columns=["date", "amount", "status"],
    sections=[
        TemplateSection.EXECUTIVE_SUMMARY,
        TemplateSection.KEY_FINDINGS,
        TemplateSection.DETAILED_ANALYSIS,
        TemplateSection.RECOMMENDATIONS,
    ],
    narrative_prompts={
        "executive_summary": "Provide a 2-3 sentence executive summary of the overall revenue health. Focus on total revenue, growth trends, and the most important insight.",
        "key_findings": "List 3-5 key findings from the revenue data. Each finding should be specific, data-driven, and actionable. Format as bullet points.",
        "detailed_analysis": "Provide detailed analysis of revenue patterns, trends, and anomalies. Discuss month-over-month growth, product mix, and any concerning or encouraging patterns.",
        "recommendations": "Provide 2-4 specific, actionable recommendations based on the data. Each recommendation should address a specific insight and suggest concrete next steps.",
    },
)


MARKETING_FUNNEL_TEMPLATE = ReportTemplate(
    template_type="marketing_funnel",
    display_name="Marketing Funnel Performance",
    description="End-to-end marketing analysis including channel performance, conversion rates, and ROI",
    required_metrics=[
        "conversion_rate",
        "channel_performance",
    ],
    optional_metrics=[
        "campaign_performance",
        "cost_per_lead",
        "roas",
        "lead_velocity",
        "funnel_analysis",
    ],
    required_columns=["date", "source", "leads", "conversions"],
    sections=[
        TemplateSection.EXECUTIVE_SUMMARY,
        TemplateSection.KEY_FINDINGS,
        TemplateSection.DETAILED_ANALYSIS,
        TemplateSection.RECOMMENDATIONS,
    ],
    narrative_prompts={
        "executive_summary": "Summarize overall marketing performance in 2-3 sentences. Highlight conversion rate, best performing channel, and overall ROI.",
        "key_findings": "Identify 3-5 key insights from marketing data. Focus on channel efficiency, conversion patterns, and cost effectiveness. Format as bullet points.",
        "detailed_analysis": "Deep dive into channel performance, conversion funnel efficiency, and spending patterns. Identify which channels deliver the best ROI and where optimization is needed.",
        "recommendations": "Provide 2-4 actionable recommendations for improving marketing performance. Suggest budget reallocations, channel optimizations, or funnel improvements.",
    },
)


FINANCIAL_OVERVIEW_TEMPLATE = ReportTemplate(
    template_type="financial_overview",
    display_name="Financial Overview",
    description="Complete financial health assessment including unit economics, runway, and profitability",
    required_metrics=[
        "cac",
        "ltv",
        "ltv_cac_ratio",
    ],
    optional_metrics=[
        "gross_margin",
        "burn_rate",
        "runway",
    ],
    required_columns=["date"],
    sections=[
        TemplateSection.EXECUTIVE_SUMMARY,
        TemplateSection.KEY_FINDINGS,
        TemplateSection.DETAILED_ANALYSIS,
        TemplateSection.RECOMMENDATIONS,
    ],
    narrative_prompts={
        "executive_summary": "Summarize financial health in 2-3 sentences. Focus on unit economics (LTV:CAC ratio), profitability, and runway if applicable.",
        "key_findings": "List 3-5 critical financial insights. Focus on sustainability, efficiency, and areas of concern or strength. Format as bullet points.",
        "detailed_analysis": "Analyze unit economics in depth. Discuss CAC trends, LTV patterns, margin health, and cash position if data is available. Identify financial risks and opportunities.",
        "recommendations": "Provide 2-4 specific financial recommendations. Focus on improving unit economics, extending runway, or optimizing spend.",
    },
)


TEMPLATES: Dict[str, ReportTemplate] = {
    "revenue_health": REVENUE_HEALTH_TEMPLATE,
    "marketing_funnel": MARKETING_FUNNEL_TEMPLATE,
    "financial_overview": FINANCIAL_OVERVIEW_TEMPLATE,
}


def get_template(template_type: str) -> ReportTemplate:
    if template_type not in TEMPLATES:
        raise ValueError(
            f"Unknown template type: {template_type}. Available: {list(TEMPLATES.keys())}"
        )
    return TEMPLATES[template_type]


def list_templates() -> List[Dict[str, str]]:
    return [
        {
            "type": template.template_type,
            "name": template.display_name,
            "description": template.description,
        }
        for template in TEMPLATES.values()
    ]
