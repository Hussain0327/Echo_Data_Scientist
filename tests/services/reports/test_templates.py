import pytest

from app.services.reports.templates import (
    FINANCIAL_OVERVIEW_TEMPLATE,
    MARKETING_FUNNEL_TEMPLATE,
    REVENUE_HEALTH_TEMPLATE,
    get_template,
    list_templates,
)


class TestReportTemplates:
    def test_revenue_health_template(self):
        template = REVENUE_HEALTH_TEMPLATE
        assert template.template_type == "revenue_health"
        assert template.display_name == "Weekly Revenue Health"
        assert "total_revenue" in template.required_metrics
        assert "date" in template.required_columns
        assert len(template.sections) == 4

    def test_marketing_funnel_template(self):
        template = MARKETING_FUNNEL_TEMPLATE
        assert template.template_type == "marketing_funnel"
        assert "conversion_rate" in template.required_metrics
        assert "leads" in template.required_columns

    def test_financial_overview_template(self):
        template = FINANCIAL_OVERVIEW_TEMPLATE
        assert template.template_type == "financial_overview"
        assert "cac" in template.required_metrics
        assert "ltv" in template.required_metrics
        assert "ltv_cac_ratio" in template.required_metrics

    def test_get_template_valid(self):
        template = get_template("revenue_health")
        assert template.template_type == "revenue_health"

    def test_get_template_invalid(self):
        with pytest.raises(ValueError) as exc_info:
            get_template("invalid_template")
        assert "Unknown template type" in str(exc_info.value)

    def test_list_templates(self):
        templates = list_templates()
        assert len(templates) == 3
        assert all("type" in t for t in templates)
        assert all("name" in t for t in templates)
        assert all("description" in t for t in templates)

    def test_narrative_prompts_exist(self):
        for template_type in ["revenue_health", "marketing_funnel", "financial_overview"]:
            template = get_template(template_type)
            assert len(template.narrative_prompts) > 0
            assert "executive_summary" in template.narrative_prompts
