from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from app.services.reports.generator import GeneratedReport, ReportGenerator


class TestReportGenerator:
    @pytest.fixture
    def revenue_df(self):
        return pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=100),
                "amount": [100.0 + i for i in range(100)],
                "status": ["paid"] * 100,
                "customer_id": [f"cust_{i % 20}" for i in range(100)],
            }
        )

    @pytest.fixture
    def marketing_df(self):
        return pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=50),
                "source": ["Google Ads", "Facebook"] * 25,
                "leads": [100 + i for i in range(50)],
                "conversions": [10 + i for i in range(50)],
                "spend": [500.0 + i * 10 for i in range(50)],
            }
        )

    @pytest.fixture
    def mock_conversation_service(self):
        with patch("app.services.reports.generator.ConversationService") as mock:
            service = MagicMock()
            response = AsyncMock()
            response.message = "This is a test narrative."
            service.chat = AsyncMock(return_value=response)
            service.clear_session = MagicMock()
            mock.return_value = service
            yield service

    @pytest.fixture
    def generator(self, mock_conversation_service):
        return ReportGenerator()

    def test_validate_data_success(self, generator, revenue_df):
        from app.services.reports.templates import get_template

        template = get_template("revenue_health")
        generator._validate_data(revenue_df, template)

    def test_validate_data_missing_columns(self, generator):
        from app.services.reports.templates import get_template

        template = get_template("revenue_health")
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3]})

        with pytest.raises(ValueError) as exc_info:
            generator._validate_data(bad_df, template)
        assert "Missing required columns" in str(exc_info.value)

    def test_calculate_metrics_revenue(self, generator, revenue_df):
        from app.services.reports.templates import get_template

        template = get_template("revenue_health")

        metrics = generator._calculate_metrics(revenue_df, template)

        assert "total_revenue" in metrics
        assert "revenue_growth" in metrics
        assert metrics["total_revenue"]["value"] > 0
        assert metrics["total_revenue"]["unit"] == "$"

    def test_calculate_metrics_marketing(self, generator, marketing_df):
        from app.services.reports.templates import get_template

        template = get_template("marketing_funnel")

        metrics = generator._calculate_metrics(marketing_df, template)

        assert "conversion_rate" in metrics
        assert "channel_performance" in metrics
        assert metrics["conversion_rate"]["unit"] == "%"

    def test_format_metrics_for_llm(self, generator, revenue_df):
        from app.services.reports.templates import get_template

        template = get_template("revenue_health")
        metrics = generator._calculate_metrics(revenue_df, template)

        formatted = generator._format_metrics_for_llm(metrics)

        assert "CALCULATED METRICS" in formatted
        assert "Total Revenue" in formatted
        assert "$" in formatted

    @pytest.mark.asyncio
    async def test_generate_narrative(self, generator):
        from app.services.reports.templates import TemplateSection

        narrative = await generator._generate_narrative(
            section_type=TemplateSection.EXECUTIVE_SUMMARY,
            prompt="Summarize the data",
            metrics_formatted="Total Revenue: $10,000",
            data_summary="100 rows",
        )

        assert narrative == "This is a test narrative."
        generator.conversation_service.chat.assert_called_once()
        generator.conversation_service.clear_session.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_full_report(self, generator, revenue_df):
        report = await generator.generate(
            df=revenue_df, template_type="revenue_health", user_id="test_user"
        )

        assert isinstance(report, GeneratedReport)
        assert report.template_type == "revenue_health"
        assert len(report.metrics) > 0
        assert len(report.narratives) > 0
        assert "executive_summary" in report.narratives
        assert report.metadata["user_id"] == "test_user"
        assert report.metadata["row_count"] == 100

    @pytest.mark.asyncio
    async def test_generate_invalid_template(self, generator, revenue_df):
        with pytest.raises(ValueError):
            await generator.generate(df=revenue_df, template_type="invalid_template")

    @pytest.mark.asyncio
    async def test_generate_missing_data(self, generator):
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3]})

        with pytest.raises(ValueError) as exc_info:
            await generator.generate(df=bad_df, template_type="revenue_health")
        assert "Missing required columns" in str(exc_info.value)
