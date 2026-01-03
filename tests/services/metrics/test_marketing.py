import pandas as pd

from app.services.metrics.marketing import (
    ROAS,
    CampaignPerformance,
    ChannelPerformance,
    ConversionRate,
    CostPerLead,
    FunnelAnalysis,
    LeadVelocity,
)


class TestConversionRate:
    def test_basic_conversion(self):
        df = pd.DataFrame({"leads": [100, 200], "conversions": [10, 20]})
        metric = ConversionRate(df)
        result = metric.calculate()

        assert result.value == 10.0
        assert result.metadata["total_leads"] == 300
        assert result.metadata["total_conversions"] == 30

    def test_zero_leads(self):
        df = pd.DataFrame({"leads": [0], "conversions": [0]})
        metric = ConversionRate(df)
        result = metric.calculate()

        assert result.value == 0.0

    def test_high_conversion(self):
        df = pd.DataFrame({"leads": [100], "conversions": [50]})
        metric = ConversionRate(df)
        result = metric.calculate()

        assert result.value == 50.0


class TestChannelPerformance:
    def test_multiple_channels(self):
        df = pd.DataFrame(
            {
                "source": ["Google", "Google", "Facebook", "Facebook"],
                "leads": [100, 150, 80, 120],
                "conversions": [10, 15, 8, 12],
                "spend": [500, 600, 400, 500],
            }
        )
        metric = ChannelPerformance(df)
        result = metric.calculate()

        assert result.metadata["channel_count"] == 2
        assert "Google" in result.metadata["channels"]
        assert "Facebook" in result.metadata["channels"]

    def test_conversion_rate_per_channel(self):
        df = pd.DataFrame(
            {
                "source": ["Google", "Facebook"],
                "leads": [100, 100],
                "conversions": [20, 10],
                "spend": [500, 500],
            }
        )
        metric = ChannelPerformance(df)
        result = metric.calculate()

        assert result.metadata["channels"]["Google"]["conversion_rate"] == 20.0
        assert result.metadata["channels"]["Facebook"]["conversion_rate"] == 10.0

    def test_top_channel(self):
        df = pd.DataFrame(
            {"source": ["Google", "Facebook"], "leads": [100, 200], "conversions": [10, 30]}
        )
        metric = ChannelPerformance(df)
        result = metric.calculate()

        assert result.metadata["top_channel"] == "Facebook"


class TestCampaignPerformance:
    def test_multiple_campaigns(self):
        df = pd.DataFrame(
            {
                "campaign": ["Winter Sale", "Winter Sale", "Spring Launch"],
                "leads": [100, 150, 200],
                "conversions": [10, 15, 25],
                "spend": [500, 600, 800],
            }
        )
        metric = CampaignPerformance(df)
        result = metric.calculate()

        assert result.metadata["campaign_count"] == 2
        assert "Winter Sale" in result.metadata["campaigns"]

    def test_cpa_calculation(self):
        df = pd.DataFrame(
            {"campaign": ["Test"], "leads": [100], "conversions": [10], "spend": [1000]}
        )
        metric = CampaignPerformance(df)
        result = metric.calculate()

        assert result.metadata["campaigns"]["Test"]["cpa"] == 100.0


class TestCostPerLead:
    def test_basic_cpl(self):
        df = pd.DataFrame({"spend": [1000, 2000], "leads": [100, 200]})
        metric = CostPerLead(df)
        result = metric.calculate()

        assert result.value == 10.0

    def test_zero_leads(self):
        df = pd.DataFrame({"spend": [1000], "leads": [0]})
        metric = CostPerLead(df)
        result = metric.calculate()

        assert result.value == 0.0


class TestROAS:
    def test_excellent_roas(self):
        df = pd.DataFrame({"spend": [1000], "revenue": [5000]})
        metric = ROAS(df)
        result = metric.calculate()

        assert result.value == 5.0
        assert result.metadata["status"] == "excellent"

    def test_good_roas(self):
        df = pd.DataFrame({"spend": [1000], "revenue": [2500]})
        metric = ROAS(df)
        result = metric.calculate()

        assert result.value == 2.5
        assert result.metadata["status"] == "good"

    def test_losing_roas(self):
        df = pd.DataFrame({"spend": [1000], "revenue": [500]})
        metric = ROAS(df)
        result = metric.calculate()

        assert result.value == 0.5
        assert result.metadata["status"] == "losing"

    def test_zero_spend(self):
        df = pd.DataFrame({"spend": [0], "revenue": [1000]})
        metric = ROAS(df)
        result = metric.calculate()

        assert result.value == 0.0
        assert result.metadata["status"] == "unknown"


class TestLeadVelocity:
    def test_positive_velocity(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "leads": [100, 150]})
        metric = LeadVelocity(df)
        result = metric.calculate()

        assert result.value == 50.0
        assert result.metadata["current_leads"] == 150
        assert result.metadata["previous_leads"] == 100

    def test_negative_velocity(self):
        df = pd.DataFrame({"date": ["2024-01-15", "2024-02-15"], "leads": [200, 100]})
        metric = LeadVelocity(df)
        result = metric.calculate()

        assert result.value == -50.0

    def test_insufficient_data(self):
        df = pd.DataFrame({"date": ["2024-01-15"], "leads": [100]})
        metric = LeadVelocity(df)
        result = metric.calculate()

        assert result.value == 0.0
        assert "Need at least 2 months" in result.metadata.get("message", "")


class TestFunnelAnalysis:
    def test_basic_funnel(self):
        df = pd.DataFrame({"stage": ["lead", "lead", "lead", "qualified", "qualified", "customer"]})
        metric = FunnelAnalysis(df)
        result = metric.calculate(stages=["lead", "qualified", "customer"])

        assert result.metadata["stage_counts"]["lead"] == 3
        assert result.metadata["stage_counts"]["qualified"] == 2
        assert result.metadata["stage_counts"]["customer"] == 1

    def test_conversion_rates(self):
        df = pd.DataFrame({"stage": ["lead"] * 100 + ["qualified"] * 50 + ["customer"] * 10})
        metric = FunnelAnalysis(df)
        result = metric.calculate(stages=["lead", "qualified", "customer"])

        assert result.metadata["stage_conversions"]["lead_to_qualified"] == 50.0
        assert result.metadata["stage_conversions"]["qualified_to_customer"] == 20.0

    def test_overall_conversion(self):
        df = pd.DataFrame({"stage": ["lead"] * 100 + ["customer"] * 10})
        metric = FunnelAnalysis(df)
        result = metric.calculate(stages=["lead", "customer"])

        assert result.value == 10.0
        assert result.metadata["total_entered"] == 100
        assert result.metadata["total_converted"] == 10

    def test_empty_stage(self):
        df = pd.DataFrame({"stage": ["lead"] * 100})
        metric = FunnelAnalysis(df)
        result = metric.calculate(stages=["lead", "qualified", "customer"])

        assert result.metadata["stage_counts"]["qualified"] == 0
        assert result.metadata["stage_counts"]["customer"] == 0
