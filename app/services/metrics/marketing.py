from typing import List

import pandas as pd

from app.services.metrics.base import BaseMetric, MetricDefinition, MetricResult


class ConversionRate(BaseMetric):
    CONVERTED_STATUSES = ["converted", "customer", "won", "closed", "success", "completed"]

    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="conversion_rate",
            display_name="Conversion Rate",
            description="Percentage of leads that convert",
            category="marketing",
            unit="%",
            formula="(Conversions / Total Leads) * 100",
            required_columns=["leads", "conversions"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        total_leads = self.df["leads"].sum()
        total_conversions = self.df["conversions"].sum()

        if total_leads == 0:
            rate = 0.0
        else:
            rate = (total_conversions / total_leads) * 100

        return self._format_result(
            value=rate, total_leads=int(total_leads), total_conversions=int(total_conversions)
        )


class ChannelPerformance(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="channel_performance",
            display_name="Channel Performance",
            description="Performance metrics by marketing channel",
            category="marketing",
            unit="$",
            formula="Metrics grouped by source/channel",
            required_columns=["source"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        agg_dict = {"source": "count"}

        if "leads" in df.columns:
            agg_dict["leads"] = "sum"
        if "conversions" in df.columns:
            agg_dict["conversions"] = "sum"
        if "spend" in df.columns:
            agg_dict["spend"] = "sum"

        channel_stats = df.groupby("source").agg(agg_dict)
        channel_stats = channel_stats.rename(columns={"source": "records"})

        if "leads" in channel_stats.columns and "conversions" in channel_stats.columns:
            channel_stats["conversion_rate"] = (
                channel_stats["conversions"] / channel_stats["leads"] * 100
            ).round(2)

        if "spend" in channel_stats.columns and "conversions" in channel_stats.columns:
            channel_stats["cost_per_conversion"] = (
                channel_stats["spend"] / channel_stats["conversions"].replace(0, 1)
            ).round(2)

        sort_col = "conversions" if "conversions" in channel_stats.columns else "records"
        channel_stats = channel_stats.sort_values(sort_col, ascending=False)

        top_channel = channel_stats.index[0] if len(channel_stats) > 0 else None
        total_spend = float(channel_stats["spend"].sum()) if "spend" in channel_stats.columns else 0

        return self._format_result(
            value=total_spend,
            channels=channel_stats.to_dict("index"),
            channel_count=len(channel_stats),
            top_channel=top_channel,
        )


class CampaignPerformance(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="campaign_performance",
            display_name="Campaign Performance",
            description="Performance metrics by campaign",
            category="marketing",
            unit="$",
            formula="Metrics grouped by campaign",
            required_columns=["campaign"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()

        agg_dict = {}
        if "leads" in df.columns:
            agg_dict["leads"] = "sum"
        if "conversions" in df.columns:
            agg_dict["conversions"] = "sum"
        if "spend" in df.columns:
            agg_dict["spend"] = "sum"

        if not agg_dict:
            agg_dict["campaign"] = "count"

        campaign_stats = df.groupby("campaign").agg(agg_dict)

        if "campaign" in campaign_stats.columns:
            campaign_stats = campaign_stats.rename(columns={"campaign": "records"})

        if "leads" in campaign_stats.columns and "conversions" in campaign_stats.columns:
            campaign_stats["conversion_rate"] = (
                campaign_stats["conversions"] / campaign_stats["leads"] * 100
            ).round(2)

        if "spend" in campaign_stats.columns and "conversions" in campaign_stats.columns:
            campaign_stats["cpa"] = (
                campaign_stats["spend"] / campaign_stats["conversions"].replace(0, 1)
            ).round(2)

        sort_col = "conversions" if "conversions" in campaign_stats.columns else "leads"
        if sort_col in campaign_stats.columns:
            campaign_stats = campaign_stats.sort_values(sort_col, ascending=False)

        top_campaign = campaign_stats.index[0] if len(campaign_stats) > 0 else None
        total_conversions = (
            int(campaign_stats["conversions"].sum())
            if "conversions" in campaign_stats.columns
            else 0
        )

        return self._format_result(
            value=float(total_conversions),
            campaigns=campaign_stats.to_dict("index"),
            campaign_count=len(campaign_stats),
            top_campaign=top_campaign,
        )


class CostPerLead(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="cost_per_lead",
            display_name="Cost Per Lead",
            description="Average cost to generate a lead",
            category="marketing",
            unit="$",
            formula="Total Spend / Total Leads",
            required_columns=["spend", "leads"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        total_spend = self.df["spend"].sum()
        total_leads = self.df["leads"].sum()

        if total_leads == 0:
            cpl = 0.0
        else:
            cpl = total_spend / total_leads

        return self._format_result(
            value=float(cpl), total_spend=round(float(total_spend), 2), total_leads=int(total_leads)
        )


class ROAS(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="roas",
            display_name="Return on Ad Spend",
            description="Revenue generated per dollar of ad spend",
            category="marketing",
            unit="ratio",
            formula="Revenue / Ad Spend",
            required_columns=["spend", "revenue"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        total_spend = self.df["spend"].sum()
        total_revenue = self.df["revenue"].sum()

        if total_spend == 0:
            roas = 0.0
            status = "unknown"
        else:
            roas = total_revenue / total_spend
            if roas >= 4:
                status = "excellent"
            elif roas >= 2:
                status = "good"
            elif roas >= 1:
                status = "break_even"
            else:
                status = "losing"

        return self._format_result(
            value=roas,
            total_revenue=round(float(total_revenue), 2),
            total_spend=round(float(total_spend), 2),
            status=status,
        )


class LeadVelocity(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="lead_velocity",
            display_name="Lead Velocity Rate",
            description="Month-over-month growth in qualified leads",
            category="marketing",
            unit="%",
            formula="((Current Month Leads - Previous Month Leads) / Previous Month) * 100",
            required_columns=["leads", "date"],
        )

    def calculate(self, **kwargs) -> MetricResult:
        df = self.df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.to_period("M")

        monthly_leads = df.groupby("month")["leads"].sum().sort_index()

        if len(monthly_leads) < 2:
            return self._format_result(
                value=0.0,
                message="Need at least 2 months of data",
                months_available=len(monthly_leads),
            )

        current = float(monthly_leads.iloc[-1])
        previous = float(monthly_leads.iloc[-2])

        if previous == 0:
            velocity = 100.0 if current > 0 else 0.0
        else:
            velocity = ((current - previous) / previous) * 100

        return self._format_result(
            value=velocity,
            current_month=str(monthly_leads.index[-1]),
            previous_month=str(monthly_leads.index[-2]),
            current_leads=int(current),
            previous_leads=int(previous),
        )


class FunnelAnalysis(BaseMetric):
    def get_definition(self) -> MetricDefinition:
        return MetricDefinition(
            name="funnel_analysis",
            display_name="Funnel Analysis",
            description="Conversion rates through funnel stages",
            category="marketing",
            unit="%",
            formula="Count and conversion at each stage",
            required_columns=["stage"],
        )

    def calculate(self, stages: List[str] = None, **kwargs) -> MetricResult:
        if stages is None:
            stages = ["lead", "qualified", "opportunity", "proposal", "customer"]

        stage_counts = {}
        df_stages = self.df["stage"].str.lower()

        for stage in stages:
            count = (df_stages == stage.lower()).sum()
            stage_counts[stage] = int(count)

        conversions = {}
        for i in range(len(stages) - 1):
            current = stages[i]
            next_stage = stages[i + 1]
            current_count = stage_counts[current]
            next_count = stage_counts[next_stage]

            if current_count > 0:
                rate = (next_count / current_count) * 100
            else:
                rate = 0.0

            conversions[f"{current}_to_{next_stage}"] = round(rate, 2)

        first_stage = stage_counts.get(stages[0], 0)
        last_stage = stage_counts.get(stages[-1], 0)

        if first_stage > 0:
            overall_conversion = (last_stage / first_stage) * 100
        else:
            overall_conversion = 0.0

        return self._format_result(
            value=overall_conversion,
            stage_counts=stage_counts,
            stage_conversions=conversions,
            total_entered=first_stage,
            total_converted=last_stage,
        )
