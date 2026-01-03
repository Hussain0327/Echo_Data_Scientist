from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.feedback import AccuracyRating, Feedback
from app.models.report import Report
from app.models.schemas import (
    AccuracyStats,
    AnalyticsOverview,
    PortfolioStats,
    SatisfactionStats,
    TimeSavingsStats,
    UsageStats,
)
from app.models.usage_metric import TaskType, UsageMetric


class AnalyticsAggregator:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_time_savings_stats(self, user_id: str = "default") -> TimeSavingsStats:
        stmt = select(UsageMetric).where(
            UsageMetric.user_id == user_id, UsageMetric.end_time.isnot(None)
        )
        result = await self.db.execute(stmt)
        metrics = list(result.scalars().all())

        total_sessions = len(metrics)
        total_time_saved_seconds = sum(
            m.time_saved_seconds for m in metrics if m.time_saved_seconds
        )
        total_duration_seconds = sum(m.duration_seconds for m in metrics if m.duration_seconds)

        sessions_by_task_type = defaultdict(int)
        for m in metrics:
            sessions_by_task_type[m.task_type.value] += 1

        return TimeSavingsStats(
            total_sessions=total_sessions,
            total_time_saved_hours=round(total_time_saved_seconds / 3600, 2)
            if total_time_saved_seconds
            else 0.0,
            avg_time_saved_hours=round((total_time_saved_seconds / total_sessions) / 3600, 2)
            if total_sessions > 0 and total_time_saved_seconds
            else 0.0,
            avg_duration_minutes=round((total_duration_seconds / total_sessions) / 60, 2)
            if total_sessions > 0 and total_duration_seconds
            else 0.0,
            sessions_by_task_type=dict(sessions_by_task_type),
        )

    async def get_satisfaction_stats(self, user_id: str = "default") -> SatisfactionStats:
        stmt = select(Feedback).where(Feedback.user_id == user_id, Feedback.rating.isnot(None))
        result = await self.db.execute(stmt)
        feedbacks = list(result.scalars().all())

        total_ratings = len(feedbacks)
        avg_rating = sum(f.rating for f in feedbacks) / total_ratings if total_ratings > 0 else 0.0

        rating_distribution = defaultdict(int)
        for f in feedbacks:
            rating_distribution[f.rating] += 1

        ratings_by_type = defaultdict(list)
        for f in feedbacks:
            ratings_by_type[f.interaction_type.value].append(f.rating)

        ratings_by_interaction_type = {
            k: round(sum(v) / len(v), 2) if v else 0.0 for k, v in ratings_by_type.items()
        }

        return SatisfactionStats(
            total_ratings=total_ratings,
            avg_rating=round(avg_rating, 2),
            rating_distribution=dict(rating_distribution),
            ratings_by_interaction_type=ratings_by_interaction_type,
        )

    async def get_accuracy_stats(self, user_id: str = "default") -> AccuracyStats:
        stmt = select(Feedback).where(
            Feedback.user_id == user_id, Feedback.accuracy_rating != AccuracyRating.NOT_RATED
        )
        result = await self.db.execute(stmt)
        feedbacks = list(result.scalars().all())

        total_ratings = len(feedbacks)

        accuracy_distribution = defaultdict(int)
        correct_count = 0
        for f in feedbacks:
            accuracy_distribution[f.accuracy_rating.value] += 1
            if f.accuracy_rating == AccuracyRating.CORRECT:
                correct_count += 1
            elif f.accuracy_rating == AccuracyRating.PARTIALLY_CORRECT:
                correct_count += 0.5

        accuracy_rate = correct_count / total_ratings if total_ratings > 0 else 0.0

        return AccuracyStats(
            total_ratings=total_ratings,
            accuracy_rate=round(accuracy_rate, 4),
            accuracy_distribution=dict(accuracy_distribution),
        )

    async def get_usage_stats(self, user_id: str = "default") -> UsageStats:
        sessions_stmt = select(UsageMetric).where(UsageMetric.user_id == user_id)
        sessions_result = await self.db.execute(sessions_stmt)
        sessions = list(sessions_result.scalars().all())

        reports_stmt = select(Report).where(Report.user_id == user_id)
        reports_result = await self.db.execute(reports_stmt)
        reports = list(reports_result.scalars().all())

        total_sessions = len(sessions)
        total_reports = len(reports)
        total_chats = sum(1 for s in sessions if s.task_type == TaskType.CHAT_INTERACTION)

        metric_usage = defaultdict(int)
        for report in reports:
            if report.metrics:
                for metric_name in report.metrics.keys():
                    metric_usage[metric_name] += 1

        most_used_metrics = sorted(metric_usage.items(), key=lambda x: x[1], reverse=True)[:5]
        most_used_metrics = [m[0] for m in most_used_metrics]

        sessions_per_day = defaultdict(int)
        for session in sessions:
            day = session.start_time.date().isoformat()
            sessions_per_day[day] += 1

        return UsageStats(
            total_sessions=total_sessions,
            total_reports=total_reports,
            total_chats=total_chats,
            most_used_metrics=most_used_metrics,
            sessions_per_day=dict(sessions_per_day),
        )

    async def get_overview(self, user_id: str = "default") -> AnalyticsOverview:
        time_savings = await self.get_time_savings_stats(user_id)
        satisfaction = await self.get_satisfaction_stats(user_id)
        accuracy = await self.get_accuracy_stats(user_id)
        usage = await self.get_usage_stats(user_id)

        return AnalyticsOverview(
            time_savings=time_savings, satisfaction=satisfaction, accuracy=accuracy, usage=usage
        )

    async def get_portfolio_stats(self, user_id: str = "default") -> PortfolioStats:
        overview = await self.get_overview(user_id)

        total_insights = overview.usage.total_reports + overview.usage.total_chats

        headline_metrics = {
            "time_saved": f"Saved users an average of {overview.time_savings.avg_time_saved_hours} hours per analysis",
            "satisfaction": f"{overview.satisfaction.avg_rating}/5 average user satisfaction from {overview.satisfaction.total_ratings} ratings",
            "accuracy": f"{int(overview.accuracy.accuracy_rate * 100)}% accuracy on {overview.accuracy.total_ratings} insights",
            "sessions": f"Processed {overview.usage.total_sessions} sessions with {total_insights} insights generated",
        }

        return PortfolioStats(
            total_sessions=overview.usage.total_sessions,
            total_time_saved_hours=overview.time_savings.total_time_saved_hours,
            avg_time_saved_hours=overview.time_savings.avg_time_saved_hours,
            avg_satisfaction_rating=overview.satisfaction.avg_rating,
            accuracy_rate=overview.accuracy.accuracy_rate,
            total_insights_generated=total_insights,
            headline_metrics=headline_metrics,
        )
