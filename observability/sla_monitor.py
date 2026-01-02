"""
SLA Monitoring and Freshness Checks.

Monitors pipeline execution times and data freshness against
defined SLAs, emitting alerts when thresholds are exceeded.

Features:
- Pipeline runtime SLAs
- Data freshness checks
- Completion time windows
- Historical SLA tracking

Usage:
    from observability.sla_monitor import SLAMonitor, SLADefinition

    monitor = SLAMonitor(db_connection, alert_manager)

    # Check if pipeline met its SLA
    result = monitor.check_pipeline_sla(
        pipeline_name="daily_metrics",
        started_at=start_time,
        completed_at=end_time
    )

    # Check data freshness
    freshness = monitor.check_freshness("fct_transactions", max_age_hours=6)
"""

import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Optional

import structlog

from observability.alerts import AlertManager, get_alert_manager


@dataclass
class SLADefinition:
    """
    Defines SLA requirements for a pipeline or table.

    Attributes:
        name: Pipeline or table name
        max_runtime_minutes: Maximum allowed runtime
        max_data_latency_hours: Maximum allowed data staleness
        required_completion_time: Time by which pipeline must complete (HH:MM)
        min_rows_per_run: Minimum rows expected per run
        max_failure_rate: Maximum allowed failure rate (0-1)
    """

    name: str
    max_runtime_minutes: int = 60
    max_data_latency_hours: int = 6
    required_completion_time: Optional[str] = None  # "HH:MM" format
    min_rows_per_run: int = 0
    max_failure_rate: float = 0.05  # 5%


# Default SLA definitions for common pipelines
DEFAULT_SLAS = {
    "daily_metrics_pipeline": SLADefinition(
        name="daily_metrics_pipeline",
        max_runtime_minutes=60,
        max_data_latency_hours=6,
        required_completion_time="08:00",
    ),
    "data_ingestion_pipeline": SLADefinition(
        name="data_ingestion_pipeline",
        max_runtime_minutes=30,
        max_data_latency_hours=4,
    ),
    "experiment_analysis_pipeline": SLADefinition(
        name="experiment_analysis_pipeline",
        max_runtime_minutes=15,
        max_data_latency_hours=24,
    ),
    "fct_transactions": SLADefinition(
        name="fct_transactions",
        max_data_latency_hours=6,
        min_rows_per_run=100,
    ),
    "fct_marketing_events": SLADefinition(
        name="fct_marketing_events",
        max_data_latency_hours=12,
    ),
}


@dataclass
class SLAResult:
    """Result of an SLA check."""

    pipeline_name: str
    sla_type: str
    passed: bool
    threshold: Any
    actual: Any
    message: str
    checked_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FreshnessResult:
    """Result of a freshness check."""

    table_name: str
    is_fresh: bool
    last_load_at: Optional[datetime]
    hours_stale: float
    max_age_hours: float
    checked_at: datetime = field(default_factory=datetime.utcnow)


class SLAMonitor:
    """
    Monitors SLAs and data freshness.

    Integrates with AlertManager to emit alerts when SLAs are breached.
    """

    def __init__(
        self,
        db_connection: Any = None,
        alert_manager: Optional[AlertManager] = None,
        sla_definitions: Optional[dict[str, SLADefinition]] = None,
    ):
        """
        Initialize SLAMonitor.

        Args:
            db_connection: Database connection for freshness checks
            alert_manager: AlertManager for emitting alerts
            sla_definitions: Custom SLA definitions (merged with defaults)
        """
        self.db = db_connection
        self.alerts = alert_manager or get_alert_manager()
        self.logger = structlog.get_logger("sla_monitor")

        # Merge custom SLAs with defaults
        self.slas = DEFAULT_SLAS.copy()
        if sla_definitions:
            self.slas.update(sla_definitions)

    def get_sla(self, name: str) -> Optional[SLADefinition]:
        """Get SLA definition by name."""
        return self.slas.get(name)

    def check_pipeline_runtime(
        self,
        pipeline_name: str,
        started_at: datetime,
        completed_at: datetime,
    ) -> SLAResult:
        """
        Check if pipeline runtime is within SLA.

        Args:
            pipeline_name: Name of the pipeline
            started_at: When the pipeline started
            completed_at: When the pipeline completed

        Returns:
            SLAResult with pass/fail status
        """
        sla = self.get_sla(pipeline_name)
        if sla is None:
            # No SLA defined, assume pass
            return SLAResult(
                pipeline_name=pipeline_name,
                sla_type="runtime",
                passed=True,
                threshold="no_sla_defined",
                actual="n/a",
                message="No SLA defined for this pipeline",
            )

        runtime_minutes = (completed_at - started_at).total_seconds() / 60
        passed = runtime_minutes <= sla.max_runtime_minutes

        result = SLAResult(
            pipeline_name=pipeline_name,
            sla_type="runtime",
            passed=passed,
            threshold=f"{sla.max_runtime_minutes} minutes",
            actual=f"{runtime_minutes:.1f} minutes",
            message=(
                f"Pipeline completed in {runtime_minutes:.1f} minutes"
                if passed
                else f"Pipeline exceeded runtime SLA: {runtime_minutes:.1f}m > {sla.max_runtime_minutes}m"
            ),
        )

        if not passed:
            self.alerts.emit_sla_breach(
                pipeline_name=pipeline_name,
                sla_type="runtime",
                threshold=f"{sla.max_runtime_minutes} minutes",
                actual=f"{runtime_minutes:.1f} minutes",
            )

        self.logger.info(
            "sla_runtime_check",
            pipeline=pipeline_name,
            passed=passed,
            runtime_minutes=round(runtime_minutes, 1),
            threshold_minutes=sla.max_runtime_minutes,
        )

        return result

    def check_completion_time(
        self,
        pipeline_name: str,
        completed_at: datetime,
    ) -> SLAResult:
        """
        Check if pipeline completed by required time.

        Args:
            pipeline_name: Name of the pipeline
            completed_at: When the pipeline completed

        Returns:
            SLAResult with pass/fail status
        """
        sla = self.get_sla(pipeline_name)
        if sla is None or sla.required_completion_time is None:
            return SLAResult(
                pipeline_name=pipeline_name,
                sla_type="completion_time",
                passed=True,
                threshold="no_sla_defined",
                actual="n/a",
                message="No completion time SLA defined",
            )

        # Parse required time
        hour, minute = map(int, sla.required_completion_time.split(":"))
        required_time = time(hour, minute)

        # Check if completed before required time
        completed_time = completed_at.time()
        passed = completed_time <= required_time

        result = SLAResult(
            pipeline_name=pipeline_name,
            sla_type="completion_time",
            passed=passed,
            threshold=sla.required_completion_time,
            actual=completed_time.strftime("%H:%M"),
            message=(
                f"Pipeline completed at {completed_time.strftime('%H:%M')}"
                if passed
                else f"Pipeline missed completion deadline: {completed_time.strftime('%H:%M')} > {sla.required_completion_time}"
            ),
        )

        if not passed:
            self.alerts.emit_sla_breach(
                pipeline_name=pipeline_name,
                sla_type="completion_time",
                threshold=sla.required_completion_time,
                actual=completed_time.strftime("%H:%M"),
            )

        return result

    def check_freshness(
        self,
        table_name: str,
        max_age_hours: Optional[float] = None,
    ) -> FreshnessResult:
        """
        Check if table data is fresh.

        Args:
            table_name: Name of the table to check
            max_age_hours: Override SLA max age (uses SLA default if not provided)

        Returns:
            FreshnessResult with freshness status
        """
        sla = self.get_sla(table_name)
        if max_age_hours is None:
            max_age_hours = sla.max_data_latency_hours if sla else 24.0

        # Query for last load timestamp
        last_load_at = None
        hours_stale = float("inf")

        if self.db:
            try:
                # Check for _loaded_at or _processed_at column
                query = f"""
                    SELECT MAX(COALESCE(_loaded_at, _processed_at, created_at))
                    FROM {table_name}
                """
                result = self.db.execute(query).fetchone()
                if result and result[0]:
                    last_load_at = result[0]
                    hours_stale = (datetime.utcnow() - last_load_at).total_seconds() / 3600
            except Exception as e:
                self.logger.error(
                    "freshness_check_failed",
                    table=table_name,
                    error=str(e),
                )

        is_fresh = hours_stale <= max_age_hours

        result = FreshnessResult(
            table_name=table_name,
            is_fresh=is_fresh,
            last_load_at=last_load_at,
            hours_stale=hours_stale,
            max_age_hours=max_age_hours,
        )

        if not is_fresh:
            self.alerts.emit_freshness_violation(
                table_name=table_name,
                max_age_hours=max_age_hours,
                actual_age_hours=hours_stale,
            )

        self.logger.info(
            "freshness_check",
            table=table_name,
            is_fresh=is_fresh,
            hours_stale=round(hours_stale, 1),
            max_age_hours=max_age_hours,
        )

        return result

    def check_all_freshness(self) -> dict[str, FreshnessResult]:
        """
        Check freshness for all tables with defined SLAs.

        Returns:
            Dict mapping table name to FreshnessResult
        """
        results = {}

        for name, sla in self.slas.items():
            # Only check tables (not pipelines)
            if name.startswith("fct_") or name.startswith("dim_"):
                results[name] = self.check_freshness(name)

        return results

    def check_pipeline_sla(
        self,
        pipeline_name: str,
        started_at: datetime,
        completed_at: datetime,
        rows_processed: int = 0,
    ) -> list[SLAResult]:
        """
        Run all SLA checks for a pipeline.

        Args:
            pipeline_name: Name of the pipeline
            started_at: When the pipeline started
            completed_at: When the pipeline completed
            rows_processed: Number of rows processed

        Returns:
            List of SLAResult for each check
        """
        results = []

        # Runtime check
        results.append(
            self.check_pipeline_runtime(pipeline_name, started_at, completed_at)
        )

        # Completion time check
        results.append(self.check_completion_time(pipeline_name, completed_at))

        # Minimum rows check
        sla = self.get_sla(pipeline_name)
        if sla and sla.min_rows_per_run > 0:
            passed = rows_processed >= sla.min_rows_per_run
            result = SLAResult(
                pipeline_name=pipeline_name,
                sla_type="min_rows",
                passed=passed,
                threshold=f"{sla.min_rows_per_run} rows",
                actual=f"{rows_processed} rows",
                message=(
                    f"Processed {rows_processed} rows"
                    if passed
                    else f"Below minimum rows: {rows_processed} < {sla.min_rows_per_run}"
                ),
            )

            if not passed:
                self.alerts.emit_sla_breach(
                    pipeline_name=pipeline_name,
                    sla_type="min_rows",
                    threshold=sla.min_rows_per_run,
                    actual=rows_processed,
                )

            results.append(result)

        return results

    def generate_sla_report(self) -> str:
        """
        Generate a markdown report of current SLA status.

        Returns:
            Markdown formatted SLA report
        """
        lines = [
            "# SLA Status Report",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            "## Freshness Status",
            "",
            "| Table | Status | Hours Stale | SLA (hours) |",
            "|-------|--------|-------------|-------------|",
        ]

        freshness_results = self.check_all_freshness()
        for name, result in freshness_results.items():
            status = "OK" if result.is_fresh else "STALE"
            lines.append(
                f"| {name} | {status} | {result.hours_stale:.1f} | {result.max_age_hours} |"
            )

        lines.extend([
            "",
            "## SLA Definitions",
            "",
            "| Pipeline/Table | Max Runtime | Max Latency | Completion Time |",
            "|----------------|-------------|-------------|-----------------|",
        ])

        for name, sla in self.slas.items():
            completion = sla.required_completion_time or "-"
            lines.append(
                f"| {name} | {sla.max_runtime_minutes}m | {sla.max_data_latency_hours}h | {completion} |"
            )

        return "\n".join(lines)
