"""
Structured Alerting System.

Provides a log-based alerting system that emits structured events
which can be picked up by any log aggregation system (DataDog,
Splunk, CloudWatch, etc.).

Features:
- Severity levels (INFO, WARNING, CRITICAL)
- Alert types (SLA breach, data quality, pipeline failure, etc.)
- Structured JSON logging via structlog
- Optional persistence to database
- Alert deduplication and throttling

Usage:
    from observability.alerts import AlertManager, Alert, AlertType, AlertSeverity

    alert_manager = AlertManager()

    alert = Alert(
        alert_type=AlertType.DATA_QUALITY_FAILURE,
        severity=AlertSeverity.WARNING,
        pipeline_name="daily_metrics_pipeline",
        message="15% null values detected in amount column",
        details={"column": "amount", "null_pct": 15.3}
    )

    alert_manager.emit(alert)
"""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

import structlog


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

    @property
    def emoji(self) -> str:
        """Get emoji for severity (useful for Slack/Discord)."""
        return {
            "info": "information_source:",
            "warning": "warning:",
            "critical": "rotating_light:",
        }.get(self.value, "")


class AlertType(Enum):
    """Types of alerts the system can emit."""

    # Pipeline health
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILURE = "pipeline_failure"
    TASK_FAILURE = "task_failure"

    # Data quality
    DATA_QUALITY_FAILURE = "data_quality_failure"
    SCHEMA_DRIFT = "schema_drift"
    ANOMALY_DETECTED = "anomaly_detected"

    # SLA and freshness
    SLA_BREACH = "sla_breach"
    SLA_WARNING = "sla_warning"
    FRESHNESS_VIOLATION = "freshness_violation"

    # Dead letter queue
    DLQ_THRESHOLD_EXCEEDED = "dlq_threshold_exceeded"
    DLQ_REPROCESS_FAILED = "dlq_reprocess_failed"

    # System health
    RESOURCE_WARNING = "resource_warning"
    CONNECTION_FAILURE = "connection_failure"


@dataclass
class Alert:
    """
    Represents a single alert event.

    Attributes:
        alert_type: Category of alert
        severity: How critical is this alert
        pipeline_name: Name of the pipeline/process that triggered it
        message: Human-readable description
        details: Additional structured data
        timestamp: When the alert occurred
        alert_id: Unique identifier for deduplication
    """

    alert_type: AlertType
    severity: AlertSeverity
    pipeline_name: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    alert_id: Optional[str] = None

    def __post_init__(self):
        """Generate alert_id if not provided."""
        if self.alert_id is None:
            # Create deterministic ID for deduplication
            key_parts = [
                self.alert_type.value,
                self.pipeline_name,
                self.timestamp.strftime("%Y%m%d%H"),  # Hour granularity
            ]
            self.alert_id = "_".join(key_parts)

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to dictionary for JSON serialization."""
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "pipeline_name": self.pipeline_name,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_structured_log(self) -> dict[str, Any]:
        """Format for structured logging."""
        return {
            "event": "ALERT",
            "alert_id": self.alert_id,
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "pipeline": self.pipeline_name,
            "message": self.message,
            **self.details,
        }


class AlertManager:
    """
    Manages alert emission, deduplication, and throttling.

    Alerts are emitted as structured log events that can be:
    - Picked up by log aggregation systems
    - Persisted to a database for historical analysis
    - Forwarded to notification systems

    Configuration via environment variables:
    - ALERT_THROTTLE_MINUTES: Minimum time between duplicate alerts (default: 15)
    - ALERT_PERSIST_TO_DB: Whether to persist alerts to database (default: false)
    """

    def __init__(
        self,
        throttle_minutes: int = 15,
        persist_to_db: bool = False,
        db_connection: Optional[Any] = None,
    ):
        """
        Initialize AlertManager.

        Args:
            throttle_minutes: Minimum minutes between duplicate alerts
            persist_to_db: Whether to persist alerts to database
            db_connection: Database connection for persistence
        """
        self.throttle_minutes = int(
            os.getenv("ALERT_THROTTLE_MINUTES", throttle_minutes)
        )
        self.persist_to_db = (
            os.getenv("ALERT_PERSIST_TO_DB", str(persist_to_db)).lower() == "true"
        )
        self.db_connection = db_connection

        # Track recent alerts for throttling
        self._recent_alerts: dict[str, datetime] = {}

        # Configure structured logger
        self.logger = structlog.get_logger("alerts")

    def _should_throttle(self, alert: Alert) -> bool:
        """Check if alert should be throttled (duplicate suppression)."""
        if alert.alert_id in self._recent_alerts:
            last_sent = self._recent_alerts[alert.alert_id]
            if datetime.utcnow() - last_sent < timedelta(minutes=self.throttle_minutes):
                return True
        return False

    def _update_throttle_cache(self, alert: Alert):
        """Update throttle cache with new alert."""
        self._recent_alerts[alert.alert_id] = datetime.utcnow()

        # Clean old entries (older than 1 hour)
        cutoff = datetime.utcnow() - timedelta(hours=1)
        self._recent_alerts = {
            k: v for k, v in self._recent_alerts.items() if v > cutoff
        }

    def emit(self, alert: Alert, force: bool = False) -> bool:
        """
        Emit an alert.

        Args:
            alert: The alert to emit
            force: Bypass throttling

        Returns:
            True if alert was emitted, False if throttled
        """
        # Check throttling
        if not force and self._should_throttle(alert):
            self.logger.debug(
                "alert_throttled",
                alert_id=alert.alert_id,
                alert_type=alert.alert_type.value,
            )
            return False

        # Emit structured log
        log_data = alert.to_structured_log()

        if alert.severity == AlertSeverity.CRITICAL:
            self.logger.critical(**log_data)
        elif alert.severity == AlertSeverity.WARNING:
            self.logger.warning(**log_data)
        else:
            self.logger.info(**log_data)

        # Persist to database if configured
        if self.persist_to_db and self.db_connection:
            self._persist_alert(alert)

        # Update throttle cache
        self._update_throttle_cache(alert)

        return True

    def _persist_alert(self, alert: Alert):
        """Persist alert to database."""
        try:
            # This would insert into an alerts table
            # For now, just log the intent
            self.logger.debug(
                "alert_persisted",
                alert_id=alert.alert_id,
            )
        except Exception as e:
            self.logger.error(
                "alert_persistence_failed",
                alert_id=alert.alert_id,
                error=str(e),
            )

    def emit_pipeline_started(self, pipeline_name: str, **details):
        """Convenience method for pipeline start alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.PIPELINE_STARTED,
                severity=AlertSeverity.INFO,
                pipeline_name=pipeline_name,
                message=f"Pipeline {pipeline_name} started",
                details=details,
            )
        )

    def emit_pipeline_completed(
        self,
        pipeline_name: str,
        duration_seconds: float,
        rows_processed: int = 0,
        **details,
    ):
        """Convenience method for pipeline completion alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.PIPELINE_COMPLETED,
                severity=AlertSeverity.INFO,
                pipeline_name=pipeline_name,
                message=f"Pipeline {pipeline_name} completed in {duration_seconds:.1f}s",
                details={
                    "duration_seconds": duration_seconds,
                    "rows_processed": rows_processed,
                    **details,
                },
            )
        )

    def emit_pipeline_failure(
        self,
        pipeline_name: str,
        error: str,
        **details,
    ):
        """Convenience method for pipeline failure alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.PIPELINE_FAILURE,
                severity=AlertSeverity.CRITICAL,
                pipeline_name=pipeline_name,
                message=f"Pipeline {pipeline_name} failed: {error}",
                details={"error": error, **details},
            ),
            force=True,  # Always emit failures
        )

    def emit_data_quality_failure(
        self,
        pipeline_name: str,
        check_name: str,
        expected: Any,
        actual: Any,
        **details,
    ):
        """Convenience method for data quality alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.DATA_QUALITY_FAILURE,
                severity=AlertSeverity.WARNING,
                pipeline_name=pipeline_name,
                message=f"Data quality check '{check_name}' failed",
                details={
                    "check_name": check_name,
                    "expected": str(expected),
                    "actual": str(actual),
                    **details,
                },
            )
        )

    def emit_sla_breach(
        self,
        pipeline_name: str,
        sla_type: str,
        threshold: Any,
        actual: Any,
        **details,
    ):
        """Convenience method for SLA breach alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.SLA_BREACH,
                severity=AlertSeverity.CRITICAL,
                pipeline_name=pipeline_name,
                message=f"SLA breach: {sla_type} exceeded threshold",
                details={
                    "sla_type": sla_type,
                    "threshold": str(threshold),
                    "actual": str(actual),
                    **details,
                },
            ),
            force=True,
        )

    def emit_freshness_violation(
        self,
        table_name: str,
        max_age_hours: float,
        actual_age_hours: float,
        **details,
    ):
        """Convenience method for freshness violation alerts."""
        self.emit(
            Alert(
                alert_type=AlertType.FRESHNESS_VIOLATION,
                severity=AlertSeverity.WARNING,
                pipeline_name=table_name,
                message=f"Table {table_name} is {actual_age_hours:.1f}h stale (max: {max_age_hours}h)",
                details={
                    "max_age_hours": max_age_hours,
                    "actual_age_hours": actual_age_hours,
                    **details,
                },
            )
        )


# Singleton instance for convenience
_default_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get or create the default AlertManager instance."""
    global _default_manager
    if _default_manager is None:
        _default_manager = AlertManager()
    return _default_manager
