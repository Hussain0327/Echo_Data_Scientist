"""
Observability module for Echo Analytics Platform.

Provides production-grade monitoring and alerting:
- Structured alerts with severity levels
- Dead letter queue for failed records
- SLA monitoring and freshness checks
"""

from observability.alerts import Alert, AlertManager, AlertSeverity, AlertType
from observability.dead_letter_queue import DeadLetterQueue, FailedRecord
from observability.sla_monitor import FreshnessResult, SLADefinition, SLAMonitor

__all__ = [
    "Alert",
    "AlertManager",
    "AlertSeverity",
    "AlertType",
    "DeadLetterQueue",
    "FailedRecord",
    "SLAMonitor",
    "SLADefinition",
    "FreshnessResult",
]
