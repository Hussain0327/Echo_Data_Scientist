"""
Dead Letter Queue (DLQ) for Failed Records.

Provides a Redis-backed queue for storing and reprocessing records
that failed during pipeline execution.

Features:
- Per-source-table queues
- Retry tracking with max retries
- Statistics and monitoring
- Batch reprocessing
- Permanent failure archival

Usage:
    from observability.dead_letter_queue import DeadLetterQueue, FailedRecord

    dlq = DeadLetterQueue(redis_client)

    # Push a failed record
    dlq.push(FailedRecord(
        record_id="txn_123",
        source_table="transactions",
        raw_data={"amount": "invalid"},
        error_message="Could not parse amount",
        error_type="ValidationError",
        pipeline_run_id="run_abc"
    ))

    # Get DLQ stats
    stats = dlq.get_stats()

    # Reprocess failed records
    for record in dlq.reprocess_batch("transactions", batch_size=100):
        try:
            process_record(record.raw_data)
            dlq.mark_processed(record)
        except Exception as e:
            dlq.mark_failed(record, str(e))
"""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Generator, Optional

import structlog


@dataclass
class FailedRecord:
    """
    Represents a record that failed processing.

    Attributes:
        record_id: Unique identifier for the record
        source_table: Table/source the record came from
        raw_data: The original record data
        error_message: Human-readable error description
        error_type: Exception class name
        pipeline_run_id: ID of the pipeline run that failed
        failed_at: When the failure occurred
        retry_count: Number of reprocess attempts
        max_retries: Maximum allowed retries before permanent failure
        last_error: Most recent error message (if retried)
    """

    record_id: str
    source_table: str
    raw_data: dict[str, Any]
    error_message: str
    error_type: str
    pipeline_run_id: str
    failed_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    max_retries: int = 3
    last_error: Optional[str] = None

    def to_json(self) -> str:
        """Serialize to JSON for Redis storage."""
        data = asdict(self)
        data["failed_at"] = self.failed_at.isoformat()
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "FailedRecord":
        """Deserialize from JSON."""
        data = json.loads(json_str)
        data["failed_at"] = datetime.fromisoformat(data["failed_at"])
        return cls(**data)

    @property
    def can_retry(self) -> bool:
        """Check if record can be retried."""
        return self.retry_count < self.max_retries


@dataclass
class DLQStats:
    """Statistics for a DLQ queue."""

    source_table: str
    pending_count: int
    permanent_failure_count: int
    oldest_record_age_hours: Optional[float]
    newest_record_age_hours: Optional[float]


class DeadLetterQueue:
    """
    Redis-backed dead letter queue for failed records.

    Queue naming convention:
    - dlq:{source_table} - Main queue for failed records
    - dlq:{source_table}:permanent - Records that exceeded max retries
    - dlq:{source_table}:processing - Records currently being reprocessed
    """

    def __init__(
        self,
        redis_client: Any,
        queue_prefix: str = "dlq",
        default_max_retries: int = 3,
    ):
        """
        Initialize DeadLetterQueue.

        Args:
            redis_client: Redis client instance
            queue_prefix: Prefix for queue names
            default_max_retries: Default max retries for new records
        """
        self.redis = redis_client
        self.queue_prefix = queue_prefix
        self.default_max_retries = int(
            os.getenv("DLQ_MAX_RETRIES", default_max_retries)
        )
        self.logger = structlog.get_logger("dlq")

    def _queue_name(self, source_table: str, suffix: str = "") -> str:
        """Generate queue name."""
        if suffix:
            return f"{self.queue_prefix}:{source_table}:{suffix}"
        return f"{self.queue_prefix}:{source_table}"

    def push(self, record: FailedRecord) -> bool:
        """
        Add a failed record to the DLQ.

        Args:
            record: The failed record to store

        Returns:
            True if successfully added
        """
        queue = self._queue_name(record.source_table)

        try:
            self.redis.lpush(queue, record.to_json())

            self.logger.info(
                "dlq_record_added",
                record_id=record.record_id,
                source_table=record.source_table,
                error_type=record.error_type,
                retry_count=record.retry_count,
            )

            return True

        except Exception as e:
            self.logger.error(
                "dlq_push_failed",
                record_id=record.record_id,
                error=str(e),
            )
            return False

    def pop(self, source_table: str) -> Optional[FailedRecord]:
        """
        Get the next record for reprocessing.

        Moves the record to the processing queue until marked as
        processed or failed.

        Args:
            source_table: Table to get records from

        Returns:
            FailedRecord or None if queue is empty
        """
        queue = self._queue_name(source_table)
        processing = self._queue_name(source_table, "processing")

        try:
            # Atomically move from main queue to processing
            data = self.redis.rpoplpush(queue, processing)
            if data:
                return FailedRecord.from_json(data)
            return None

        except Exception as e:
            self.logger.error(
                "dlq_pop_failed",
                source_table=source_table,
                error=str(e),
            )
            return None

    def mark_processed(self, record: FailedRecord) -> bool:
        """
        Mark a record as successfully reprocessed.

        Removes the record from the processing queue.

        Args:
            record: The record that was successfully processed

        Returns:
            True if successfully removed
        """
        processing = self._queue_name(record.source_table, "processing")

        try:
            self.redis.lrem(processing, 1, record.to_json())

            self.logger.info(
                "dlq_record_processed",
                record_id=record.record_id,
                source_table=record.source_table,
                retry_count=record.retry_count,
            )

            return True

        except Exception as e:
            self.logger.error(
                "dlq_mark_processed_failed",
                record_id=record.record_id,
                error=str(e),
            )
            return False

    def mark_failed(self, record: FailedRecord, error: str) -> bool:
        """
        Mark a reprocess attempt as failed.

        Increments retry count and either returns to main queue
        or moves to permanent failure queue.

        Args:
            record: The record that failed reprocessing
            error: Error message from the failure

        Returns:
            True if successfully handled
        """
        processing = self._queue_name(record.source_table, "processing")

        try:
            # Remove from processing queue
            self.redis.lrem(processing, 1, record.to_json())

            # Update record
            record.retry_count += 1
            record.last_error = error

            if record.can_retry:
                # Return to main queue for later retry
                queue = self._queue_name(record.source_table)
                self.redis.lpush(queue, record.to_json())

                self.logger.warning(
                    "dlq_record_retry_scheduled",
                    record_id=record.record_id,
                    source_table=record.source_table,
                    retry_count=record.retry_count,
                    error=error,
                )
            else:
                # Move to permanent failure queue
                permanent = self._queue_name(record.source_table, "permanent")
                self.redis.lpush(permanent, record.to_json())

                self.logger.error(
                    "dlq_record_permanent_failure",
                    record_id=record.record_id,
                    source_table=record.source_table,
                    retry_count=record.retry_count,
                    error=error,
                )

            return True

        except Exception as e:
            self.logger.error(
                "dlq_mark_failed_error",
                record_id=record.record_id,
                error=str(e),
            )
            return False

    def get_stats(self) -> dict[str, DLQStats]:
        """
        Get statistics for all DLQ queues.

        Returns:
            Dict mapping source_table to DLQStats
        """
        stats = {}

        try:
            # Find all DLQ queues
            pattern = f"{self.queue_prefix}:*"
            keys = self.redis.keys(pattern)

            # Group by source table
            tables = set()
            for key in keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                parts = key_str.replace(f"{self.queue_prefix}:", "").split(":")
                if parts:
                    tables.add(parts[0])

            for table in tables:
                main_queue = self._queue_name(table)
                permanent_queue = self._queue_name(table, "permanent")

                pending = self.redis.llen(main_queue)
                permanent = self.redis.llen(permanent_queue)

                # Get age of oldest record
                oldest_age = None
                oldest = self.redis.lindex(main_queue, -1)  # Oldest is at end
                if oldest:
                    oldest_str = oldest.decode() if isinstance(oldest, bytes) else oldest
                    oldest_record = FailedRecord.from_json(oldest_str)
                    oldest_age = (datetime.utcnow() - oldest_record.failed_at).total_seconds() / 3600

                # Get age of newest record
                newest_age = None
                newest = self.redis.lindex(main_queue, 0)  # Newest is at front
                if newest:
                    newest_str = newest.decode() if isinstance(newest, bytes) else newest
                    newest_record = FailedRecord.from_json(newest_str)
                    newest_age = (datetime.utcnow() - newest_record.failed_at).total_seconds() / 3600

                stats[table] = DLQStats(
                    source_table=table,
                    pending_count=pending,
                    permanent_failure_count=permanent,
                    oldest_record_age_hours=oldest_age,
                    newest_record_age_hours=newest_age,
                )

        except Exception as e:
            self.logger.error("dlq_get_stats_failed", error=str(e))

        return stats

    def reprocess_batch(
        self,
        source_table: str,
        batch_size: int = 100,
    ) -> Generator[FailedRecord, None, None]:
        """
        Yield records for reprocessing.

        Records are moved to the processing queue and yielded.
        Caller should call mark_processed() or mark_failed() for each.

        Args:
            source_table: Table to reprocess records from
            batch_size: Maximum number of records to process

        Yields:
            FailedRecord instances ready for reprocessing
        """
        processed = 0

        while processed < batch_size:
            record = self.pop(source_table)
            if record is None:
                break

            processed += 1
            yield record

        self.logger.info(
            "dlq_batch_started",
            source_table=source_table,
            batch_size=processed,
        )

    def clear(self, source_table: str, include_permanent: bool = False):
        """
        Clear all records from a DLQ.

        Args:
            source_table: Table queue to clear
            include_permanent: Also clear permanent failure queue
        """
        queues = [
            self._queue_name(source_table),
            self._queue_name(source_table, "processing"),
        ]

        if include_permanent:
            queues.append(self._queue_name(source_table, "permanent"))

        for queue in queues:
            self.redis.delete(queue)

        self.logger.warning(
            "dlq_cleared",
            source_table=source_table,
            include_permanent=include_permanent,
        )

    def recover_processing(self, source_table: str) -> int:
        """
        Recover records stuck in processing queue.

        Moves all records from processing back to main queue.
        Useful after worker crash/restart.

        Args:
            source_table: Table to recover

        Returns:
            Number of records recovered
        """
        processing = self._queue_name(source_table, "processing")
        main = self._queue_name(source_table)

        recovered = 0
        while True:
            data = self.redis.rpoplpush(processing, main)
            if data is None:
                break
            recovered += 1

        if recovered > 0:
            self.logger.warning(
                "dlq_processing_recovered",
                source_table=source_table,
                recovered_count=recovered,
            )

        return recovered
