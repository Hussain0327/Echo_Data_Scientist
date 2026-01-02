"""
PostgreSQL Partition Management Script.

Manages partitions for the transactions_partitioned table:
- Creates new partitions for upcoming months
- Archives old partitions (optional)
- Runs maintenance operations (VACUUM, ANALYZE)
- Reports partition statistics

Usage:
    python manage_partitions.py create --months-ahead 3
    python manage_partitions.py archive --older-than 24
    python manage_partitions.py stats
    python manage_partitions.py maintain
"""

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from psycopg2 import sql


@dataclass
class PartitionInfo:
    """Information about a partition."""

    name: str
    year: int
    month: int
    size_bytes: int
    row_count: int
    is_empty: bool


def get_connection():
    """Get database connection from environment."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://echo_user:echo_password@localhost:5432/echo_db",
    )

    # Parse URL for psycopg2
    if database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")

    return psycopg2.connect(database_url)


def partition_exists(conn, partition_name: str) -> bool:
    """Check if a partition already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE tablename = %s
            )
            """,
            (partition_name,),
        )
        return cur.fetchone()[0]


def create_partition(
    conn,
    year: int,
    month: int,
    table_name: str = "transactions_partitioned",
) -> bool:
    """
    Create a new monthly partition.

    Args:
        conn: Database connection
        year: Year for partition
        month: Month for partition
        table_name: Parent table name

    Returns:
        True if created, False if already exists
    """
    partition_name = f"transactions_y{year}m{month:02d}"

    if partition_exists(conn, partition_name):
        print(f"  Partition {partition_name} already exists, skipping")
        return False

    # Calculate date range
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    # Create partition
    with conn.cursor() as cur:
        create_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {partition}
            PARTITION OF {parent}
            FOR VALUES FROM (%s) TO (%s)
            """
        ).format(
            partition=sql.Identifier(partition_name),
            parent=sql.Identifier(table_name),
        )

        cur.execute(create_sql, (start_date.date(), end_date.date()))
        conn.commit()

    print(f"  Created partition {partition_name} ({start_date.date()} to {end_date.date()})")
    return True


def cmd_create(args: argparse.Namespace) -> int:
    """Create partitions for upcoming months."""
    print(f"\nCreating partitions for next {args.months_ahead} months...")

    conn = get_connection()
    today = datetime.now()
    created_count = 0

    for i in range(args.months_ahead):
        future_date = today + timedelta(days=30 * (i + 1))
        year = future_date.year
        month = future_date.month

        if create_partition(conn, year, month):
            created_count += 1

    conn.close()

    print(f"\nCreated {created_count} new partitions")
    return 0


def get_partition_stats(conn) -> list[PartitionInfo]:
    """Get statistics for all partitions."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.relname AS partition_name,
                pg_total_relation_size(c.oid) AS size_bytes,
                COALESCE(s.n_live_tup, 0) AS row_count
            FROM pg_class c
            JOIN pg_inherits i ON c.oid = i.inhrelid
            JOIN pg_class p ON i.inhparent = p.oid
            LEFT JOIN pg_stat_user_tables s ON c.relname = s.relname
            WHERE p.relname = 'transactions_partitioned'
              AND c.relname LIKE 'transactions_y%'
            ORDER BY c.relname
            """
        )

        partitions = []
        for row in cur.fetchall():
            name = row[0]
            # Parse year/month from name (transactions_y2024m01)
            try:
                parts = name.replace("transactions_y", "").split("m")
                year = int(parts[0])
                month = int(parts[1])
            except (IndexError, ValueError):
                year = 0
                month = 0

            partitions.append(
                PartitionInfo(
                    name=name,
                    year=year,
                    month=month,
                    size_bytes=row[1],
                    row_count=row[2],
                    is_empty=row[2] == 0,
                )
            )

        return partitions


def cmd_stats(args: argparse.Namespace) -> int:
    """Display partition statistics."""
    print("\nPartition Statistics")
    print("=" * 80)

    conn = get_connection()
    partitions = get_partition_stats(conn)
    conn.close()

    if not partitions:
        print("No partitions found.")
        return 0

    total_size = 0
    total_rows = 0
    empty_count = 0

    print(f"\n{'Partition':<30} {'Year-Month':<12} {'Rows':>12} {'Size':>12}")
    print("-" * 80)

    for p in partitions:
        size_mb = p.size_bytes / (1024 * 1024)
        total_size += p.size_bytes
        total_rows += p.row_count
        if p.is_empty:
            empty_count += 1

        print(f"{p.name:<30} {p.year}-{p.month:02d}      {p.row_count:>12,} {size_mb:>10.2f} MB")

    print("-" * 80)
    print(f"{'Total':<30} {'':<12} {total_rows:>12,} {total_size / (1024 * 1024):>10.2f} MB")
    print(f"\nPartition count: {len(partitions)}")
    print(f"Empty partitions: {empty_count}")

    return 0


def cmd_archive(args: argparse.Namespace) -> int:
    """Archive (detach) old partitions."""
    print(f"\nArchiving partitions older than {args.older_than} months...")

    conn = get_connection()
    partitions = get_partition_stats(conn)

    cutoff_date = datetime.now() - timedelta(days=30 * args.older_than)
    archived_count = 0

    for p in partitions:
        if p.year == 0:
            continue

        partition_date = datetime(p.year, p.month, 1)
        if partition_date < cutoff_date:
            if args.dry_run:
                print(f"  [DRY RUN] Would archive {p.name}")
            else:
                with conn.cursor() as cur:
                    # Detach partition (keeps data, removes from parent)
                    detach_sql = sql.SQL(
                        """
                        ALTER TABLE transactions_partitioned
                        DETACH PARTITION {partition}
                        """
                    ).format(partition=sql.Identifier(p.name))

                    cur.execute(detach_sql)
                    conn.commit()

                    print(f"  Archived {p.name} ({p.row_count:,} rows)")
                    archived_count += 1

    conn.close()

    action = "Would archive" if args.dry_run else "Archived"
    print(f"\n{action} {archived_count} partitions")
    return 0


def cmd_maintain(args: argparse.Namespace) -> int:
    """Run maintenance operations on partitions."""
    print("\nRunning partition maintenance...")

    conn = get_connection()
    partitions = get_partition_stats(conn)

    with conn.cursor() as cur:
        for p in partitions:
            if p.is_empty and not args.include_empty:
                continue

            print(f"  Maintaining {p.name}...")

            # ANALYZE for query planner statistics
            cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(p.name)))

            # VACUUM for dead tuple cleanup (non-blocking)
            if args.vacuum:
                # Need autocommit for VACUUM
                old_autocommit = conn.autocommit
                conn.autocommit = True
                cur.execute(sql.SQL("VACUUM {}").format(sql.Identifier(p.name)))
                conn.autocommit = old_autocommit

    conn.commit()
    conn.close()

    print(f"\nMaintenance complete for {len(partitions)} partitions")
    return 0


def cmd_generate_ddl(args: argparse.Namespace) -> int:
    """Generate DDL for partitions without executing."""
    print(f"\n-- Partition DDL for {args.start_year} to {args.end_year}")
    print("-- Generated by manage_partitions.py")
    print()

    for year in range(args.start_year, args.end_year + 1):
        print(f"-- Year {year}")
        for month in range(1, 13):
            partition_name = f"transactions_y{year}m{month:02d}"
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1)
            else:
                end_date = datetime(year, month + 1, 1)

            print(
                f"CREATE TABLE IF NOT EXISTS {partition_name} "
                f"PARTITION OF transactions_partitioned"
            )
            print(f"    FOR VALUES FROM ('{start_date.date()}') TO ('{end_date.date()}');")
        print()

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PostgreSQL Partition Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create new partitions")
    create_parser.add_argument(
        "--months-ahead",
        type=int,
        default=3,
        help="Number of months ahead to create partitions (default: 3)",
    )
    create_parser.set_defaults(func=cmd_create)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show partition statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Archive command
    archive_parser = subparsers.add_parser("archive", help="Archive old partitions")
    archive_parser.add_argument(
        "--older-than",
        type=int,
        default=24,
        help="Archive partitions older than N months (default: 24)",
    )
    archive_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be archived without doing it",
    )
    archive_parser.set_defaults(func=cmd_archive)

    # Maintain command
    maintain_parser = subparsers.add_parser("maintain", help="Run maintenance operations")
    maintain_parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Also run VACUUM (can be slow for large partitions)",
    )
    maintain_parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Include empty partitions in maintenance",
    )
    maintain_parser.set_defaults(func=cmd_maintain)

    # Generate DDL command
    ddl_parser = subparsers.add_parser("generate-ddl", help="Generate partition DDL")
    ddl_parser.add_argument(
        "--start-year",
        type=int,
        default=2023,
        help="Start year (default: 2023)",
    )
    ddl_parser.add_argument(
        "--end-year",
        type=int,
        default=2026,
        help="End year (default: 2026)",
    )
    ddl_parser.set_defaults(func=cmd_generate_ddl)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
