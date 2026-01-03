"""
Query Benchmarking Framework.

Measures query performance across different data scales with:
- Cold run (no cache) vs warm run (with cache) comparisons
- EXPLAIN ANALYZE for query plan analysis
- Partition pruning verification
- Index usage tracking

Usage:
    from benchmarks import QueryBenchmark

    benchmark = QueryBenchmark()
    results = benchmark.run_all(table="transactions_partitioned")
    benchmark.print_results(results)
"""

import gc
import os
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import psycopg2
from psycopg2 import sql


class QueryCategory(Enum):
    """Query categories for grouping benchmarks."""

    AGGREGATION = "aggregation"
    TIME_SERIES = "time_series"
    CUSTOMER_ANALYTICS = "customer_analytics"
    JOINS = "joins"
    WINDOW_FUNCTIONS = "window_functions"


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    query_name: str
    category: QueryCategory
    cold_time_ms: float
    warm_time_ms: float
    rows_returned: int
    rows_scanned: Optional[int] = None
    partitions_scanned: Optional[int] = None
    index_used: bool = False
    seq_scan: bool = False
    execution_plan: str = ""
    error: Optional[str] = None

    @property
    def cache_speedup(self) -> float:
        """Calculate speedup from caching."""
        if self.warm_time_ms > 0:
            return self.cold_time_ms / self.warm_time_ms
        return 1.0


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results for a scale."""

    scale: str
    table: str
    row_count: int
    results: list[BenchmarkResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_markdown_table(self) -> str:
        """Generate markdown table of results."""
        lines = [
            f"### {self.scale} Scale ({self.row_count:,} rows)",
            "",
            "| Query | Category | Cold (ms) | Warm (ms) | Speedup | Rows | Index | Notes |",
            "|-------|----------|-----------|-----------|---------|------|-------|-------|",
        ]

        for r in self.results:
            notes = []
            if r.seq_scan:
                notes.append("seq scan")
            if r.partitions_scanned and r.partitions_scanned < 5:
                notes.append(f"{r.partitions_scanned} partitions")
            if r.error:
                notes.append(f"ERROR: {r.error[:20]}")

            lines.append(
                f"| {r.query_name} | {r.category.value} | "
                f"{r.cold_time_ms:.1f} | {r.warm_time_ms:.1f} | "
                f"{r.cache_speedup:.1f}x | {r.rows_returned:,} | "
                f"{'Yes' if r.index_used else 'No'} | {', '.join(notes)} |"
            )

        return "\n".join(lines)


class QueryBenchmark:
    """
    Benchmarks analytical queries against the database.

    Runs queries multiple times and collects performance metrics including
    execution time, rows scanned, index usage, and partition pruning.
    """

    # Standard benchmark queries
    QUERIES = {
        "monthly_revenue": {
            "category": QueryCategory.AGGREGATION,
            "sql": """
                SELECT
                    DATE_TRUNC('month', transaction_date) AS month,
                    SUM(amount) AS revenue,
                    COUNT(*) AS transaction_count,
                    COUNT(DISTINCT customer_id) AS unique_customers
                FROM {table}
                WHERE transaction_date >= %s AND transaction_date < %s
                  AND status IN ('completed', 'paid')
                GROUP BY DATE_TRUNC('month', transaction_date)
                ORDER BY month
            """,
            "params_func": lambda: (
                datetime(2024, 1, 1),
                datetime(2025, 1, 1),
            ),
        },
        "daily_revenue_trend": {
            "category": QueryCategory.TIME_SERIES,
            "sql": """
                SELECT
                    transaction_date,
                    SUM(amount) AS daily_revenue,
                    AVG(SUM(amount)) OVER (
                        ORDER BY transaction_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS revenue_7d_ma
                FROM {table}
                WHERE transaction_date >= %s AND transaction_date < %s
                  AND status IN ('completed', 'paid')
                GROUP BY transaction_date
                ORDER BY transaction_date
            """,
            "params_func": lambda: (
                datetime(2024, 10, 1),
                datetime(2025, 1, 1),
            ),
        },
        "customer_ltv": {
            "category": QueryCategory.CUSTOMER_ANALYTICS,
            "sql": """
                SELECT
                    customer_id,
                    SUM(amount) AS lifetime_value,
                    COUNT(*) AS transaction_count,
                    MIN(transaction_date) AS first_purchase,
                    MAX(transaction_date) AS last_purchase,
                    MAX(transaction_date) - MIN(transaction_date) AS customer_tenure_days
                FROM {table}
                WHERE status IN ('completed', 'paid')
                GROUP BY customer_id
                ORDER BY lifetime_value DESC
                LIMIT 100
            """,
            "params_func": lambda: (),
        },
        "customer_cohort_retention": {
            "category": QueryCategory.CUSTOMER_ANALYTICS,
            "sql": """
                WITH first_purchase AS (
                    SELECT
                        customer_id,
                        DATE_TRUNC('month', MIN(transaction_date)) AS cohort_month
                    FROM {table}
                    WHERE status IN ('completed', 'paid')
                    GROUP BY customer_id
                ),
                monthly_activity AS (
                    SELECT
                        customer_id,
                        DATE_TRUNC('month', transaction_date) AS activity_month
                    FROM {table}
                    WHERE status IN ('completed', 'paid')
                    GROUP BY customer_id, DATE_TRUNC('month', transaction_date)
                )
                SELECT
                    fp.cohort_month,
                    EXTRACT(MONTH FROM AGE(ma.activity_month, fp.cohort_month)) AS months_since_first,
                    COUNT(DISTINCT ma.customer_id) AS active_customers
                FROM first_purchase fp
                JOIN monthly_activity ma ON fp.customer_id = ma.customer_id
                WHERE fp.cohort_month >= %s
                GROUP BY fp.cohort_month, months_since_first
                ORDER BY fp.cohort_month, months_since_first
            """,
            "params_func": lambda: (datetime(2024, 1, 1),),
        },
        "product_revenue": {
            "category": QueryCategory.JOINS,
            "sql": """
                SELECT
                    product_id,
                    SUM(amount) AS total_revenue,
                    COUNT(*) AS sales_count,
                    AVG(amount) AS avg_order_value
                FROM {table}
                WHERE status IN ('completed', 'paid')
                  AND transaction_date >= %s
                GROUP BY product_id
                ORDER BY total_revenue DESC
                LIMIT 50
            """,
            "params_func": lambda: (datetime(2024, 1, 1),),
        },
        "revenue_growth_mom": {
            "category": QueryCategory.WINDOW_FUNCTIONS,
            "sql": """
                WITH monthly AS (
                    SELECT
                        DATE_TRUNC('month', transaction_date) AS month,
                        SUM(amount) AS revenue
                    FROM {table}
                    WHERE status IN ('completed', 'paid')
                    GROUP BY DATE_TRUNC('month', transaction_date)
                )
                SELECT
                    month,
                    revenue,
                    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
                    CASE
                        WHEN LAG(revenue) OVER (ORDER BY month) > 0
                        THEN (revenue - LAG(revenue) OVER (ORDER BY month)) /
                             LAG(revenue) OVER (ORDER BY month) * 100
                        ELSE NULL
                    END AS growth_pct
                FROM monthly
                ORDER BY month
            """,
            "params_func": lambda: (),
        },
        "payment_method_analysis": {
            "category": QueryCategory.AGGREGATION,
            "sql": """
                SELECT
                    payment_method,
                    status,
                    COUNT(*) AS transaction_count,
                    SUM(amount) AS total_amount,
                    AVG(amount) AS avg_amount
                FROM {table}
                WHERE transaction_date >= %s
                GROUP BY payment_method, status
                ORDER BY total_amount DESC
            """,
            "params_func": lambda: (datetime(2024, 1, 1),),
        },
        "last_30_days": {
            "category": QueryCategory.TIME_SERIES,
            "sql": """
                SELECT
                    transaction_date,
                    SUM(amount) AS daily_revenue,
                    COUNT(*) AS transaction_count
                FROM {table}
                WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
                  AND status IN ('completed', 'paid')
                GROUP BY transaction_date
                ORDER BY transaction_date
            """,
            "params_func": lambda: (),
        },
    }

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize benchmark runner.

        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url or os.getenv(
            "DATABASE_URL",
            "postgresql://echo_user:echo_password@localhost:5432/echo_db",
        )

        # Handle async driver in URL
        if self.database_url.startswith("postgresql+asyncpg://"):
            self.database_url = self.database_url.replace("postgresql+asyncpg://", "postgresql://")

    def _get_connection(self):
        """Get database connection."""
        return psycopg2.connect(self.database_url)

    def _clear_cache(self, conn):
        """Clear PostgreSQL caches as much as possible."""
        with conn.cursor() as cur:
            # Discard cached plans
            cur.execute("DISCARD ALL")
            conn.commit()

        # Force Python garbage collection
        gc.collect()

    def _get_row_count(self, conn, table: str) -> int:
        """Get approximate row count for table."""
        with conn.cursor() as cur:
            # Use pg_stat for fast approximate count
            cur.execute(
                """
                SELECT COALESCE(
                    (SELECT reltuples::bigint FROM pg_class WHERE relname = %s),
                    0
                )
                """,
                (table,),
            )
            count = cur.fetchone()[0]

            # If stats not available, do actual count (slower)
            if count == 0:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                count = cur.fetchone()[0]

            return count

    def _run_query(
        self,
        conn,
        query_sql: str,
        params: tuple,
        table: str,
    ) -> tuple[float, int]:
        """
        Run a query and return execution time and row count.

        Returns:
            Tuple of (time_ms, row_count)
        """
        formatted_sql = query_sql.format(table=table)

        with conn.cursor() as cur:
            start = time.perf_counter()
            cur.execute(formatted_sql, params)
            rows = cur.fetchall()
            elapsed = (time.perf_counter() - start) * 1000

        return elapsed, len(rows)

    def _analyze_query(
        self,
        conn,
        query_sql: str,
        params: tuple,
        table: str,
    ) -> dict[str, Any]:
        """
        Run EXPLAIN ANALYZE and parse results.

        Returns:
            Dict with plan analysis results
        """
        formatted_sql = query_sql.format(table=table)
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {formatted_sql}"

        with conn.cursor() as cur:
            cur.execute(explain_sql, params)
            result = cur.fetchone()[0]

        plan = result[0] if result else {}
        plan_node = plan.get("Plan", {})

        # Extract key metrics
        analysis = {
            "execution_time": plan.get("Execution Time", 0),
            "planning_time": plan.get("Planning Time", 0),
            "rows_scanned": self._extract_rows_scanned(plan_node),
            "index_used": self._check_index_usage(plan_node),
            "seq_scan": self._check_seq_scan(plan_node),
            "partitions_scanned": self._count_partitions(plan_node),
            "plan_text": str(plan),
        }

        return analysis

    def _extract_rows_scanned(self, plan_node: dict) -> int:
        """Extract total rows scanned from plan."""
        rows = plan_node.get("Actual Rows", 0)

        # Recursively check child nodes
        for child in plan_node.get("Plans", []):
            rows += self._extract_rows_scanned(child)

        return rows

    def _check_index_usage(self, plan_node: dict) -> bool:
        """Check if any index scan was used."""
        node_type = plan_node.get("Node Type", "")
        if "Index" in node_type:
            return True

        for child in plan_node.get("Plans", []):
            if self._check_index_usage(child):
                return True

        return False

    def _check_seq_scan(self, plan_node: dict) -> bool:
        """Check if sequential scan was used."""
        node_type = plan_node.get("Node Type", "")
        if node_type == "Seq Scan":
            return True

        for child in plan_node.get("Plans", []):
            if self._check_seq_scan(child):
                return True

        return False

    def _count_partitions(self, plan_node: dict) -> int:
        """Count number of partitions scanned."""
        count = 0

        # Look for partition-related info
        if "Subplans Removed" in str(plan_node):
            # Partition pruning happened
            pass

        node_type = plan_node.get("Node Type", "")
        if "Append" in node_type:
            # Count child nodes (partitions)
            count = len(plan_node.get("Plans", []))

        for child in plan_node.get("Plans", []):
            child_count = self._count_partitions(child)
            if child_count > count:
                count = child_count

        return count

    def run_benchmark(
        self,
        query_name: str,
        table: str,
        warmup_runs: int = 1,
        benchmark_runs: int = 3,
    ) -> BenchmarkResult:
        """
        Run a single benchmark query.

        Args:
            query_name: Name of query from QUERIES dict
            table: Table to query
            warmup_runs: Number of warmup runs (not measured)
            benchmark_runs: Number of measured runs

        Returns:
            BenchmarkResult with timing and analysis
        """
        if query_name not in self.QUERIES:
            raise ValueError(f"Unknown query: {query_name}")

        query_def = self.QUERIES[query_name]
        query_sql = query_def["sql"]
        params = query_def["params_func"]()
        category = query_def["category"]

        conn = self._get_connection()

        try:
            # Cold run (after cache clear)
            self._clear_cache(conn)
            cold_time, row_count = self._run_query(conn, query_sql, params, table)

            # Warmup runs
            for _ in range(warmup_runs):
                self._run_query(conn, query_sql, params, table)

            # Warm runs
            warm_times = []
            for _ in range(benchmark_runs):
                elapsed, _ = self._run_query(conn, query_sql, params, table)
                warm_times.append(elapsed)

            warm_time = statistics.median(warm_times)

            # Analyze query plan
            analysis = self._analyze_query(conn, query_sql, params, table)

            return BenchmarkResult(
                query_name=query_name,
                category=category,
                cold_time_ms=cold_time,
                warm_time_ms=warm_time,
                rows_returned=row_count,
                rows_scanned=analysis["rows_scanned"],
                partitions_scanned=analysis["partitions_scanned"],
                index_used=analysis["index_used"],
                seq_scan=analysis["seq_scan"],
                execution_plan=analysis["plan_text"][:500],  # Truncate
            )

        except Exception as e:
            return BenchmarkResult(
                query_name=query_name,
                category=category,
                cold_time_ms=0,
                warm_time_ms=0,
                rows_returned=0,
                error=str(e),
            )

        finally:
            conn.close()

    def run_all(
        self,
        table: str,
        scale: str = "unknown",
    ) -> BenchmarkSuite:
        """
        Run all benchmark queries.

        Args:
            table: Table to benchmark
            scale: Scale label (e.g., "1M", "10M")

        Returns:
            BenchmarkSuite with all results
        """
        conn = self._get_connection()
        row_count = self._get_row_count(conn, table)
        conn.close()

        suite = BenchmarkSuite(
            scale=scale,
            table=table,
            row_count=row_count,
        )

        for query_name in self.QUERIES:
            print(f"  Running {query_name}...")
            result = self.run_benchmark(query_name, table)
            suite.results.append(result)

            if result.error:
                print(f"    ERROR: {result.error}")
            else:
                print(
                    f"    Cold: {result.cold_time_ms:.1f}ms, "
                    f"Warm: {result.warm_time_ms:.1f}ms, "
                    f"Rows: {result.rows_returned:,}"
                )

        return suite

    def compare_tables(
        self,
        table1: str,
        table2: str,
        label1: str = "Table 1",
        label2: str = "Table 2",
    ) -> str:
        """
        Compare benchmark results between two tables.

        Useful for comparing partitioned vs non-partitioned tables.

        Returns:
            Markdown comparison table
        """
        print(f"\nBenchmarking {label1} ({table1})...")
        suite1 = self.run_all(table1, label1)

        print(f"\nBenchmarking {label2} ({table2})...")
        suite2 = self.run_all(table2, label2)

        # Generate comparison table
        lines = [
            "## Query Performance Comparison",
            "",
            f"| Query | {label1} (ms) | {label2} (ms) | Improvement |",
            "|-------|--------------|--------------|-------------|",
        ]

        for r1, r2 in zip(suite1.results, suite2.results):
            if r1.error or r2.error:
                lines.append(f"| {r1.query_name} | ERROR | ERROR | - |")
                continue

            improvement = (r1.warm_time_ms - r2.warm_time_ms) / r1.warm_time_ms * 100
            arrow = "faster" if improvement > 0 else "slower"

            lines.append(
                f"| {r1.query_name} | {r1.warm_time_ms:.1f} | "
                f"{r2.warm_time_ms:.1f} | {abs(improvement):.1f}% {arrow} |"
            )

        return "\n".join(lines)


def main():
    """Example usage."""
    benchmark = QueryBenchmark()

    print("Running benchmarks on transactions table...")
    suite = benchmark.run_all("transactions", scale="Test")

    print("\n" + "=" * 60)
    print(suite.to_markdown_table())


if __name__ == "__main__":
    main()
