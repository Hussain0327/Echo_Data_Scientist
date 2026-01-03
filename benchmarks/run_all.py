#!/usr/bin/env python3
"""
Benchmark Runner Script.

Runs query benchmarks across multiple data scales and generates
a markdown report for inclusion in README.

Usage:
    # Run benchmarks on existing data
    python -m benchmarks.run_all --table transactions

    # Run with comparison (partitioned vs non-partitioned)
    python -m benchmarks.run_all --compare transactions transactions_partitioned

    # Generate synthetic data and benchmark
    python -m benchmarks.run_all --generate --scales 1M,10M

    # Output to file
    python -m benchmarks.run_all --output benchmarks/results.md
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from benchmarks.query_benchmarks import BenchmarkSuite, QueryBenchmark


def run_single_benchmark(table: str, scale: str) -> BenchmarkSuite:
    """Run benchmarks on a single table."""
    benchmark = QueryBenchmark()
    return benchmark.run_all(table, scale)


def run_comparison(
    table1: str,
    table2: str,
    label1: str = "Standard",
    label2: str = "Partitioned",
) -> str:
    """Run comparison benchmarks between two tables."""
    benchmark = QueryBenchmark()
    return benchmark.compare_tables(table1, table2, label1, label2)


def generate_full_report(suites: list[BenchmarkSuite]) -> str:
    """Generate full markdown report from benchmark suites."""
    lines = [
        "# Query Performance Benchmarks",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Environment",
        "",
        "- PostgreSQL 15",
        "- Database: echo_db",
        "- Connection pool: 10 connections",
        "",
        "## Summary Table",
        "",
        "| Query | " + " | ".join(s.scale for s in suites) + " |",
        "|-------| " + " | ".join("---:" for _ in suites) + " |",
    ]

    # Get all query names from first suite
    if suites:
        for i, result in enumerate(suites[0].results):
            query_name = result.query_name
            row = f"| {query_name} |"

            for suite in suites:
                if i < len(suite.results):
                    r = suite.results[i]
                    if r.error:
                        row += " ERROR |"
                    else:
                        row += f" {r.warm_time_ms:.0f}ms |"
                else:
                    row += " - |"

            lines.append(row)

    lines.extend(
        [
            "",
            "## Detailed Results",
            "",
        ]
    )

    for suite in suites:
        lines.append(suite.to_markdown_table())
        lines.append("")

    # Add insights section
    lines.extend(
        [
            "## Key Insights",
            "",
        ]
    )

    if len(suites) >= 2:
        # Compare smallest vs largest scale
        small = suites[0]
        large = suites[-1]

        for i, (r_small, r_large) in enumerate(zip(small.results, large.results)):
            if r_small.error or r_large.error:
                continue

            scale_factor = large.row_count / max(small.row_count, 1)
            time_factor = r_large.warm_time_ms / max(r_small.warm_time_ms, 0.1)

            if time_factor < scale_factor * 0.5:
                lines.append(
                    f"- **{r_small.query_name}**: Scales sub-linearly "
                    f"({scale_factor:.0f}x data, only {time_factor:.1f}x slower)"
                )
            elif time_factor > scale_factor * 1.5:
                lines.append(
                    f"- **{r_small.query_name}**: Needs optimization "
                    f"({scale_factor:.0f}x data, {time_factor:.1f}x slower)"
                )

    return "\n".join(lines)


def save_results(suites: list[BenchmarkSuite], output_path: Path):
    """Save benchmark results to JSON for later analysis."""
    data = {
        "generated_at": datetime.now().isoformat(),
        "suites": [],
    }

    for suite in suites:
        suite_data = {
            "scale": suite.scale,
            "table": suite.table,
            "row_count": suite.row_count,
            "timestamp": suite.timestamp.isoformat(),
            "results": [],
        }

        for r in suite.results:
            suite_data["results"].append(
                {
                    "query_name": r.query_name,
                    "category": r.category.value,
                    "cold_time_ms": r.cold_time_ms,
                    "warm_time_ms": r.warm_time_ms,
                    "rows_returned": r.rows_returned,
                    "rows_scanned": r.rows_scanned,
                    "partitions_scanned": r.partitions_scanned,
                    "index_used": r.index_used,
                    "seq_scan": r.seq_scan,
                    "error": r.error,
                }
            )

        data["suites"].append(suite_data)

    json_path = output_path.with_suffix(".json")
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Results saved to {json_path}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run query benchmarks across data scales",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--table",
        type=str,
        default="transactions",
        help="Table to benchmark (default: transactions)",
    )
    parser.add_argument(
        "--scales",
        type=str,
        default="current",
        help="Comma-separated scales to benchmark (e.g., '1M,10M,50M') or 'current'",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("TABLE1", "TABLE2"),
        help="Compare two tables (e.g., --compare transactions transactions_partitioned)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for markdown report",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also save results as JSON",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Echo Analytics Query Benchmarks")
    print("=" * 60)
    print()

    if args.compare:
        # Comparison mode
        table1, table2 = args.compare
        print(f"Comparing {table1} vs {table2}...")
        report = run_comparison(table1, table2)
        print()
        print(report)

        if args.output:
            Path(args.output).write_text(report)
            print(f"\nReport saved to {args.output}")

        return 0

    # Single or multi-scale benchmark
    suites = []

    if args.scales == "current":
        # Just run on current table
        print(f"Benchmarking {args.table}...")
        suite = run_single_benchmark(args.table, "current")
        suites.append(suite)
    else:
        # Multiple scales
        scales = [s.strip() for s in args.scales.split(",")]
        for scale in scales:
            table_name = f"{args.table}_{scale.lower()}" if scale != "current" else args.table
            print(f"\nBenchmarking {table_name} ({scale})...")

            try:
                suite = run_single_benchmark(table_name, scale)
                suites.append(suite)
            except Exception as e:
                print(f"  ERROR: {e}")
                continue

    # Generate report
    report = generate_full_report(suites)

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print()
    print(report)

    # Save outputs
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report)
        print(f"\nMarkdown report saved to {args.output}")

        if args.json:
            save_results(suites, output_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
