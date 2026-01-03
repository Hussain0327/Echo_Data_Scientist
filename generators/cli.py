"""
CLI for synthetic data generation.

Usage:
    python -m generators.cli generate --scale 1M --output-dir ./data/generated/1M
    python -m generators.cli generate --scale 10M --output-dir ./data/generated/10M
    python -m generators.cli generate --scale 50M --output-dir ./data/generated/50M --format parquet
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

from generators.synthetic_data import ScaleConfig, SyntheticDataGenerator


def format_size(size_bytes: int) -> str:
    """Format bytes as human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def save_dataframe(
    df: pd.DataFrame,
    output_dir: Path,
    name: str,
    file_format: str,
) -> int:
    """Save DataFrame to file and return size in bytes."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        path = output_dir / f"{name}.parquet"
        df.to_parquet(path, index=False, compression="snappy")
    elif file_format == "csv":
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unknown format: {file_format}")

    return path.stat().st_size


def cmd_generate(args: argparse.Namespace) -> int:
    """Generate synthetic data at specified scale."""
    print("\nSynthetic Data Generator")
    print("========================")
    print(f"Scale: {args.scale}")
    print(f"Output: {args.output_dir}")
    print(f"Format: {args.format}")
    print(f"Seed: {args.seed}")
    print(f"Include history: {not args.no_history}")
    print()

    start_time = time.time()

    # Initialize generator
    generator = SyntheticDataGenerator(seed=args.seed)

    # Generate data
    data = generator.generate_all(
        scale=args.scale,
        include_history=not args.no_history,
    )

    # Save to files
    output_dir = Path(args.output_dir)
    print(f"\nSaving to {output_dir}...")

    total_size = 0
    file_sizes = {}

    for name, df in data.items():
        size = save_dataframe(df, output_dir, name, args.format)
        file_sizes[name] = size
        total_size += size
        print(f"  {name}.{args.format}: {len(df):,} rows, {format_size(size)}")

    elapsed = time.time() - start_time

    # Print summary
    print(f"\n{'='*60}")
    print("Generation Summary")
    print(f"{'='*60}")
    print(f"Total rows: {sum(len(df) for df in data.values()):,}")
    print(f"Total size: {format_size(total_size)}")
    print(f"Time elapsed: {elapsed:.1f}s")
    print(f"Output directory: {output_dir.absolute()}")
    print()

    # Write manifest
    manifest_path = output_dir / "manifest.json"
    manifest = {
        "scale": args.scale,
        "seed": args.seed,
        "format": args.format,
        "include_history": not args.no_history,
        "files": {
            name: {"rows": len(df), "size_bytes": file_sizes[name]} for name, df in data.items()
        },
        "total_rows": sum(len(df) for df in data.values()),
        "total_size_bytes": total_size,
        "generation_time_seconds": elapsed,
    }

    import json

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Manifest written to: {manifest_path}")

    return 0


def cmd_info(args: argparse.Namespace) -> int:
    """Display information about scale configurations."""
    print("\nScale Configurations")
    print("====================\n")

    for scale in ["1M", "10M", "50M"]:
        config = ScaleConfig.from_scale(scale)
        print(f"{scale}:")
        print(f"  Customers:        {config.customers:>12,}")
        print(f"  Products:         {config.products:>12,}")
        print(f"  Transactions:     {config.transactions:>12,}")
        print(f"  Marketing Events: {config.marketing_events:>12,}")
        print(f"  Experiments:      {config.experiments:>12,}")
        print()

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate generated data for referential integrity."""
    data_dir = Path(args.data_dir)

    if not data_dir.exists():
        print(f"Error: Directory not found: {data_dir}")
        return 1

    print(f"\nValidating data in: {data_dir}")
    print("=" * 60)

    # Load manifest
    manifest_path = data_dir / "manifest.json"
    if manifest_path.exists():
        import json

        with open(manifest_path) as f:
            manifest = json.load(f)
        print(f"Scale: {manifest.get('scale', 'unknown')}")
        print(f"Format: {manifest.get('format', 'unknown')}")
        print()

    # Determine file format
    file_format = "parquet" if (data_dir / "customers.parquet").exists() else "csv"

    # Load data
    def load_file(name: str) -> pd.DataFrame:
        if file_format == "parquet":
            return pd.read_parquet(data_dir / f"{name}.parquet")
        return pd.read_csv(data_dir / f"{name}.csv")

    customers = load_file("customers")
    products = load_file("products")
    transactions = load_file("transactions")

    # Validation checks
    errors = []
    warnings = []

    # Check FK: transactions -> customers
    invalid_customers = set(transactions["customer_id"]) - set(customers["customer_id"])
    if invalid_customers:
        errors.append(f"Transactions reference {len(invalid_customers)} invalid customer_ids")

    # Check FK: transactions -> products
    invalid_products = set(transactions["product_id"]) - set(products["product_id"])
    if invalid_products:
        errors.append(f"Transactions reference {len(invalid_products)} invalid product_ids")

    # Check for nulls in required columns
    required_cols = {
        "customers": ["customer_id", "email", "segment"],
        "products": ["product_id", "name", "price"],
        "transactions": ["transaction_id", "amount", "customer_id"],
    }

    for name, cols in required_cols.items():
        df = load_file(name)
        for col in cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    errors.append(f"{name}.{col} has {null_count} null values")

    # Check uniqueness
    unique_cols = {
        "customers": "customer_id",
        "products": "product_id",
        "transactions": "transaction_id",
    }

    for name, col in unique_cols.items():
        df = load_file(name)
        if col in df.columns:
            dup_count = df[col].duplicated().sum()
            if dup_count > 0:
                errors.append(f"{name}.{col} has {dup_count} duplicate values")

    # Check data quality
    if (transactions["amount"] < 0).any():
        neg_count = (transactions["amount"] < 0).sum()
        errors.append(f"Transactions has {neg_count} negative amounts")

    # Check date ranges
    if "transaction_date" in transactions.columns:
        transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"])
        date_range = (
            transactions["transaction_date"].max() - transactions["transaction_date"].min()
        ).days
        if date_range < 30:
            warnings.append(f"Transaction date range is only {date_range} days")

    # Report results
    print("Validation Results")
    print("-" * 40)

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for error in errors:
            print(f"  [ERROR] {error}")
    else:
        print("\nNo errors found!")

    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for warning in warnings:
            print(f"  [WARN] {warning}")

    print()
    return 1 if errors else 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Echo Analytics Synthetic Data Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Generate 1M scale dataset:
    python -m generators.cli generate --scale 1M --output-dir ./data/generated/1M

  Generate 10M scale as parquet:
    python -m generators.cli generate --scale 10M --output-dir ./data/generated/10M --format parquet

  Show scale configurations:
    python -m generators.cli info

  Validate generated data:
    python -m generators.cli validate --data-dir ./data/generated/1M
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Generate command
    gen_parser = subparsers.add_parser("generate", help="Generate synthetic data")
    gen_parser.add_argument(
        "--scale",
        choices=["1M", "10M", "50M"],
        default="1M",
        help="Data scale (default: 1M)",
    )
    gen_parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        help="Output directory for generated files",
    )
    gen_parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="parquet",
        help="Output file format (default: parquet)",
    )
    gen_parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    gen_parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip generating SCD history records",
    )
    gen_parser.set_defaults(func=cmd_generate)

    # Info command
    info_parser = subparsers.add_parser("info", help="Show scale configurations")
    info_parser.set_defaults(func=cmd_info)

    # Validate command
    val_parser = subparsers.add_parser("validate", help="Validate generated data")
    val_parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Directory containing generated data",
    )
    val_parser.set_defaults(func=cmd_validate)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
