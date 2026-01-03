"""
Synthetic Data Generator for Echo Analytics Platform.

Generates realistic business data at scale (1M, 10M, 50M rows) with:
- Proper foreign key relationships between entities
- Realistic distributions (Pareto for revenue, normal for daily patterns)
- SCD-triggering changes for dimensional history testing
- Reproducible results via seed
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ScaleConfig:
    """Configuration for different data scales."""

    name: str
    customers: int
    products: int
    transactions: int
    marketing_events: int
    experiments: int

    @classmethod
    def from_scale(cls, scale: str) -> "ScaleConfig":
        """Create config from scale string (1M, 10M, 50M)."""
        configs = {
            "1M": cls(
                name="1M",
                customers=50_000,
                products=1_000,
                transactions=1_000_000,
                marketing_events=100_000,
                experiments=10,
            ),
            "10M": cls(
                name="10M",
                customers=500_000,
                products=5_000,
                transactions=10_000_000,
                marketing_events=1_000_000,
                experiments=50,
            ),
            "50M": cls(
                name="50M",
                customers=2_000_000,
                products=10_000,
                transactions=50_000_000,
                marketing_events=5_000_000,
                experiments=100,
            ),
        }
        if scale not in configs:
            raise ValueError(f"Unknown scale: {scale}. Use one of: {list(configs.keys())}")
        return configs[scale]


class SyntheticDataGenerator:
    """
    Generates synthetic business data with realistic patterns and relationships.

    Features:
    - Reproducible via seed
    - Pareto distribution for customer spending (80/20 rule)
    - Seasonal patterns in transactions
    - Customer segment changes over time (for SCD2 testing)
    - Product price changes (for SCD2 testing)
    """

    # Customer segments with upgrade/downgrade paths
    SEGMENTS = ["starter", "growth", "professional", "enterprise"]
    SEGMENT_WEIGHTS = [0.40, 0.35, 0.20, 0.05]

    # Plan types per segment
    PLAN_TYPES = {
        "starter": ["free", "basic"],
        "growth": ["basic", "standard"],
        "professional": ["standard", "premium"],
        "enterprise": ["premium", "enterprise"],
    }

    # Acquisition channels with realistic distribution
    CHANNELS = ["organic", "paid_search", "social", "email", "referral", "direct"]
    CHANNEL_WEIGHTS = [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]

    # Product categories
    CATEGORIES = ["analytics", "integration", "automation", "reporting", "api_access"]

    # Transaction statuses
    STATUSES = ["completed", "paid", "pending", "refunded", "cancelled", "failed"]
    STATUS_WEIGHTS = [0.45, 0.35, 0.10, 0.05, 0.03, 0.02]

    # Experiment variants
    VARIANTS = ["control", "variant_a", "variant_b", "variant_c"]

    def __init__(self, seed: int = 42):
        """Initialize generator with seed for reproducibility."""
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self._customer_ids: Optional[np.ndarray] = None
        self._product_ids: Optional[np.ndarray] = None

    def _generate_id(self, prefix: str, index: int) -> str:
        """Generate deterministic ID based on prefix and index."""
        hash_input = f"{prefix}_{self.seed}_{index}"
        return f"{prefix}_{hashlib.md5(hash_input.encode()).hexdigest()[:12]}"

    def _generate_email(self, index: int) -> str:
        """Generate fake email address."""
        domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com", "business.org"]
        domain = domains[index % len(domains)]
        return f"user_{index}@{domain}"

    def _generate_name(self, index: int) -> str:
        """Generate fake name."""
        first_names = [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Elizabeth",
            "David",
            "Barbara",
            "Richard",
            "Susan",
            "Joseph",
            "Jessica",
            "Thomas",
            "Sarah",
            "Charles",
            "Karen",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
            "Taylor",
            "Moore",
            "Jackson",
            "Martin",
        ]
        first = first_names[index % len(first_names)]
        last = last_names[(index * 7) % len(last_names)]
        return f"{first} {last}"

    def _generate_product_name(self, category: str, index: int) -> str:
        """Generate product name based on category."""
        prefixes = {
            "analytics": ["Insight", "Metric", "Data", "Trend", "Pattern"],
            "integration": ["Connect", "Sync", "Bridge", "Link", "Flow"],
            "automation": ["Auto", "Smart", "Quick", "Rapid", "Swift"],
            "reporting": ["Report", "Dashboard", "Chart", "Summary", "View"],
            "api_access": ["API", "Endpoint", "Gateway", "Access", "Stream"],
        }
        suffixes = ["Pro", "Plus", "Basic", "Enterprise", "Standard", "Premium"]
        prefix = prefixes[category][index % len(prefixes[category])]
        suffix = suffixes[(index * 3) % len(suffixes)]
        return f"{prefix} {suffix}"

    def generate_customers(
        self,
        n: int,
        start_date: datetime = datetime(2022, 1, 1),
        end_date: datetime = datetime(2025, 12, 31),
    ) -> pd.DataFrame:
        """
        Generate customer data with realistic segment distribution.

        Args:
            n: Number of customers to generate
            start_date: Earliest customer signup date
            end_date: Latest customer signup date

        Returns:
            DataFrame with customer_id, email, name, segment, plan_type,
            acquisition_channel, created_at, updated_at
        """
        print(f"Generating {n:,} customers...")

        # Generate base data
        customer_ids = [self._generate_id("cust", i) for i in range(n)]
        self._customer_ids = np.array(customer_ids)

        # Segment assignment using weighted random
        segments = self.rng.choice(self.SEGMENTS, size=n, p=self.SEGMENT_WEIGHTS)

        # Plan type based on segment
        plan_types = [self.rng.choice(self.PLAN_TYPES[seg]) for seg in segments]

        # Acquisition channel
        channels = self.rng.choice(self.CHANNELS, size=n, p=self.CHANNEL_WEIGHTS)

        # Signup dates with slight recency bias
        date_range = (end_date - start_date).days
        # Use beta distribution for recency bias (more recent signups)
        days_offset = self.rng.beta(2, 5, size=n) * date_range
        created_dates = [start_date + timedelta(days=int(d)) for d in days_offset]

        # Updated dates (some customers have been updated)
        updated_dates = []
        for i, created in enumerate(created_dates):
            if self.rng.random() < 0.3:  # 30% have been updated
                days_since = (end_date - created).days
                if days_since > 0:
                    update_offset = self.rng.integers(1, max(2, days_since))
                    updated_dates.append(created + timedelta(days=int(update_offset)))
                else:
                    updated_dates.append(created)
            else:
                updated_dates.append(created)

        df = pd.DataFrame(
            {
                "customer_id": customer_ids,
                "email": [self._generate_email(i) for i in range(n)],
                "name": [self._generate_name(i) for i in range(n)],
                "segment": segments,
                "plan_type": plan_types,
                "acquisition_channel": channels,
                "created_at": created_dates,
                "updated_at": updated_dates,
            }
        )

        print(f"  Generated {len(df):,} customers")
        return df

    def generate_customer_history(
        self,
        customers: pd.DataFrame,
        change_rate: float = 0.15,
    ) -> pd.DataFrame:
        """
        Generate historical customer changes for SCD Type 2 testing.

        Creates previous versions of customer records where segment
        or plan_type has changed over time.

        Args:
            customers: Current customer DataFrame
            change_rate: Fraction of customers with historical changes

        Returns:
            DataFrame with historical customer records
        """
        print(f"Generating customer history (change_rate={change_rate})...")

        n_changes = int(len(customers) * change_rate)
        change_indices = self.rng.choice(len(customers), size=n_changes, replace=False)

        history_records = []
        for idx in change_indices:
            current = customers.iloc[idx]
            created = current["created_at"]
            updated = current["updated_at"]

            if updated > created:
                # Create a previous version
                prev_segment_idx = max(0, self.SEGMENTS.index(current["segment"]) - 1)
                prev_segment = self.SEGMENTS[prev_segment_idx]

                history_records.append(
                    {
                        "customer_id": current["customer_id"],
                        "email": current["email"],
                        "name": current["name"],
                        "segment": prev_segment,
                        "plan_type": self.rng.choice(self.PLAN_TYPES[prev_segment]),
                        "acquisition_channel": current["acquisition_channel"],
                        "created_at": created,
                        "updated_at": created,  # Original state
                        "valid_from": created,
                        "valid_to": updated,
                        "is_current": False,
                    }
                )

        history_df = pd.DataFrame(history_records)
        print(f"  Generated {len(history_df):,} historical customer records")
        return history_df

    def generate_products(
        self,
        n: int,
        start_date: datetime = datetime(2022, 1, 1),
    ) -> pd.DataFrame:
        """
        Generate product catalog with pricing.

        Args:
            n: Number of products to generate
            start_date: Product creation start date

        Returns:
            DataFrame with product_id, name, category, price, created_at
        """
        print(f"Generating {n:,} products...")

        product_ids = [self._generate_id("prod", i) for i in range(n)]
        self._product_ids = np.array(product_ids)

        # Category distribution
        categories = self.rng.choice(self.CATEGORIES, size=n)

        # Pricing: log-normal distribution for realistic spread
        # Mean ~$50, with range from $10 to $500
        prices = self.rng.lognormal(mean=3.9, sigma=0.8, size=n)
        prices = np.clip(prices, 10, 500).round(2)

        # Creation dates
        created_dates = [start_date + timedelta(days=int(i * 30 / n)) for i in range(n)]

        df = pd.DataFrame(
            {
                "product_id": product_ids,
                "name": [self._generate_product_name(cat, i) for i, cat in enumerate(categories)],
                "category": categories,
                "price": prices,
                "created_at": created_dates,
            }
        )

        print(f"  Generated {len(df):,} products")
        return df

    def generate_product_history(
        self,
        products: pd.DataFrame,
        change_rate: float = 0.20,
    ) -> pd.DataFrame:
        """
        Generate historical product price changes for SCD Type 2 testing.

        Args:
            products: Current product DataFrame
            change_rate: Fraction of products with price changes

        Returns:
            DataFrame with historical product records
        """
        print(f"Generating product history (change_rate={change_rate})...")

        n_changes = int(len(products) * change_rate)
        change_indices = self.rng.choice(len(products), size=n_changes, replace=False)

        history_records = []
        for idx in change_indices:
            current = products.iloc[idx]
            created = current["created_at"]

            # Previous price (10-30% lower)
            price_change = self.rng.uniform(0.10, 0.30)
            prev_price = round(current["price"] * (1 - price_change), 2)

            # Change happened sometime after creation
            change_date = created + timedelta(days=int(self.rng.integers(30, 365)))

            history_records.append(
                {
                    "product_id": current["product_id"],
                    "name": current["name"],
                    "category": current["category"],
                    "price": prev_price,
                    "created_at": created,
                    "valid_from": created,
                    "valid_to": change_date,
                    "is_current": False,
                }
            )

        history_df = pd.DataFrame(history_records)
        print(f"  Generated {len(history_df):,} historical product records")
        return history_df

    def generate_transactions(
        self,
        n: int,
        customers: pd.DataFrame,
        products: pd.DataFrame,
        start_date: datetime = datetime(2023, 1, 1),
        end_date: datetime = datetime(2025, 12, 31),
    ) -> pd.DataFrame:
        """
        Generate transaction data with realistic patterns.

        Features:
        - Pareto distribution (20% of customers generate 80% of transactions)
        - Weekly seasonality (higher on weekdays)
        - Monthly seasonality (end of month spike)
        - Status distribution reflecting real-world patterns

        Args:
            n: Number of transactions to generate
            customers: Customer DataFrame for FK relationships
            products: Product DataFrame for FK relationships
            start_date: Transaction period start
            end_date: Transaction period end

        Returns:
            DataFrame with transaction_id, transaction_date, amount,
            customer_id, product_id, status, payment_method
        """
        print(f"Generating {n:,} transactions...")

        # Transaction IDs
        transaction_ids = [self._generate_id("txn", i) for i in range(n)]

        # Customer selection with Pareto distribution (80/20 rule)
        # Use power law distribution for customer indices
        customer_ids = customers["customer_id"].values
        pareto_indices = self.rng.pareto(a=1.5, size=n) + 1
        pareto_indices = (pareto_indices / pareto_indices.max() * (len(customer_ids) - 1)).astype(
            int
        )
        pareto_indices = np.clip(pareto_indices, 0, len(customer_ids) - 1)
        selected_customers = customer_ids[pareto_indices]

        # Product selection (uniform for simplicity)
        product_ids = products["product_id"].values
        selected_products = self.rng.choice(product_ids, size=n)

        # Transaction dates with weekly/monthly patterns
        date_range = (end_date - start_date).days
        base_days = self.rng.uniform(0, date_range, size=n)

        # Apply weekly seasonality (reduce weekends by 40%)
        transaction_dates = []
        for day_offset in base_days:
            date = start_date + timedelta(days=int(day_offset))
            # Weekend reduction
            if date.weekday() >= 5:  # Saturday or Sunday
                if self.rng.random() < 0.4:  # 40% chance to move to weekday
                    date += timedelta(days=int(self.rng.integers(1, 3)))
            transaction_dates.append(date)

        # Amount based on product price with variation
        product_prices = products.set_index("product_id")["price"]
        amounts = []
        for prod_id in selected_products:
            base_price = product_prices[prod_id]
            # Quantity variation (1-3 items, weighted toward 1)
            quantity = self.rng.choice([1, 1, 1, 2, 2, 3])
            # Small random variation (+/- 5%)
            variation = self.rng.uniform(0.95, 1.05)
            amounts.append(round(base_price * quantity * variation, 2))

        # Status distribution
        statuses = self.rng.choice(self.STATUSES, size=n, p=self.STATUS_WEIGHTS)

        # Payment methods
        payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
        payment_weights = [0.45, 0.25, 0.15, 0.10, 0.05]
        payments = self.rng.choice(payment_methods, size=n, p=payment_weights)

        df = pd.DataFrame(
            {
                "transaction_id": transaction_ids,
                "transaction_date": transaction_dates,
                "amount": amounts,
                "customer_id": selected_customers,
                "product_id": selected_products,
                "status": statuses,
                "payment_method": payments,
            }
        )

        # Sort by date for realistic data ordering
        df = df.sort_values("transaction_date").reset_index(drop=True)

        print(f"  Generated {len(df):,} transactions")
        print(f"  Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
        print(f"  Total amount: ${df['amount'].sum():,.2f}")
        return df

    def generate_marketing_events(
        self,
        n: int,
        start_date: datetime = datetime(2023, 1, 1),
        end_date: datetime = datetime(2025, 12, 31),
    ) -> pd.DataFrame:
        """
        Generate marketing event data for funnel analysis.

        Args:
            n: Number of events to generate
            start_date: Event period start
            end_date: Event period end

        Returns:
            DataFrame with event_id, event_date, channel, campaign,
            leads, conversions, spend
        """
        print(f"Generating {n:,} marketing events...")

        event_ids = [self._generate_id("mkt", i) for i in range(n)]

        # Channel distribution
        channels = self.rng.choice(self.CHANNELS, size=n, p=self.CHANNEL_WEIGHTS)

        # Campaign names per channel
        campaigns = []
        for i, channel in enumerate(channels):
            campaign_num = (i % 10) + 1
            campaigns.append(f"{channel}_campaign_{campaign_num}")

        # Event dates
        date_range = (end_date - start_date).days
        days_offset = self.rng.uniform(0, date_range, size=n)
        event_dates = [start_date + timedelta(days=int(d)) for d in days_offset]

        # Leads: log-normal distribution
        leads = self.rng.lognormal(mean=4, sigma=1, size=n).astype(int)
        leads = np.clip(leads, 1, 10000)

        # Conversion rate varies by channel
        channel_conversion_rates = {
            "organic": 0.08,
            "paid_search": 0.05,
            "social": 0.03,
            "email": 0.12,
            "referral": 0.15,
            "direct": 0.10,
        }
        conversions = []
        for i, (channel, lead_count) in enumerate(zip(channels, leads)):
            base_rate = channel_conversion_rates[channel]
            # Add some variation
            actual_rate = base_rate * self.rng.uniform(0.5, 1.5)
            conv = int(lead_count * actual_rate)
            conversions.append(min(conv, lead_count))

        # Spend varies by channel (paid channels have spend, organic minimal)
        channel_cpm = {
            "organic": 0,
            "paid_search": 25,
            "social": 15,
            "email": 5,
            "referral": 10,
            "direct": 0,
        }
        spends = []
        for channel, lead_count in zip(channels, leads):
            cpm = channel_cpm[channel]
            if cpm > 0:
                # Cost = CPM * impressions (leads * 100 assumed impressions per lead)
                spend = round(cpm * lead_count * 0.1 * self.rng.uniform(0.8, 1.2), 2)
            else:
                spend = 0
            spends.append(spend)

        df = pd.DataFrame(
            {
                "event_id": event_ids,
                "event_date": event_dates,
                "channel": channels,
                "campaign": campaigns,
                "leads": leads,
                "conversions": conversions,
                "spend": spends,
            }
        )

        df = df.sort_values("event_date").reset_index(drop=True)

        print(f"  Generated {len(df):,} marketing events")
        print(f"  Total leads: {df['leads'].sum():,}")
        print(f"  Total conversions: {df['conversions'].sum():,}")
        print(f"  Total spend: ${df['spend'].sum():,.2f}")
        return df

    def generate_experiments(
        self,
        n_experiments: int,
        customers: pd.DataFrame,
        assignment_rate: float = 0.20,
        start_date: datetime = datetime(2024, 1, 1),
        end_date: datetime = datetime(2025, 12, 31),
    ) -> pd.DataFrame:
        """
        Generate A/B test experiment assignments.

        Args:
            n_experiments: Number of experiments to create
            customers: Customer DataFrame for user assignments
            assignment_rate: Fraction of customers assigned to experiments
            start_date: Experiment period start
            end_date: Experiment period end

        Returns:
            DataFrame with assignment_id, experiment_id, experiment_name,
            user_id, variant, assigned_at, converted
        """
        print(f"Generating {n_experiments} experiments...")

        experiment_names = [
            "pricing_page_redesign",
            "checkout_flow_optimization",
            "onboarding_wizard",
            "email_frequency_test",
            "feature_discovery_prompt",
            "payment_method_order",
            "trial_length_test",
            "homepage_hero_test",
            "notification_timing",
            "upsell_modal_test",
        ]

        n_assignments = int(len(customers) * assignment_rate)
        assigned_customers = self.rng.choice(
            customers["customer_id"].values, size=n_assignments, replace=False
        )

        records = []
        for i, customer_id in enumerate(assigned_customers):
            # Assign to 1-3 experiments
            n_exp = self.rng.choice([1, 1, 1, 2, 2, 3])
            experiments = self.rng.choice(
                range(min(n_experiments, len(experiment_names))), size=n_exp, replace=False
            )

            for exp_idx in experiments:
                exp_id = self._generate_id("exp", exp_idx)
                exp_name = experiment_names[exp_idx % len(experiment_names)]

                # Variant assignment (weighted toward control)
                variant = self.rng.choice(self.VARIANTS, p=[0.5, 0.25, 0.15, 0.10])

                # Assignment date
                date_range = (end_date - start_date).days
                assigned_at = start_date + timedelta(days=int(self.rng.uniform(0, date_range)))

                # Conversion (varies by variant to simulate lift)
                base_conversion_rate = 0.05
                variant_lifts = {
                    "control": 1.0,
                    "variant_a": 1.15,  # 15% lift
                    "variant_b": 0.95,  # 5% drop
                    "variant_c": 1.08,  # 8% lift
                }
                actual_rate = base_conversion_rate * variant_lifts[variant]
                converted = self.rng.random() < actual_rate

                records.append(
                    {
                        "assignment_id": self._generate_id("asgn", len(records)),
                        "experiment_id": exp_id,
                        "experiment_name": exp_name,
                        "user_id": customer_id,
                        "variant": variant,
                        "assigned_at": assigned_at,
                        "converted": converted,
                    }
                )

        df = pd.DataFrame(records)
        df = df.sort_values("assigned_at").reset_index(drop=True)

        print(f"  Generated {len(df):,} experiment assignments")
        print(f"  Unique experiments: {df['experiment_id'].nunique()}")
        print(f"  Conversion rate: {df['converted'].mean():.2%}")
        return df

    def generate_channel_history(
        self,
        marketing_events: pd.DataFrame,
        change_rate: float = 0.10,
    ) -> pd.DataFrame:
        """
        Generate historical channel attribute changes for SCD Type 2 testing.

        Simulates changes like channel reclassification or attribution updates.

        Args:
            marketing_events: Marketing events DataFrame
            change_rate: Fraction of channels with historical changes

        Returns:
            DataFrame with historical channel records
        """
        print(f"Generating channel history (change_rate={change_rate})...")

        # Get unique channels
        unique_channels = marketing_events["channel"].unique()
        n_changes = int(len(unique_channels) * change_rate)

        if n_changes == 0:
            n_changes = 1

        changed_channels = self.rng.choice(
            unique_channels, size=min(n_changes, len(unique_channels)), replace=False
        )

        history_records = []
        for channel in changed_channels:
            # Previous classification
            channel_mappings = {
                "paid_search": "sem",
                "social": "organic_social",
                "email": "crm",
                "referral": "affiliate",
            }

            if channel in channel_mappings:
                history_records.append(
                    {
                        "channel_id": self._generate_id("ch", hash(channel) % 1000),
                        "channel_name": channel_mappings[channel],
                        "channel_type": "historical",
                        "is_paid": channel in ["paid_search"],
                        "valid_from": datetime(2022, 1, 1),
                        "valid_to": datetime(2023, 6, 1),
                        "is_current": False,
                    }
                )

        history_df = pd.DataFrame(history_records)
        print(f"  Generated {len(history_df):,} historical channel records")
        return history_df

    def generate_all(
        self,
        scale: str = "1M",
        include_history: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """
        Generate complete dataset at specified scale.

        Args:
            scale: Data scale ("1M", "10M", "50M")
            include_history: Whether to generate SCD history records

        Returns:
            Dictionary with all generated DataFrames
        """
        config = ScaleConfig.from_scale(scale)
        print(f"\n{'='*60}")
        print(f"Generating {scale} scale dataset")
        print(f"{'='*60}\n")

        # Generate base entities
        customers = self.generate_customers(config.customers)
        products = self.generate_products(config.products)

        # Generate transactional data
        transactions = self.generate_transactions(config.transactions, customers, products)
        marketing_events = self.generate_marketing_events(config.marketing_events)
        experiments = self.generate_experiments(config.experiments, customers)

        result = {
            "customers": customers,
            "products": products,
            "transactions": transactions,
            "marketing_events": marketing_events,
            "experiments": experiments,
        }

        # Generate history for SCD testing
        if include_history:
            result["customer_history"] = self.generate_customer_history(customers)
            result["product_history"] = self.generate_product_history(products)
            result["channel_history"] = self.generate_channel_history(marketing_events)

        print(f"\n{'='*60}")
        print("Generation complete!")
        print(f"{'='*60}")
        for name, df in result.items():
            print(f"  {name}: {len(df):,} rows")

        return result


def main():
    """Example usage."""
    generator = SyntheticDataGenerator(seed=42)

    # Generate 1M scale dataset
    data = generator.generate_all(scale="1M")

    # Print sample from each
    for name, df in data.items():
        print(f"\n{name} sample:")
        print(df.head())


if __name__ == "__main__":
    main()
