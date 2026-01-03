from typing import Dict, List, Optional, Set, Type

import pandas as pd

from app.services.metrics.base import BaseMetric, MetricDefinition, MetricResult


class MetricsEngine:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._registry: Dict[str, Type[BaseMetric]] = {}
        self._data_columns: Set[str] = set(col.lower() for col in df.columns)

    def register(self, metric_class: Type[BaseMetric]):
        temp_df = pd.DataFrame()
        try:
            instance = metric_class.__new__(metric_class)
            instance.df = temp_df
            definition = instance.get_definition()
            self._registry[definition.name] = metric_class
        except Exception:
            pass

    def _has_required_columns(self, metric_class: Type[BaseMetric]) -> bool:
        try:
            instance = metric_class.__new__(metric_class)
            instance.df = pd.DataFrame()
            definition = instance.get_definition()
            required = definition.required_columns or []
            return all(col.lower() in self._data_columns for col in required)
        except Exception:
            return False

    def calculate(self, metric_name: str, **kwargs) -> MetricResult:
        if metric_name not in self._registry:
            raise ValueError(f"Unknown metric: {metric_name}")
        metric_class = self._registry[metric_name]
        metric = metric_class(self.df)
        return metric.calculate(**kwargs)

    def calculate_all(self, category: Optional[str] = None) -> List[MetricResult]:
        results = []
        for name, metric_class in self._registry.items():
            try:
                # Skip metrics that don't have required columns
                if not self._has_required_columns(metric_class):
                    continue

                metric = metric_class(self.df)
                definition = metric.get_definition()
                if category and definition.category != category:
                    continue
                result = metric.calculate()
                results.append(result)
            except (ValueError, KeyError, TypeError):
                continue
        return results

    def calculate_category(self, category: str) -> List[MetricResult]:
        return self.calculate_all(category=category)

    def detect_data_type(self) -> Dict[str, any]:
        cols = self._data_columns

        # Check for revenue/financial data indicators
        has_revenue_data = any(
            c in cols for c in ["amount", "revenue", "price", "total", "payment"]
        )

        # Check for marketing data indicators
        has_marketing_data = any(
            c in cols for c in ["leads", "conversions", "source", "channel", "campaign"]
        )
        has_spend = "spend" in cols

        # Check for customer data
        has_customer_data = any(c in cols for c in ["customer_id", "user_id", "customer", "user"])

        # Determine primary data type
        if has_revenue_data:
            primary_type = "revenue"
        elif has_marketing_data:
            primary_type = "marketing"
        else:
            primary_type = "general"

        # Count applicable metrics
        applicable_metrics = []
        for name, metric_class in self._registry.items():
            if self._has_required_columns(metric_class):
                applicable_metrics.append(name)

        return {
            "primary_type": primary_type,
            "has_revenue_data": has_revenue_data,
            "has_marketing_data": has_marketing_data,
            "has_customer_data": has_customer_data,
            "has_spend_data": has_spend,
            "applicable_metrics": applicable_metrics,
            "applicable_count": len(applicable_metrics),
            "columns_detected": list(cols),
        }

    def list_metrics(self, category: Optional[str] = None) -> List[MetricDefinition]:
        definitions = []
        for metric_class in self._registry.values():
            try:
                instance = metric_class.__new__(metric_class)
                instance.df = pd.DataFrame()
                definition = instance.get_definition()
                if category and definition.category != category:
                    continue
                definitions.append(definition)
            except Exception:
                continue
        return definitions

    def available_metrics(self) -> List[str]:
        return list(self._registry.keys())
