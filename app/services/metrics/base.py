from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from pydantic import BaseModel, Field


class MetricResult(BaseModel):
    metric_name: str
    value: float
    unit: str
    period: str = "all_time"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    calculated_at: datetime = Field(default_factory=datetime.now)

    class Config:
        from_attributes = True


class MetricDefinition(BaseModel):
    name: str
    display_name: str
    description: str
    category: str
    unit: str
    formula: str
    required_columns: List[str]


class BaseMetric(ABC):
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._validate_columns()

    @abstractmethod
    def calculate(self, **kwargs) -> MetricResult:
        pass

    @abstractmethod
    def get_definition(self) -> MetricDefinition:
        pass

    def _validate_columns(self):
        if self.df.empty:
            return
        definition = self.get_definition()
        missing = [col for col in definition.required_columns if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing columns for {definition.name}: {missing}")

    def _format_result(self, value: float, period: str = None, **metadata) -> MetricResult:
        definition = self.get_definition()
        return MetricResult(
            metric_name=definition.name,
            value=round(value, 2),
            unit=definition.unit,
            period=period or "all_time",
            metadata=metadata,
        )
