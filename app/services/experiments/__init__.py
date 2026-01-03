"""
Experimentation service module for A/B testing functionality.

This module provides:
- Statistical analysis for A/B tests (z-test, confidence intervals)
- Experiment management (create, update, submit results)
- Decision engine for shipping/holding experiments
"""

from app.services.experiments.service import ExperimentService
from app.services.experiments.stats import (
    analyze_experiment,
    calculate_confidence_interval,
    calculate_conversion_rate,
    calculate_lift,
    calculate_sample_size_requirement,
    run_proportion_z_test,
)

__all__ = [
    "calculate_conversion_rate",
    "calculate_lift",
    "calculate_confidence_interval",
    "run_proportion_z_test",
    "calculate_sample_size_requirement",
    "analyze_experiment",
    "ExperimentService",
]
