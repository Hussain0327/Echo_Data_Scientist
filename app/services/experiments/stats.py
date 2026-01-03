import math
from dataclasses import dataclass
from typing import Optional, Tuple

from scipy import stats as scipy_stats


@dataclass
class VariantData:
    name: str
    users: int
    conversions: int
    is_control: bool = False

    @property
    def conversion_rate(self) -> float:
        if self.users == 0:
            return 0.0
        return self.conversions / self.users


@dataclass
class ExperimentAnalysis:
    control_conversion_rate: float
    variant_conversion_rate: float
    absolute_lift: float  # Percentage points
    relative_lift: float  # Percentage
    confidence_interval_lower: float
    confidence_interval_upper: float
    z_score: float
    p_value: float
    is_significant: bool
    sample_size_adequate: bool
    power: Optional[float] = None
    decision: str = "pending"
    decision_rationale: str = ""


def calculate_conversion_rate(conversions: int, users: int) -> float:
    if users == 0:
        return 0.0
    return (conversions / users) * 100


def calculate_lift(control_rate: float, variant_rate: float) -> Tuple[float, float]:
    # Absolute lift in percentage points
    absolute_lift = (variant_rate - control_rate) * 100

    # Relative lift as percentage improvement
    if control_rate == 0:
        relative_lift = float("inf") if variant_rate > 0 else 0.0
    else:
        relative_lift = ((variant_rate - control_rate) / control_rate) * 100

    return absolute_lift, relative_lift


def calculate_pooled_proportion(control: VariantData, variant: VariantData) -> float:
    total_conversions = control.conversions + variant.conversions
    total_users = control.users + variant.users

    if total_users == 0:
        return 0.0

    return total_conversions / total_users


def calculate_standard_error(
    control: VariantData, variant: VariantData, pooled: bool = True
) -> float:
    if pooled:
        p_pooled = calculate_pooled_proportion(control, variant)
        se = math.sqrt(p_pooled * (1 - p_pooled) * (1 / control.users + 1 / variant.users))
    else:
        # Unpooled SE for confidence intervals
        p1 = control.conversion_rate
        p2 = variant.conversion_rate
        se = math.sqrt((p1 * (1 - p1) / control.users) + (p2 * (1 - p2) / variant.users))

    return se


def run_proportion_z_test(control: VariantData, variant: VariantData) -> Tuple[float, float]:
    p1 = control.conversion_rate
    p2 = variant.conversion_rate

    se = calculate_standard_error(control, variant, pooled=True)

    if se == 0:
        return 0.0, 1.0

    z_score = (p2 - p1) / se

    # Two-tailed p-value
    p_value = 2 * (1 - scipy_stats.norm.cdf(abs(z_score)))

    return z_score, p_value


def calculate_confidence_interval(
    control: VariantData, variant: VariantData, confidence_level: float = 0.95
) -> Tuple[float, float]:
    p1 = control.conversion_rate
    p2 = variant.conversion_rate
    diff = p2 - p1

    # Use unpooled SE for confidence intervals
    se = calculate_standard_error(control, variant, pooled=False)

    # Z critical value for the given confidence level
    alpha = 1 - confidence_level
    z_critical = scipy_stats.norm.ppf(1 - alpha / 2)

    margin_of_error = z_critical * se

    # Convert to percentage points
    lower = (diff - margin_of_error) * 100
    upper = (diff + margin_of_error) * 100

    return lower, upper


def calculate_sample_size_requirement(
    baseline_rate: float, minimum_detectable_effect: float, alpha: float = 0.05, power: float = 0.80
) -> int:
    if baseline_rate <= 0 or baseline_rate >= 1:
        return 0

    # Convert MDE from percentage points to proportion
    mde = minimum_detectable_effect / 100
    p1 = baseline_rate
    p2 = baseline_rate + mde

    # Z values
    z_alpha = scipy_stats.norm.ppf(1 - alpha / 2)
    z_beta = scipy_stats.norm.ppf(power)

    # Pooled proportion estimate
    p_pooled = (p1 + p2) / 2

    # Sample size formula
    numerator = (
        z_alpha * math.sqrt(2 * p_pooled * (1 - p_pooled))
        + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))
    ) ** 2

    denominator = (p2 - p1) ** 2

    if denominator == 0:
        return 0

    n = numerator / denominator

    return math.ceil(n)


def calculate_statistical_power(
    control: VariantData, variant: VariantData, alpha: float = 0.05
) -> float:
    p1 = control.conversion_rate
    p2 = variant.conversion_rate

    if p1 == p2:
        return alpha  # Power equals alpha when there's no effect

    # Effect size
    effect = abs(p2 - p1)

    # Pooled SE under null
    p_pooled = calculate_pooled_proportion(control, variant)
    se_null = math.sqrt(2 * p_pooled * (1 - p_pooled) / min(control.users, variant.users))

    # SE under alternative
    se_alt = math.sqrt((p1 * (1 - p1) / control.users) + (p2 * (1 - p2) / variant.users))

    if se_alt == 0:
        return 1.0 if effect > 0 else alpha

    # Z critical value
    z_alpha = scipy_stats.norm.ppf(1 - alpha / 2)

    # Calculate power
    z_power = (effect - z_alpha * se_null) / se_alt
    power = scipy_stats.norm.cdf(z_power)

    return min(max(power, 0), 1)


def make_decision(analysis: ExperimentAnalysis, alpha: float = 0.05) -> Tuple[str, str]:
    if not analysis.sample_size_adequate:
        return (
            "inconclusive",
            "Insufficient sample size. The test may not have enough power to detect "
            "the expected effect. Consider running longer to collect more data.",
        )

    if analysis.p_value > alpha:
        return (
            "inconclusive",
            f"The difference is not statistically significant (p={analysis.p_value:.4f} > {alpha}). "
            f"We cannot confidently conclude there is a real difference between variants. "
            f"The observed {analysis.relative_lift:+.1f}% lift could be due to random chance.",
        )

    if analysis.relative_lift > 0:
        return (
            "ship_variant",
            f"Statistically significant positive effect detected (p={analysis.p_value:.4f}). "
            f"The variant shows a {analysis.relative_lift:+.1f}% relative improvement "
            f"({analysis.absolute_lift:+.2f} percentage points). "
            f"95% CI: [{analysis.confidence_interval_lower:+.2f}, {analysis.confidence_interval_upper:+.2f}] pp. "
            f"Recommend shipping the variant.",
        )
    else:
        return (
            "keep_control",
            f"Statistically significant negative effect detected (p={analysis.p_value:.4f}). "
            f"The variant shows a {analysis.relative_lift:.1f}% relative decrease "
            f"({analysis.absolute_lift:.2f} percentage points). "
            f"Recommend keeping the control.",
        )


def analyze_experiment(
    control: VariantData,
    variant: VariantData,
    alpha: float = 0.05,
    minimum_sample_size: Optional[int] = None,
) -> ExperimentAnalysis:
    # Calculate conversion rates
    control_rate = control.conversion_rate
    variant_rate = variant.conversion_rate

    # Calculate lift
    absolute_lift, relative_lift = calculate_lift(control_rate, variant_rate)

    # Run z-test
    z_score, p_value = run_proportion_z_test(control, variant)

    # Calculate confidence interval
    ci_lower, ci_upper = calculate_confidence_interval(control, variant, 1 - alpha)

    # Calculate power
    power = calculate_statistical_power(control, variant, alpha)

    # Check sample size adequacy
    if minimum_sample_size:
        sample_size_adequate = (
            control.users >= minimum_sample_size and variant.users >= minimum_sample_size
        )
    else:
        # Rule of thumb: at least 100 per variant, or enough for 80% power
        sample_size_adequate = (
            control.users >= 100 and variant.users >= 100 and power >= 0.5  # At least 50% power
        )

    # Build analysis object
    analysis = ExperimentAnalysis(
        control_conversion_rate=control_rate * 100,  # Convert to percentage
        variant_conversion_rate=variant_rate * 100,
        absolute_lift=absolute_lift,
        relative_lift=relative_lift,
        confidence_interval_lower=ci_lower,
        confidence_interval_upper=ci_upper,
        z_score=z_score,
        p_value=p_value,
        is_significant=p_value <= alpha,
        sample_size_adequate=sample_size_adequate,
        power=power,
    )

    # Make decision
    decision, rationale = make_decision(analysis, alpha)
    analysis.decision = decision
    analysis.decision_rationale = rationale

    return analysis
