import pytest

from app.services.experiments.stats import (
    ExperimentAnalysis,
    VariantData,
    analyze_experiment,
    calculate_confidence_interval,
    calculate_conversion_rate,
    calculate_lift,
    calculate_pooled_proportion,
    calculate_sample_size_requirement,
    calculate_statistical_power,
    make_decision,
    run_proportion_z_test,
)


class TestConversionRate:
    def test_basic_conversion_rate(self):
        rate = calculate_conversion_rate(25, 100)
        assert rate == 25.0

    def test_zero_users(self):
        rate = calculate_conversion_rate(0, 0)
        assert rate == 0.0

    def test_full_conversion(self):
        rate = calculate_conversion_rate(100, 100)
        assert rate == 100.0

    def test_no_conversions(self):
        rate = calculate_conversion_rate(0, 100)
        assert rate == 0.0

    def test_decimal_precision(self):
        rate = calculate_conversion_rate(33, 100)
        assert rate == 33.0


class TestLiftCalculations:
    """Tests for lift calculations."""

    def test_positive_lift(self):
        """Test positive lift calculation."""
        absolute, relative = calculate_lift(0.20, 0.25)
        assert absolute == pytest.approx(5.0, rel=0.01)  # 5 percentage points
        assert relative == pytest.approx(25.0, rel=0.01)  # 25% relative improvement

    def test_negative_lift(self):
        """Test negative lift calculation."""
        absolute, relative = calculate_lift(0.25, 0.20)
        assert absolute == pytest.approx(-5.0, rel=0.01)
        assert relative == pytest.approx(-20.0, rel=0.01)

    def test_zero_control_rate(self):
        """Test lift when control rate is zero."""
        absolute, relative = calculate_lift(0.0, 0.10)
        assert absolute == pytest.approx(10.0, rel=0.01)
        assert relative == float("inf")

    def test_no_difference(self):
        """Test lift when rates are equal."""
        absolute, relative = calculate_lift(0.25, 0.25)
        assert absolute == pytest.approx(0.0, abs=0.001)
        assert relative == pytest.approx(0.0, abs=0.001)


class TestVariantData:
    """Tests for VariantData dataclass."""

    def test_conversion_rate_property(self):
        """Test conversion rate property calculation."""
        variant = VariantData(name="control", users=100, conversions=25, is_control=True)
        assert variant.conversion_rate == pytest.approx(0.25, rel=0.01)

    def test_zero_users_conversion_rate(self):
        """Test conversion rate with zero users."""
        variant = VariantData(name="empty", users=0, conversions=0, is_control=False)
        assert variant.conversion_rate == 0.0


class TestPooledProportion:
    """Tests for pooled proportion calculation."""

    def test_basic_pooled_proportion(self):
        """Test basic pooled proportion."""
        control = VariantData("control", users=100, conversions=20, is_control=True)
        variant = VariantData("variant", users=100, conversions=30, is_control=False)

        pooled = calculate_pooled_proportion(control, variant)
        assert pooled == pytest.approx(0.25, rel=0.01)  # (20+30)/(100+100)

    def test_unequal_sample_sizes(self):
        """Test pooled proportion with unequal samples."""
        control = VariantData("control", users=200, conversions=50, is_control=True)
        variant = VariantData("variant", users=100, conversions=25, is_control=False)

        pooled = calculate_pooled_proportion(control, variant)
        assert pooled == pytest.approx(0.25, rel=0.01)  # (50+25)/(200+100)


class TestZTest:
    """Tests for two-proportion z-test."""

    def test_significant_difference(self):
        """Test detection of significant difference."""
        # Large difference with adequate sample
        control = VariantData("control", users=1000, conversions=200, is_control=True)
        variant = VariantData("variant", users=1000, conversions=280, is_control=False)

        z_score, p_value = run_proportion_z_test(control, variant)

        assert z_score > 0  # Variant is better
        assert p_value < 0.05  # Significant

    def test_no_difference(self):
        """Test when there's no real difference."""
        control = VariantData("control", users=100, conversions=25, is_control=True)
        variant = VariantData("variant", users=100, conversions=26, is_control=False)

        z_score, p_value = run_proportion_z_test(control, variant)

        assert p_value > 0.05  # Not significant

    def test_symmetric_results(self):
        """Test that swapping control/variant gives symmetric z-score."""
        control = VariantData("control", users=500, conversions=100, is_control=True)
        variant = VariantData("variant", users=500, conversions=150, is_control=False)

        z1, p1 = run_proportion_z_test(control, variant)
        z2, p2 = run_proportion_z_test(variant, control)

        assert z1 == pytest.approx(-z2, rel=0.01)
        assert p1 == pytest.approx(p2, rel=0.01)


class TestConfidenceInterval:
    """Tests for confidence interval calculation."""

    def test_95_confidence_interval(self):
        """Test 95% confidence interval."""
        control = VariantData("control", users=1000, conversions=200, is_control=True)
        variant = VariantData("variant", users=1000, conversions=250, is_control=False)

        lower, upper = calculate_confidence_interval(control, variant, 0.95)

        # CI should contain the true difference and be symmetric around the point estimate
        point_estimate = (0.25 - 0.20) * 100  # 5 percentage points
        assert lower < point_estimate < upper
        assert lower > 0  # Both bounds positive means significant positive effect

    def test_narrower_interval_with_larger_sample(self):
        """Test that larger samples give narrower intervals."""
        # Small sample
        control_small = VariantData("control", users=100, conversions=20, is_control=True)
        variant_small = VariantData("variant", users=100, conversions=25, is_control=False)

        lower_small, upper_small = calculate_confidence_interval(control_small, variant_small)
        width_small = upper_small - lower_small

        # Large sample (same proportions)
        control_large = VariantData("control", users=1000, conversions=200, is_control=True)
        variant_large = VariantData("variant", users=1000, conversions=250, is_control=False)

        lower_large, upper_large = calculate_confidence_interval(control_large, variant_large)
        width_large = upper_large - lower_large

        assert width_large < width_small


class TestSampleSizeRequirement:
    """Tests for sample size calculation."""

    def test_basic_sample_size(self):
        """Test basic sample size calculation."""
        # 20% baseline, want to detect 5pp improvement
        n = calculate_sample_size_requirement(
            baseline_rate=0.20,
            minimum_detectable_effect=5.0,  # 5 percentage points
            alpha=0.05,
            power=0.80,
        )

        assert n > 0
        assert isinstance(n, int)

    def test_smaller_effect_needs_more_samples(self):
        """Test that smaller effects require larger samples."""
        n_large_effect = calculate_sample_size_requirement(0.20, 10.0)  # 10pp
        n_small_effect = calculate_sample_size_requirement(0.20, 2.0)  # 2pp

        assert n_small_effect > n_large_effect

    def test_higher_power_needs_more_samples(self):
        """Test that higher power requires larger samples."""
        n_80_power = calculate_sample_size_requirement(0.20, 5.0, power=0.80)
        n_95_power = calculate_sample_size_requirement(0.20, 5.0, power=0.95)

        assert n_95_power > n_80_power


class TestStatisticalPower:
    """Tests for statistical power calculation."""

    def test_power_increases_with_sample_size(self):
        """Test that power increases with larger samples."""
        control_small = VariantData("control", users=100, conversions=20, is_control=True)
        variant_small = VariantData("variant", users=100, conversions=30, is_control=False)
        power_small = calculate_statistical_power(control_small, variant_small)

        control_large = VariantData("control", users=1000, conversions=200, is_control=True)
        variant_large = VariantData("variant", users=1000, conversions=300, is_control=False)
        power_large = calculate_statistical_power(control_large, variant_large)

        assert power_large > power_small

    def test_power_increases_with_effect_size(self):
        """Test that power increases with larger effect size."""
        control = VariantData("control", users=500, conversions=100, is_control=True)

        variant_small_effect = VariantData("variant", users=500, conversions=110, is_control=False)
        power_small = calculate_statistical_power(control, variant_small_effect)

        variant_large_effect = VariantData("variant", users=500, conversions=150, is_control=False)
        power_large = calculate_statistical_power(control, variant_large_effect)

        assert power_large > power_small


class TestAnalyzeExperiment:
    """Tests for the main analyze_experiment function."""

    def test_complete_analysis(self):
        """Test that analyze_experiment returns all required fields."""
        control = VariantData("control", users=1000, conversions=200, is_control=True)
        variant = VariantData("variant", users=1000, conversions=260, is_control=False)

        analysis = analyze_experiment(control, variant)

        assert isinstance(analysis, ExperimentAnalysis)
        assert analysis.control_conversion_rate == pytest.approx(20.0, rel=0.01)
        assert analysis.variant_conversion_rate == pytest.approx(26.0, rel=0.01)
        assert analysis.absolute_lift == pytest.approx(6.0, rel=0.1)
        assert analysis.relative_lift == pytest.approx(30.0, rel=0.1)
        assert analysis.z_score != 0
        assert 0 <= analysis.p_value <= 1
        assert analysis.confidence_interval_lower < analysis.confidence_interval_upper
        assert analysis.is_significant in (True, False)  # numpy bool compatibility
        assert analysis.decision in ["ship_variant", "keep_control", "inconclusive", "pending"]

    def test_significant_positive_result(self):
        """Test analysis with significant positive result."""
        control = VariantData("control", users=1000, conversions=200, is_control=True)
        variant = VariantData("variant", users=1000, conversions=280, is_control=False)

        analysis = analyze_experiment(control, variant, alpha=0.05)

        assert analysis.is_significant
        assert analysis.relative_lift > 0
        assert analysis.decision == "ship_variant"

    def test_significant_negative_result(self):
        """Test analysis with significant negative result."""
        control = VariantData("control", users=1000, conversions=280, is_control=True)
        variant = VariantData("variant", users=1000, conversions=200, is_control=False)

        analysis = analyze_experiment(control, variant, alpha=0.05)

        assert analysis.is_significant
        assert analysis.relative_lift < 0
        assert analysis.decision == "keep_control"

    def test_inconclusive_result(self):
        """Test analysis with inconclusive result."""
        control = VariantData("control", users=100, conversions=22, is_control=True)
        variant = VariantData("variant", users=100, conversions=25, is_control=False)

        analysis = analyze_experiment(control, variant, alpha=0.05)

        # Small sample, small difference - should be inconclusive
        assert analysis.decision == "inconclusive"


class TestMakeDecision:
    """Tests for decision logic."""

    def test_ship_variant_decision(self):
        """Test ship_variant decision."""
        analysis = ExperimentAnalysis(
            control_conversion_rate=20.0,
            variant_conversion_rate=26.0,
            absolute_lift=6.0,
            relative_lift=30.0,
            confidence_interval_lower=2.0,
            confidence_interval_upper=10.0,
            z_score=3.0,
            p_value=0.003,
            is_significant=True,
            sample_size_adequate=True,
            power=0.85,
        )

        decision, rationale = make_decision(analysis, alpha=0.05)

        assert decision == "ship_variant"
        assert "recommend shipping" in rationale.lower()

    def test_keep_control_decision(self):
        """Test keep_control decision."""
        analysis = ExperimentAnalysis(
            control_conversion_rate=26.0,
            variant_conversion_rate=20.0,
            absolute_lift=-6.0,
            relative_lift=-23.0,
            confidence_interval_lower=-10.0,
            confidence_interval_upper=-2.0,
            z_score=-3.0,
            p_value=0.003,
            is_significant=True,
            sample_size_adequate=True,
            power=0.85,
        )

        decision, rationale = make_decision(analysis, alpha=0.05)

        assert decision == "keep_control"
        assert "keep" in rationale.lower() or "control" in rationale.lower()

    def test_inconclusive_not_significant(self):
        """Test inconclusive decision when not significant."""
        analysis = ExperimentAnalysis(
            control_conversion_rate=20.0,
            variant_conversion_rate=22.0,
            absolute_lift=2.0,
            relative_lift=10.0,
            confidence_interval_lower=-3.0,
            confidence_interval_upper=7.0,
            z_score=1.2,
            p_value=0.23,
            is_significant=False,
            sample_size_adequate=True,
            power=0.30,
        )

        decision, rationale = make_decision(analysis, alpha=0.05)

        assert decision == "inconclusive"
        assert "not statistically significant" in rationale.lower()

    def test_inconclusive_inadequate_sample(self):
        """Test inconclusive decision when sample size is inadequate."""
        analysis = ExperimentAnalysis(
            control_conversion_rate=20.0,
            variant_conversion_rate=26.0,
            absolute_lift=6.0,
            relative_lift=30.0,
            confidence_interval_lower=-5.0,
            confidence_interval_upper=17.0,
            z_score=1.5,
            p_value=0.13,
            is_significant=False,
            sample_size_adequate=False,  # Key difference
            power=0.40,
        )

        decision, rationale = make_decision(analysis, alpha=0.05)

        assert decision == "inconclusive"
        assert "sample size" in rationale.lower()


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_perfect_conversion_control(self):
        """Test when control has 100% conversion."""
        control = VariantData("control", users=100, conversions=100, is_control=True)
        variant = VariantData("variant", users=100, conversions=95, is_control=False)

        analysis = analyze_experiment(control, variant)

        assert analysis.control_conversion_rate == 100.0
        assert analysis.relative_lift < 0

    def test_zero_conversion_control(self):
        """Test when control has 0% conversion."""
        control = VariantData("control", users=100, conversions=0, is_control=True)
        variant = VariantData("variant", users=100, conversions=10, is_control=False)

        analysis = analyze_experiment(control, variant)

        assert analysis.control_conversion_rate == 0.0
        assert analysis.variant_conversion_rate == 10.0

    def test_very_small_sample(self):
        """Test with very small sample sizes."""
        control = VariantData("control", users=10, conversions=2, is_control=True)
        variant = VariantData("variant", users=10, conversions=4, is_control=False)

        analysis = analyze_experiment(control, variant)

        # Should complete without error
        assert analysis is not None
        # Should likely be inconclusive due to small sample
        assert not analysis.sample_size_adequate or analysis.decision == "inconclusive"

    def test_equal_conversion_rates(self):
        """Test when conversion rates are exactly equal."""
        control = VariantData("control", users=500, conversions=100, is_control=True)
        variant = VariantData("variant", users=500, conversions=100, is_control=False)

        analysis = analyze_experiment(control, variant)

        assert analysis.absolute_lift == pytest.approx(0.0, abs=0.1)
        assert analysis.relative_lift == pytest.approx(0.0, abs=0.1)
        assert not analysis.is_significant
