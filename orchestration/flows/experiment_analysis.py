from datetime import datetime

from prefect import flow, get_run_logger, task

from orchestration.tasks.extract import extract_csv
from orchestration.tasks.validate import run_expectations

EXPERIMENT_EXPECTATIONS = [
    {"expectation_type": "expect_column_to_exist", "column": "user_id"},
    {"expectation_type": "expect_column_to_exist", "column": "variant"},
    {"expectation_type": "expect_column_values_to_not_be_null", "column": "user_id"},
    {"expectation_type": "expect_column_values_to_not_be_null", "column": "variant"},
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "variant",
        "value_set": ["control", "variant_a", "variant_b", "variant_c", "treatment"],
    },
]


@task
def aggregate_variant_results(df, variant_col: str, conversion_col: str):
    logger = get_run_logger()

    results = {}
    for variant in df[variant_col].unique():
        variant_df = df[df[variant_col] == variant]
        users = len(variant_df)
        conversions = variant_df[conversion_col].sum()

        results[variant] = {
            "users": users,
            "conversions": int(conversions),
            "conversion_rate": conversions / users if users > 0 else 0,
        }
        logger.info(
            f"{variant}: {users} users, {conversions} conversions, {results[variant]['conversion_rate']:.2%}"
        )

    return results


@task
def run_statistical_analysis(variant_results: dict, control_name: str = "control"):
    from app.services.experiments.stats import VariantData, analyze_experiment

    control_data = variant_results.get(control_name)
    if not control_data:
        raise ValueError(f"Control variant '{control_name}' not found")

    control = VariantData(
        name=control_name,
        users=control_data["users"],
        conversions=control_data["conversions"],
        is_control=True,
    )

    results = {}
    for variant_name, data in variant_results.items():
        if variant_name == control_name:
            continue

        variant = VariantData(
            name=variant_name,
            users=data["users"],
            conversions=data["conversions"],
            is_control=False,
        )

        analysis = analyze_experiment(control, variant)

        results[variant_name] = {
            "control_rate": analysis.control_conversion_rate,
            "variant_rate": analysis.variant_conversion_rate,
            "absolute_lift": analysis.absolute_lift,
            "relative_lift": analysis.relative_lift,
            "p_value": analysis.p_value,
            "is_significant": analysis.is_significant,
            "confidence_interval": [
                analysis.confidence_interval_lower,
                analysis.confidence_interval_upper,
            ],
            "decision": analysis.decision,
            "rationale": analysis.decision_rationale,
        }

    return results


@task
def generate_experiment_report(
    experiment_name: str, variant_results: dict, statistical_results: dict
) -> dict:
    report = {
        "experiment_name": experiment_name,
        "generated_at": datetime.utcnow().isoformat(),
        "variants": variant_results,
        "analysis": statistical_results,
        "summary": {},
    }

    for variant_name, stats in statistical_results.items():
        if stats["is_significant"]:
            if stats["relative_lift"] > 0:
                report["summary"][
                    variant_name
                ] = f"Winner: +{stats['relative_lift']:.1f}% lift (p={stats['p_value']:.4f})"
            else:
                report["summary"][
                    variant_name
                ] = f"Loser: {stats['relative_lift']:.1f}% lift (p={stats['p_value']:.4f})"
        else:
            report["summary"][
                variant_name
            ] = f"Inconclusive: {stats['relative_lift']:+.1f}% lift (p={stats['p_value']:.4f})"

    return report


@flow(name="experiment_analysis_pipeline", log_prints=True)
def experiment_analysis_pipeline(
    data_file: str,
    experiment_name: str,
    variant_column: str = "variant",
    conversion_column: str = "converted",
    control_name: str = "control",
    validate_data: bool = True,
):
    logger = get_run_logger()
    logger.info(f"Starting experiment analysis: {experiment_name}")

    df = extract_csv(data_file)
    logger.info(f"Loaded {len(df)} rows")

    if validate_data:
        validation = run_expectations(df, EXPERIMENT_EXPECTATIONS)
        if not validation["success"]:
            logger.warning(f"Validation issues: {validation['failed_count']} failed")

    variant_results = aggregate_variant_results(df, variant_column, conversion_column)

    statistical_results = run_statistical_analysis(variant_results, control_name)

    report = generate_experiment_report(experiment_name, variant_results, statistical_results)

    for variant, summary in report["summary"].items():
        logger.info(f"{variant}: {summary}")

    return report


@flow(name="multi_experiment_analysis", log_prints=True)
def multi_experiment_analysis(experiments: list[dict]):
    logger = get_run_logger()
    logger.info(f"Analyzing {len(experiments)} experiments")

    results = []
    for exp in experiments:
        try:
            result = experiment_analysis_pipeline(
                data_file=exp["data_file"],
                experiment_name=exp["name"],
                variant_column=exp.get("variant_column", "variant"),
                conversion_column=exp.get("conversion_column", "converted"),
                control_name=exp.get("control_name", "control"),
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to analyze {exp['name']}: {e}")
            results.append({"experiment_name": exp["name"], "error": str(e)})

    return results


if __name__ == "__main__":
    experiment_analysis_pipeline(
        data_file="data/experiments/onboarding_ab_test.csv",
        experiment_name="Onboarding Flow Test",
        variant_column="variant",
        conversion_column="activated",
        control_name="control",
    )
