import json
from pathlib import Path
from typing import Optional

EXPECTATIONS_DIR = Path(__file__).parent / "expectations"


def get_expectation_suite(name: str) -> Optional[dict]:
    if not name.endswith("_suite"):
        name = f"{name}_suite"

    suite_path = EXPECTATIONS_DIR / f"{name}.json"
    if not suite_path.exists():
        suite_path = EXPECTATIONS_DIR / f"{name.replace('_suite', '_data_suite')}.json"

    if not suite_path.exists():
        return None

    with open(suite_path) as f:
        return json.load(f)


def list_suites() -> list[str]:
    if not EXPECTATIONS_DIR.exists():
        return []

    suites = []
    for path in EXPECTATIONS_DIR.glob("*.json"):
        suites.append(path.stem)

    return suites


def get_expectations_for_data_type(data_type: str) -> list[dict]:
    suite = get_expectation_suite(f"{data_type}_data_suite")
    if suite is None:
        return []
    return suite.get("expectations", [])


INLINE_EXPECTATIONS = {
    "revenue": [
        {"expectation_type": "expect_column_to_exist", "column": "amount"},
        {"expectation_type": "expect_column_to_exist", "column": "date"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "amount"},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "amount",
            "min_value": 0,
        },
    ],
    "marketing": [
        {"expectation_type": "expect_column_to_exist", "column": "source"},
        {"expectation_type": "expect_column_to_exist", "column": "leads"},
        {"expectation_type": "expect_column_to_exist", "column": "conversions"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "source"},
        {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "leads",
            "min_value": 0,
        },
    ],
    "experiment": [
        {"expectation_type": "expect_column_to_exist", "column": "user_id"},
        {"expectation_type": "expect_column_to_exist", "column": "variant"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "user_id"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "variant"},
    ],
    "customers": [
        {"expectation_type": "expect_column_to_exist", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_be_unique", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
    ],
}


def get_inline_expectations(data_type: str) -> list[dict]:
    return INLINE_EXPECTATIONS.get(data_type, [])
