import pandas as pd

from app.services.data_validator import DataValidator


class TestDataValidator:
    def test_valid_data_passes(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "amount": [100, 200, 300],
                "customer": ["A", "B", "C"],
            }
        )
        validator = DataValidator(df)
        errors = validator.validate()

        error_severities = [e.severity for e in errors]
        assert "error" not in error_severities

    def test_empty_file_error(self):
        df = pd.DataFrame()
        validator = DataValidator(df)
        errors = validator.validate()

        error_messages = [e.message for e in errors]
        assert any("empty" in m.lower() for m in error_messages)

    def test_single_column_error(self):
        df = pd.DataFrame({"only_column": [1, 2, 3]})
        validator = DataValidator(df)
        errors = validator.validate()

        error_messages = [e.message for e in errors]
        assert any("at least 2 columns" in m for m in error_messages)

    def test_few_rows_warning(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "amount": [100, 200]})
        validator = DataValidator(df)
        errors = validator.validate()

        warnings = [e for e in errors if e.severity == "warning"]
        warning_messages = [e.message for e in warnings]
        assert any("only" in m.lower() and "rows" in m.lower() for m in warning_messages)

    def test_high_null_percentage_error(self):
        df = pd.DataFrame(
            {"col1": [None, None, None, None, 1], "col2": [None, None, None, None, 2]}
        )
        validator = DataValidator(df)
        errors = validator.validate()

        error_messages = [e.message for e in errors if e.severity == "error"]
        assert any("missing values" in m for m in error_messages)

    def test_moderate_null_percentage_warning(self):
        df = pd.DataFrame({"date": ["2024-01-01"] * 10, "amount": [100] * 5 + [None] * 5})
        validator = DataValidator(df)
        errors = validator.validate()

        warnings = [e for e in errors if e.severity == "warning"]
        warning_messages = [e.message for e in warnings]
        assert any("missing values" in m for m in warning_messages)

    def test_no_date_columns_warning(self):
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "amount": [100, 200, 300]})
        validator = DataValidator(df)
        errors = validator.validate()

        warnings = [e for e in errors if e.severity == "warning"]
        warning_fields = [e.field for e in warnings]
        assert "dates" in warning_fields

    def test_no_numeric_columns_error(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        validator = DataValidator(df)
        errors = validator.validate()

        has_numeric_error = any("numeric" in e.message.lower() for e in errors)
        assert has_numeric_error

    def test_duplicate_columns_error(self):
        df = pd.DataFrame([[1, 2, 3]], columns=["a", "b", "a"])
        validator = DataValidator(df)
        errors = validator.validate()

        error_messages = [e.message for e in errors if e.severity == "error"]
        assert any("duplicate" in m.lower() for m in error_messages)

    def test_revenue_use_case_missing_amount(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "customer": ["A", "B"]})
        validator = DataValidator(df, use_case="revenue")
        errors = validator.validate()

        error_fields = [e.field for e in errors if e.severity == "error"]
        assert "amount" in error_fields

    def test_revenue_use_case_valid(self):
        df = pd.DataFrame(
            {"date": ["2024-01-01", "2024-01-02"], "amount": [100, 200], "customer": ["A", "B"]}
        )
        validator = DataValidator(df, use_case="revenue")
        errors = validator.validate()

        error_severities = [e.severity for e in errors]
        assert "error" not in error_severities

    def test_marketing_use_case_missing_source(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "leads": [100, 200]})
        validator = DataValidator(df, use_case="marketing")
        errors = validator.validate()

        error_fields = [e.field for e in errors if e.severity == "error"]
        assert "source" in error_fields

    def test_suggestions_provided(self):
        df = pd.DataFrame()
        validator = DataValidator(df)
        errors = validator.validate()

        for error in errors:
            assert error.suggestion is not None
            assert len(error.suggestion) > 0
