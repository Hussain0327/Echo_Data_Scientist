import pandas as pd

from app.services.schema_detector import SchemaDetector


class TestSchemaDetector:
    def test_detect_basic_schema(self):
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "salary": [50000.50, 60000.75, 70000.25],
            }
        )
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.total_rows == 3
        assert schema.total_columns == 3
        assert "name" in schema.columns
        assert "age" in schema.columns
        assert "salary" in schema.columns

    def test_detect_string_type(self):
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["name"].data_type == "string"

    def test_detect_integer_type(self):
        df = pd.DataFrame({"count": [1, 2, 3, 4, 5]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["count"].data_type == "integer"

    def test_detect_numeric_type(self):
        df = pd.DataFrame({"value": [1.5, 2.7, 3.9]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["value"].data_type == "numeric"

    def test_detect_date_string(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02", "2024-01-03"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["date"].data_type == "date"

    def test_detect_datetime_type(self):
        df = pd.DataFrame({"timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["timestamp"].data_type == "datetime"

    def test_detect_email_type(self):
        df = pd.DataFrame({"email": ["alice@example.com", "bob@test.org", "charlie@domain.net"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["email"].data_type == "email"

    def test_detect_url_type(self):
        df = pd.DataFrame(
            {"website": ["https://example.com", "https://test.org", "http://domain.net"]}
        )
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["website"].data_type == "url"

    def test_detect_boolean_string(self):
        df = pd.DataFrame({"active": ["true", "false", "true", "false"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["active"].data_type == "boolean"

    def test_detect_currency_by_name(self):
        df = pd.DataFrame({"amount": [100.50, 200.75, 300.25]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["amount"].data_type == "currency"

    def test_null_count(self):
        df = pd.DataFrame({"value": [1, None, 3, None, 5]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["value"].null_count == 2
        assert schema.columns["value"].nullable is True

    def test_unique_count(self):
        df = pd.DataFrame({"category": ["A", "B", "A", "C", "B"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["category"].unique_count == 3

    def test_sample_values(self):
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert len(schema.columns["name"].sample_values) <= 5
        assert "Alice" in schema.columns["name"].sample_values

    def test_empty_column(self):
        df = pd.DataFrame({"empty": [None, None, None]})
        detector = SchemaDetector(df)
        schema = detector.detect()

        assert schema.columns["empty"].data_type == "unknown"
        assert schema.columns["empty"].null_count == 3
