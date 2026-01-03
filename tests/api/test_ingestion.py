from io import BytesIO


def test_upload_csv_success(client):
    csv_content = b"date,amount,customer\n2024-01-01,100,CUST001\n2024-01-02,200,CUST002"
    files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}

    response = client.post("/api/v1/ingestion/upload/csv", files=files)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["valid", "invalid"]
    assert data["file_name"] == "test.csv"
    assert "schema_info" in data


def test_upload_csv_wrong_extension(client):
    csv_content = b"date,amount\n2024-01-01,100"
    files = {"file": ("test.txt", BytesIO(csv_content), "text/plain")}

    response = client.post("/api/v1/ingestion/upload/csv", files=files)

    assert response.status_code == 400
    assert "CSV" in response.json()["detail"]


def test_upload_csv_with_use_case(client):
    csv_content = b"date,amount,customer\n2024-01-01,100,CUST001\n2024-01-02,200,CUST002"
    files = {"file": ("revenue.csv", BytesIO(csv_content), "text/csv")}

    response = client.post(
        "/api/v1/ingestion/upload/csv", files=files, params={"use_case": "revenue"}
    )

    assert response.status_code == 200


def test_upload_csv_schema_detection(client):
    csv_content = (
        b"date,amount,email\n2024-01-01,100.50,test@example.com\n2024-01-02,200.75,user@test.org"
    )
    files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}

    response = client.post("/api/v1/ingestion/upload/csv", files=files)

    assert response.status_code == 200
    data = response.json()
    schema = data["schema_info"]

    assert schema["total_rows"] == 2
    assert schema["total_columns"] == 3
    assert "date" in schema["columns"]
    assert "amount" in schema["columns"]
    assert "email" in schema["columns"]


def test_upload_csv_validation_errors(client):
    csv_content = b"only_column\n1\n2"
    files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}

    response = client.post("/api/v1/ingestion/upload/csv", files=files)

    assert response.status_code == 200
    data = response.json()
    assert data["validation_errors"] is not None
    assert len(data["validation_errors"]) > 0


def test_upload_excel_wrong_extension(client):
    content = b"fake excel content"
    files = {"file": ("test.txt", BytesIO(content), "text/plain")}

    response = client.post("/api/v1/ingestion/upload/excel", files=files)

    assert response.status_code == 400
    assert "Excel" in response.json()["detail"]


def test_list_sources_empty(client):
    response = client.get("/api/v1/ingestion/sources")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_list_sources_with_limit(client):
    response = client.get("/api/v1/ingestion/sources", params={"limit": 5})

    assert response.status_code == 200


def test_get_source_not_found(client):
    response = client.get("/api/v1/ingestion/sources/nonexistent-id")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_upload_and_retrieve_source(client):
    csv_content = b"date,amount\n2024-01-01,100\n2024-01-02,200"
    files = {"file": ("test.csv", BytesIO(csv_content), "text/csv")}

    upload_response = client.post("/api/v1/ingestion/upload/csv", files=files)
    assert upload_response.status_code == 200

    source_id = upload_response.json()["id"]

    get_response = client.get(f"/api/v1/ingestion/sources/{source_id}")
    assert get_response.status_code == 200

    source_data = get_response.json()
    assert source_data["id"] == source_id
    assert source_data["file_name"] == "test.csv"
