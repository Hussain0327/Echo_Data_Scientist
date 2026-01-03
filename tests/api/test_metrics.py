import io

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def revenue_csv():
    return b"""date,amount,customer_id,status,product
2024-01-01,100.00,CUST001,paid,Basic
2024-01-02,200.00,CUST002,paid,Pro
2024-01-03,300.00,CUST003,paid,Pro
2024-02-01,150.00,CUST001,paid,Basic
2024-02-02,250.00,CUST002,paid,Pro
"""


@pytest.fixture
def marketing_csv():
    return b"""date,source,campaign,leads,conversions,spend
2024-01-01,Google,Winter Sale,100,10,500
2024-01-02,Google,Winter Sale,120,12,600
2024-01-01,Facebook,Winter Sale,80,8,400
2024-01-02,Facebook,Winter Sale,90,9,450
"""


@pytest.mark.asyncio
async def test_list_available_metrics():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/metrics/available")

    assert response.status_code == 200
    data = response.json()
    assert "revenue" in data
    assert "financial" in data
    assert "marketing" in data


@pytest.mark.asyncio
async def test_calculate_revenue_metrics(revenue_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/revenue",
            files={"file": ("test.csv", io.BytesIO(revenue_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["category"] == "revenue"
    assert data["metrics_calculated"] > 0
    assert len(data["results"]) > 0


@pytest.mark.asyncio
async def test_calculate_marketing_metrics(marketing_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/marketing",
            files={"file": ("test.csv", io.BytesIO(marketing_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["category"] == "marketing"
    assert data["metrics_calculated"] > 0


@pytest.mark.asyncio
async def test_calculate_specific_metric(revenue_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/csv?metrics=total_revenue",
            files={"file": ("test.csv", io.BytesIO(revenue_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["metric_name"] == "total_revenue"


@pytest.mark.asyncio
async def test_calculate_all_metrics(revenue_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/csv?metrics=all",
            files={"file": ("test.csv", io.BytesIO(revenue_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["metrics_calculated"] > 0


@pytest.mark.asyncio
async def test_trend_analysis(revenue_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/trend?value_column=amount&date_column=date&period=month",
            files={"file": ("test.csv", io.BytesIO(revenue_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert "trend" in data
    assert "comparison" in data
    assert "date_range" in data


@pytest.mark.asyncio
async def test_growth_analysis(revenue_csv):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/growth?value_column=amount&date_column=date&period=month",
            files={"file": ("test.csv", io.BytesIO(revenue_csv), "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert "growth_data" in data


@pytest.mark.asyncio
async def test_invalid_file_type():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/csv",
            files={"file": ("test.txt", io.BytesIO(b"test"), "text/plain")},
        )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_empty_file():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/calculate/csv",
            files={"file": ("test.csv", io.BytesIO(b""), "text/csv")},
        )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_missing_column_for_trend():
    csv_data = b"date,amount\n2024-01-01,100"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/metrics/trend?value_column=missing_col&date_column=date",
            files={"file": ("test.csv", io.BytesIO(csv_data), "text/csv")},
        )

    assert response.status_code == 400
