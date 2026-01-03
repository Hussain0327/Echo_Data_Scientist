def test_start_session(client):
    response = client.post(
        "/api/v1/analytics/session/start",
        json={"task_type": "report_generation", "baseline_time_seconds": 7200.0},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["task_type"] == "report_generation"
    assert data["start_time"] is not None
    assert data["end_time"] is None


def test_end_session(client):
    start_response = client.post(
        "/api/v1/analytics/session/start", json={"task_type": "chat_interaction"}
    )
    session_id = start_response.json()["id"]

    end_response = client.post("/api/v1/analytics/session/end", json={"session_id": session_id})

    assert end_response.status_code == 200
    data = end_response.json()
    assert data["end_time"] is not None
    assert data["duration_seconds"] is not None


def test_get_session(client):
    start_response = client.post(
        "/api/v1/analytics/session/start", json={"task_type": "metric_calculation"}
    )
    session_id = start_response.json()["id"]

    get_response = client.get(f"/api/v1/analytics/session/{session_id}")

    assert get_response.status_code == 200
    data = get_response.json()
    assert data["id"] == session_id
    assert data["task_type"] == "metric_calculation"


def test_get_session_not_found(client):
    response = client.get("/api/v1/analytics/session/nonexistent")
    assert response.status_code == 404


def test_get_sessions(client):
    client.post("/api/v1/analytics/session/start", json={"task_type": "report_generation"})
    client.post("/api/v1/analytics/session/start", json={"task_type": "chat_interaction"})

    response = client.get("/api/v1/analytics/sessions")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2


def test_time_savings_stats(client):
    response = client.get("/api/v1/analytics/time-savings")

    assert response.status_code == 200
    data = response.json()
    assert "total_sessions" in data
    assert "total_time_saved_hours" in data
    assert "avg_time_saved_hours" in data
    assert "avg_duration_minutes" in data
    assert "sessions_by_task_type" in data


def test_satisfaction_stats(client):
    response = client.get("/api/v1/analytics/satisfaction")

    assert response.status_code == 200
    data = response.json()
    assert "total_ratings" in data
    assert "avg_rating" in data
    assert "rating_distribution" in data
    assert "ratings_by_interaction_type" in data


def test_accuracy_stats(client):
    response = client.get("/api/v1/analytics/accuracy")

    assert response.status_code == 200
    data = response.json()
    assert "total_ratings" in data
    assert "accuracy_rate" in data
    assert "accuracy_distribution" in data


def test_usage_stats(client):
    response = client.get("/api/v1/analytics/usage")

    assert response.status_code == 200
    data = response.json()
    assert "total_sessions" in data
    assert "total_reports" in data
    assert "total_chats" in data
    assert "most_used_metrics" in data
    assert "sessions_per_day" in data


def test_analytics_overview(client):
    response = client.get("/api/v1/analytics/overview")

    assert response.status_code == 200
    data = response.json()
    assert "time_savings" in data
    assert "satisfaction" in data
    assert "accuracy" in data
    assert "usage" in data


def test_portfolio_stats(client):
    response = client.get("/api/v1/analytics/portfolio")

    assert response.status_code == 200
    data = response.json()
    assert "total_sessions" in data
    assert "total_time_saved_hours" in data
    assert "avg_time_saved_hours" in data
    assert "avg_satisfaction_rating" in data
    assert "accuracy_rate" in data
    assert "total_insights_generated" in data
    assert "headline_metrics" in data
    assert isinstance(data["headline_metrics"], dict)
