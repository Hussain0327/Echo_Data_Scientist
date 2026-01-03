def test_submit_feedback(client):
    response = client.post(
        "/api/v1/feedback",
        json={
            "interaction_type": "report",
            "rating": 5,
            "feedback_text": "Great report!",
            "accuracy_rating": "correct",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["interaction_type"] == "report"
    assert data["rating"] == 5
    assert data["feedback_text"] == "Great report!"
    assert data["accuracy_rating"] == "correct"


def test_submit_feedback_minimal(client):
    response = client.post("/api/v1/feedback", json={"interaction_type": "chat"})

    assert response.status_code == 200
    data = response.json()
    assert data["interaction_type"] == "chat"
    assert data["rating"] is None
    assert data["accuracy_rating"] == "not_rated"


def test_submit_feedback_invalid_rating(client):
    response = client.post("/api/v1/feedback", json={"interaction_type": "metric", "rating": 6})

    assert response.status_code == 422


def test_submit_feedback_with_session(client):
    start_response = client.post(
        "/api/v1/analytics/session/start", json={"task_type": "report_generation"}
    )
    session_id = start_response.json()["id"]

    response = client.post(
        "/api/v1/feedback",
        json={
            "interaction_type": "report",
            "session_id": session_id,
            "rating": 4,
            "feedback_text": "Good but could be better",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["rating"] == 4


def test_get_feedback(client):
    submit_response = client.post(
        "/api/v1/feedback", json={"interaction_type": "chat", "rating": 3}
    )
    feedback_id = submit_response.json()["id"]

    get_response = client.get(f"/api/v1/feedback/{feedback_id}")

    assert get_response.status_code == 200
    data = get_response.json()
    assert data["id"] == feedback_id
    assert data["rating"] == 3


def test_get_feedback_not_found(client):
    response = client.get("/api/v1/feedback/nonexistent")
    assert response.status_code == 404


def test_list_feedback(client):
    client.post("/api/v1/feedback", json={"interaction_type": "report", "rating": 5})
    client.post("/api/v1/feedback", json={"interaction_type": "chat", "rating": 4})

    response = client.get("/api/v1/feedback")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2


def test_submit_feedback_with_accuracy_notes(client):
    response = client.post(
        "/api/v1/feedback",
        json={
            "interaction_type": "metric",
            "accuracy_rating": "partially_correct",
            "accuracy_notes": "The calculation was mostly right but missed one edge case",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["accuracy_rating"] == "partially_correct"
