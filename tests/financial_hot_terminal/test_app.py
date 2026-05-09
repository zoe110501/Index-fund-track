from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from financial_hot_terminal.app import create_app
from financial_hot_terminal.database import initialize_database, open_connection
from financial_hot_terminal.repository import HotspotRepository
from financial_hot_terminal.seed import seed_demo_data


SHANGHAI = timezone(timedelta(hours=8))


def build_client(tmp_path):
    db_path = tmp_path / "hotspots.sqlite"
    connection = open_connection(db_path)
    initialize_database(connection)
    repository = HotspotRepository(connection)
    seed_demo_data(repository, now=datetime(2026, 5, 9, 12, 0, tzinfo=SHANGHAI))
    app = create_app(repository=repository)
    return TestClient(app)


def test_items_api_filters_selected_and_includes_source_urls(tmp_path):
    client = build_client(tmp_path)

    response = client.get("/api/items?status=selected")

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert all(item["source_urls"] for item in payload["items"])
    assert all(item["review_status"] in {"selected", "approved"} for item in payload["items"])


def test_social_only_candidate_is_not_in_selected_filter(tmp_path):
    client = build_client(tmp_path)

    selected = client.get("/api/items?status=selected").json()["items"]
    candidates = client.get("/api/items?status=candidate").json()["items"]

    assert all("未证实传闻" not in item["title"] for item in selected)
    assert any("未证实传闻" in item["title"] for item in candidates)


def test_daily_api_contains_disclaimer_and_sections(tmp_path):
    client = build_client(tmp_path)

    response = client.get("/api/daily?date=2026-05-09")

    assert response.status_code == 200
    payload = response.json()
    assert payload["date"] == "2026-05-09"
    assert "非投资建议" in payload["disclaimer"]
    assert payload["sections"]


def test_daily_page_renders_latest_report(tmp_path):
    client = build_client(tmp_path)

    response = client.get("/daily")

    assert response.status_code == 200
    assert "金融热点日报" in response.text


def test_review_api_approves_candidate(tmp_path):
    client = build_client(tmp_path)
    candidate = client.get("/api/items?status=candidate").json()["items"][0]

    response = client.post(f"/api/review/{candidate['id']}", json={"status": "approved", "reviewer": "tester"})

    assert response.status_code == 200
    assert response.json()["review_status"] == "approved"


def test_source_status_api_reports_fetch_audits(tmp_path):
    client = build_client(tmp_path)

    response = client.get("/api/sources/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sources"]
    assert all("latest_status" in source for source in payload["sources"])


def test_favicon_request_does_not_log_browser_error(tmp_path):
    client = build_client(tmp_path)

    response = client.get("/favicon.ico")

    assert response.status_code == 204
