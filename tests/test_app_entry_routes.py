from fastapi.testclient import TestClient

from app import app


def test_explore_root_and_legacy_redirect_preserve_deep_link_query():
    client = TestClient(app)

    root = client.get("/?pack=earthquakes&event_id=EQ%2B123")
    assert root.status_code == 200
    assert "<title>DaedalMap</title>" in root.text

    legacy = client.get(
        "/explore?pack=earthquakes&event_id=EQ%2B123",
        follow_redirects=False,
    )
    assert legacy.status_code == 308
    assert legacy.headers["location"] == "/?pack=earthquakes&event_id=EQ%2B123"

    assert client.get("/explore", follow_redirects=False).headers["location"] == "/"


def test_ops_and_research_paths_still_serve_the_app_shell():
    client = TestClient(app)
    for path in ("/ops?feed=hurricanes_live&event_id=storm-1", "/research?packs=earthquakes"):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200
        assert "<title>DaedalMap</title>" in response.text
