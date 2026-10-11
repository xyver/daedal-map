"""Browser navigations get HTML error pages; API, MCP, and asset callers keep machine errors."""

import unittest

from fastapi.testclient import TestClient

import app as app_module

BROWSER_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


class ErrorPageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app_module.app, raise_server_exceptions=False)
        self.addCleanup(self.client.close)

    def test_browser_404_is_html(self) -> None:
        response = self.client.get("/no-such-page", headers={"Accept": BROWSER_ACCEPT})
        self.assertEqual(404, response.status_code)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Page not found", response.text)
        self.assertIn('name="robots" content="noindex"', response.text)

    def test_non_browser_404_keeps_default_body(self) -> None:
        response = self.client.get("/no-such-page", headers={"Accept": "*/*"})
        self.assertEqual(404, response.status_code)
        self.assertEqual({"detail": "Not Found"}, response.json())

    def test_api_404_stays_machine_readable_for_browsers(self) -> None:
        response = self.client.get("/api/no-such-route", headers={"Accept": BROWSER_ACCEPT})
        self.assertEqual(404, response.status_code)
        self.assertNotIn("text/html", response.headers["content-type"])

    def test_html_error_page_500(self) -> None:
        response = app_module._html_error_page(500)
        self.assertEqual(500, response.status_code)
        self.assertIn(b"Something went wrong", response.body)


if __name__ == "__main__":
    unittest.main()
