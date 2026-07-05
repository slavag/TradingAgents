import unittest

from tradingagents.llm_clients.model_catalog import get_web_model_options
from tradingagents.web.app import _render_index_response


class WebModelCatalogTests(unittest.TestCase):
    def test_shared_web_catalog_uses_current_openai_models(self):
        values = [value for _, value in get_web_model_options()["openai"]]

        self.assertIn("gpt-5.5", values)
        self.assertIn("gpt-5.4-mini", values)
        self.assertIn("gpt-5.4-nano", values)

    def test_shared_web_catalog_uses_current_anthropic_models(self):
        values = [value for _, value in get_web_model_options()["anthropic"]]

        self.assertIn("claude-fable-5", values)
        self.assertIn("claude-opus-4-8", values)
        self.assertIn("claude-sonnet-5", values)
        self.assertIn("claude-haiku-4-5", values)
        self.assertNotIn("claude-opus-4-7", values)
        self.assertNotIn("claude-sonnet-4-6", values)

    def test_shared_web_catalog_uses_current_google_models(self):
        values = [value for _, value in get_web_model_options()["google"]]

        self.assertIn("gemini-3.5-flash", values)
        self.assertIn("gemini-3.1-flash-lite", values)
        self.assertIn("gemini-3.1-pro-preview", values)
        self.assertNotIn("gemini-3.1-flash-lite-preview", values)

    def test_shared_web_catalog_uses_current_xai_models(self):
        values = [value for _, value in get_web_model_options()["xai"]]

        self.assertIn("grok-4.3", values)
        self.assertIn("grok-build-0.1", values)
        self.assertNotIn("grok-4-1-fast-reasoning", values)

    def test_index_injects_shared_model_catalog(self):
        html = _render_index_response().body.decode("utf-8")

        self.assertIn("window.TRADINGAGENTS_MODEL_OPTIONS", html)
        self.assertIn("gpt-5.5", html)
        self.assertIn("claude-fable-5", html)
        self.assertIn("gemini-3.5-flash", html)
        self.assertIn("grok-build-0.1", html)


if __name__ == "__main__":
    unittest.main()
