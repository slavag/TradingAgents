import unittest

from tradingagents.web.app import _render_index_response
from tradingagents.llm_clients.model_catalog import get_web_model_options


class WebModelCatalogTests(unittest.TestCase):
    def test_shared_web_catalog_uses_current_anthropic_models(self):
        values = [value for _, value in get_web_model_options()["anthropic"]]

        self.assertIn("claude-opus-4-6", values)
        self.assertIn("claude-sonnet-4-5", values)
        self.assertIn("claude-haiku-4-5", values)
        self.assertNotIn("claude-sonnet-4-6", values)

    def test_index_injects_shared_model_catalog(self):
        html = _render_index_response().body.decode("utf-8")

        self.assertIn("window.TRADINGAGENTS_MODEL_OPTIONS", html)
        self.assertIn("claude-opus-4-6", html)
        self.assertIn("claude-sonnet-4-5", html)


if __name__ == "__main__":
    unittest.main()
