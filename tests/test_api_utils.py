import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from core.utils import api_utils


class ApiUtilsTests(unittest.TestCase):
    def setUp(self):
        self.cfg = Mock()
        self.cfg.get.side_effect = lambda key, fallback=None: fallback
        self.auth = patch.object(api_utils, "auth_headers", return_value={
            "Authorization": "Bearer test-token",
        }).start()
        self.urlopen = patch.object(api_utils, "urlopen").start()
        self.addCleanup(patch.stopall)
        self.response = self.urlopen.return_value.__enter__.return_value
        self.response.status = 200
        self.response.read.return_value = b'{"data": [{"source": "SKU"}]}'

    def test_partner_query_encoding_and_response_preservation(self):
        result = api_utils.fetch_partner_mappings(self.cfg, "a&b /c")
        request = self.urlopen.call_args.args[0]
        url = urlsplit(request.full_url)
        self.assertEqual(url.scheme + "://" + url.netloc + url.path,
                         "https://test.bomisco.ai/api/partnermapping/partner-mapping")
        self.assertEqual(parse_qs(url.query), {
            "clientid": ["2"], "key": ["partnermap"], "partnerId": ["a&b /c"],
            "partnersOnly": ["false"], "showAll": ["false"],
        })
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        self.assertEqual(result, {"data": [{"source": "SKU"}]})
        self.assertEqual(self.urlopen.call_args.kwargs, {"timeout": 30})

    def test_configured_base_client_and_explicit_overrides(self):
        self.cfg.get.side_effect = lambda key, fallback=None: {
            "node_service_base_url": "https://example.com/api/", "client_id": "7",
        }.get(key, fallback)
        api_utils.fetch_partner_mappings(self.cfg, "abc")
        self.assertIn("clientid=7", self.urlopen.call_args.args[0].full_url)
        api_utils.fetch_partner_mappings(
            self.cfg, "abc", client_id=9, partners_only=True, show_all=True,
        )
        url = self.urlopen.call_args.args[0].full_url
        self.assertTrue(url.startswith("https://example.com/api/partnermapping/"))
        self.assertEqual(parse_qs(urlsplit(url).query)["clientid"], ["9"])
        self.assertIn("partnersOnly=true&showAll=true", url)

    def test_generic_json_post_and_no_content(self):
        self.response.status = 204
        result = api_utils.call_api(
            self.cfg, "/future-endpoint", method="post", payload={"input": [1, 2]},
            params={"optional": None}, headers={"X-Request-Id": "test"}, timeout=5,
        )
        request = self.urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"input": [1, 2]})
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertNotIn("?", request.full_url)
        self.assertEqual(self.urlopen.call_args.kwargs, {"timeout": 5})
        self.assertIsNone(result)

    def test_invalid_partner_fails_before_network_or_auth(self):
        for partner_id in (None, "", "   "):
            with self.subTest(partner_id=partner_id), self.assertRaises(ValueError):
                api_utils.fetch_partner_mappings(self.cfg, partner_id)
        self.urlopen.assert_not_called()
        self.auth.assert_not_called()

    def test_http_timeout_and_invalid_json_errors_propagate(self):
        for error in (HTTPError("https://example.com", 401, "Unauthorized", {}, None),
                      TimeoutError("timed out")):
            with self.subTest(error=error):
                self.urlopen.side_effect = error
                with self.assertRaises(type(error)):
                    api_utils.fetch_partner_mappings(self.cfg, "abc")
        self.urlopen.side_effect = None
        self.response.read.return_value = b"not JSON"
        with self.assertRaises(json.JSONDecodeError):
            api_utils.fetch_partner_mappings(self.cfg, "abc")


if __name__ == "__main__":
    unittest.main()
