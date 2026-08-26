import unittest
from datetime import datetime
from unittest.mock import Mock

from psy29.config import load_dhan_config
from psy29.dhan_rest import DhanRestClient, DhanRestError


class RestAdapterTests(unittest.TestCase):
    def setUp(self):
        self.config = load_dhan_config(
            {
                "DHAN_CLIENT_ID": "1234567890",
                "DHAN_PIN": "123456",
                "DHAN_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
            }
        )
        self.session = Mock()
        self.client = DhanRestClient(
            self.config,
            "jwt-token",
            session=self.session,
        )

    def _response(self, payload, status=200):
        response = Mock(status_code=status)
        response.json.return_value = payload
        return response

    def test_intraday_builds_documented_payload(self):
        self.session.post.return_value = self._response({"open": [], "high": [], "low": [], "close": [], "volume": [], "timestamp": []})

        self.client.intraday(
            "1333",
            1,
            datetime(2026, 8, 27, 9, 15),
            datetime(2026, 8, 27, 10, 15),
        )

        call = self.session.post.call_args
        self.assertEqual(call.args[0], "https://api.dhan.co/v2/charts/intraday")
        self.assertEqual(call.kwargs["json"]["securityId"], "1333")
        self.assertEqual(call.kwargs["json"]["exchangeSegment"], "NSE_EQ")
        self.assertEqual(call.kwargs["json"]["instrument"], "EQUITY")
        self.assertEqual(call.kwargs["json"]["interval"], "1")
        self.assertEqual(call.kwargs["json"]["fromDate"], "2026-08-27 09:15:00")
        self.assertEqual(call.kwargs["json"]["toDate"], "2026-08-27 10:15:00")
        self.assertEqual(call.kwargs["headers"]["access-token"], "jwt-token")

    def test_all_supported_intraday_intervals_are_accepted(self):
        self.session.post.return_value = self._response({})
        for interval in (1, 5, 15, 25, 60):
            self.client.intraday("1333", interval, "2026-08-27 09:15:00", "2026-08-27 10:15:00")
        self.assertEqual(self.session.post.call_count, 5)

    def test_invalid_intraday_interval_is_rejected_before_network_call(self):
        with self.assertRaises(ValueError):
            self.client.intraday("1333", 2, "2026-08-27 09:15:00", "2026-08-27 10:15:00")
        self.session.post.assert_not_called()

    def test_daily_historical_uses_nse_equity(self):
        self.session.post.return_value = self._response({"open": [], "high": [], "low": [], "close": [], "volume": [], "timestamp": []})
        self.client.historical_daily("1333", "2026-08-01", "2026-08-27")
        payload = self.session.post.call_args.kwargs["json"]
        self.assertEqual(payload["exchangeSegment"], "NSE_EQ")
        self.assertEqual(payload["instrument"], "EQUITY")
        self.assertEqual(payload["expiryCode"], 0)
        self.assertFalse(payload["oi"])

    def test_quote_family_sends_client_id(self):
        self.session.post.return_value = self._response({"status": "success", "data": {}})
        for method in (self.client.ltp, self.client.ohlc, self.client.quote):
            method(["1333", "11536"])
            headers = self.session.post.call_args.kwargs["headers"]
            self.assertEqual(headers["client-id"], "1234567890")

    def test_http_error_is_normalized(self):
        self.session.post.return_value = self._response(
            {"errorCode": "DH-401", "errorMessage": "invalid token"},
            status=401,
        )
        with self.assertRaisesRegex(DhanRestError, "invalid token"):
            self.client.ltp(["1333"])

    def test_non_json_response_is_rejected(self):
        response = Mock(status_code=502)
        response.json.side_effect = ValueError
        self.session.post.return_value = response
        with self.assertRaisesRegex(DhanRestError, "non-JSON"):
            self.client.ltp(["1333"])


if __name__ == "__main__":
    unittest.main()
