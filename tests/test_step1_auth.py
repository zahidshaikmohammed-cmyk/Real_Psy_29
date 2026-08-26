import unittest
from unittest.mock import Mock

from psy29.config import ConfigurationError, load_dhan_config
from psy29.dhan_auth import DhanAuthenticationError, generate_access_token


class ConfigTests(unittest.TestCase):
    def test_loads_required_environment(self):
        config = load_dhan_config(
            {
                "DHAN_CLIENT_ID": "1234567890",
                "DHAN_PIN": "123456",
                "DHAN_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
            }
        )
        self.assertEqual(config.client_id, "1234567890")
        self.assertEqual(config.pin, "123456")
        self.assertEqual(config.totp_secret, "JBSWY3DPEHPK3PXP")

    def test_missing_environment_is_rejected(self):
        with self.assertRaises(ConfigurationError):
            load_dhan_config({})

    def test_non_numeric_client_id_is_rejected(self):
        with self.assertRaises(ConfigurationError):
            load_dhan_config(
                {
                    "DHAN_CLIENT_ID": "client",
                    "DHAN_PIN": "123456",
                    "DHAN_TOTP_SECRET": "secret",
                }
            )


class AuthTests(unittest.TestCase):
    def test_successful_token_generation(self):
        session = Mock()
        response = Mock(status_code=200)
        response.json.return_value = {
            "accessToken": "jwt-token",
            "expiryTime": "2026-08-27T09:15:00+05:30",
        }
        session.post.return_value = response

        config = load_dhan_config(
            {
                "DHAN_CLIENT_ID": "1234567890",
                "DHAN_PIN": "123456",
                "DHAN_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
            }
        )
        token = generate_access_token(config, session=session)

        self.assertEqual(token.value, "jwt-token")
        self.assertIsNotNone(token.expiry_time)
        session.post.assert_called_once()
        params = session.post.call_args.kwargs["params"]
        self.assertEqual(params["dhanClientId"], "1234567890")
        self.assertEqual(params["pin"], "123456")
        self.assertTrue(params["totp"].isdigit())
        self.assertEqual(len(params["totp"]), 6)

    def test_http_auth_failure_does_not_leak_credentials(self):
        session = Mock()
        response = Mock(status_code=401)
        response.json.return_value = {"errorMessage": "invalid credentials"}
        session.post.return_value = response

        config = load_dhan_config(
            {
                "DHAN_CLIENT_ID": "1234567890",
                "DHAN_PIN": "123456",
                "DHAN_TOTP_SECRET": "JBSWY3DPEHPK3PXP",
            }
        )

        with self.assertRaisesRegex(DhanAuthenticationError, "invalid credentials"):
            generate_access_token(config, session=session)


if __name__ == "__main__":
    unittest.main()
