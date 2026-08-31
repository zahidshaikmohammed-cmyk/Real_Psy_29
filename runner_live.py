from __future__ import annotations

import os

import main

# Prefer an already-issued 24h Dhan token when the Render environment provides
# one. This avoids blocking startup on the TOTP auth endpoint. If absent, the
# production runner retains its existing automatic TOTP generation path.
_env_token = os.getenv("DHAN_ACCESS_TOKEN") or os.getenv("DHAN_ACCESS_TOKEN_VALUE")
if _env_token:
    main.generate_access_token = lambda: (_env_token, None)

import runner  # noqa: E402

app = runner.app
