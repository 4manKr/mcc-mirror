"""
CI-only bootstrap: reads secrets from environment variables (set from GitHub
secrets in the workflow) and writes them into the locations the rest of the
codebase already expects:

    credentials/token.json          <- $GOOGLE_OAUTH_TOKEN_JSON
    credentials/client_secret.json  <- $GOOGLE_OAUTH_CLIENT_SECRET_JSON
    config.yaml notify.smtp.app_password <- $GMAIL_APP_PASSWORD

Also patches config.yaml's Windows-style staging paths to Linux paths so the
pipelines can run on the GitHub Actions Ubuntu runner without any code changes.

Run this once at the start of every CI job.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import yaml


def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val.strip():
        print(f"[inject_secrets] ERROR: env var {name} is missing or empty", file=sys.stderr)
        sys.exit(1)
    return val


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    creds_dir = repo_root / "credentials"
    creds_dir.mkdir(exist_ok=True)

    # 1. Drive OAuth token (from earlier local consent — must include refresh_token)
    token_json = _require_env("GOOGLE_OAUTH_TOKEN_JSON")
    (creds_dir / "token.json").write_text(token_json, encoding="utf-8")
    print(f"[inject_secrets] wrote credentials/token.json ({len(token_json)} chars)")

    # 2. OAuth client secret (the JSON downloaded from Google Cloud Console)
    client_json = _require_env("GOOGLE_OAUTH_CLIENT_SECRET_JSON")
    (creds_dir / "client_secret.json").write_text(client_json, encoding="utf-8")
    print(f"[inject_secrets] wrote credentials/client_secret.json ({len(client_json)} chars)")

    # 3. Patch config.yaml — Linux paths + app password
    cfg_path = repo_root / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    # Linux-compatible staging paths
    cfg.setdefault("paths", {})
    cfg["paths"]["staging"] = "/tmp/mcc-staging"
    cfg["paths"]["admissions_staging"] = "/tmp/mcc-admissions-staging"

    # SMTP app password (Gmail)
    app_password = _require_env("GMAIL_APP_PASSWORD")
    cfg.setdefault("notify", {}).setdefault("smtp", {})
    cfg["notify"]["smtp"]["app_password"] = app_password
    cfg["notify"]["enabled"] = True

    # Optional override of recipient
    notify_to = os.environ.get("NOTIFY_EMAIL_TO", "").strip()
    if notify_to:
        cfg["notify"]["to"] = notify_to

    # WhatsApp via CallMeBot (optional — only enabled if both env vars present)
    wa_phone = os.environ.get("WHATSAPP_PHONE", "").strip()
    wa_key = os.environ.get("WHATSAPP_API_KEY", "").strip()
    cfg["notify"].setdefault("whatsapp", {})
    if wa_phone and wa_key:
        cfg["notify"]["whatsapp"]["enabled"] = True
        cfg["notify"]["whatsapp"]["phone"] = wa_phone
        cfg["notify"]["whatsapp"]["api_key"] = wa_key
        print(f"[inject_secrets] WhatsApp configured (phone {wa_phone[:4]}***, "
              f"key len {len(wa_key)})")
    else:
        cfg["notify"]["whatsapp"]["enabled"] = False
        print("[inject_secrets] WhatsApp NOT configured "
              "(WHATSAPP_PHONE and/or WHATSAPP_API_KEY env vars missing)")

    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False),
                        encoding="utf-8")
    print("[inject_secrets] patched config.yaml (paths -> /tmp, app_password injected)")
    print("[inject_secrets] done.")


if __name__ == "__main__":
    main()
