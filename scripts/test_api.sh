#!/usr/bin/env bash
set -euo pipefail

OPENSKY_USERNAME="${OPENSKY_USERNAME:-}"
OPENSKY_PASSWORD="${OPENSKY_PASSWORD:-}"
OPENSKY_CLIENT_ID="${OPENSKY_CLIENT_ID:-}"
OPENSKY_CLIENT_SECRET="${OPENSKY_CLIENT_SECRET:-}"
OPENSKY_TOKEN_URL="${OPENSKY_TOKEN_URL:-https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token}"

url="https://opensky-network.org/api/states/all"

if [[ -n "$OPENSKY_CLIENT_ID" && -n "$OPENSKY_CLIENT_SECRET" ]]; then
  echo "Using OAuth2 client credentials (client_id=$OPENSKY_CLIENT_ID)"
  token=$(curl -fsS -X POST "$OPENSKY_TOKEN_URL" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials" \
    -d "client_id=$OPENSKY_CLIENT_ID" \
    -d "client_secret=$OPENSKY_CLIENT_SECRET" \
    | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]\+\)".*/\1/p')
  if [[ -z "$token" ]]; then
    echo "Failed to extract access_token from OAuth response" >&2
    exit 2
  fi
  curl -fsS -H "Authorization: Bearer $token" "$url" | head -c 500 && echo
  exit 0
fi

if [[ -n "$OPENSKY_USERNAME" && -n "$OPENSKY_PASSWORD" ]]; then
  echo "Using Basic Auth user=$OPENSKY_USERNAME"
  curl -fsS -u "$OPENSKY_USERNAME:$OPENSKY_PASSWORD" "$url" | head -c 500 && echo
else
  echo "No credentials set; using anonymous access (may be heavily rate-limited)."
  curl -fsS "$url" | head -c 500 && echo
fi
