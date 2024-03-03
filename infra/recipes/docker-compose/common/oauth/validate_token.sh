#!/bin/bash

# Check if token is provided
if [ -z "$1" ]; then
    echo "Error: Token not provided."
    echo "Usage: $0 <token>"
    exit 1
fi

KEYCLOAK_URL="http://localhost:8085"
REALM="openhouse"
TOKEN="$1"

USERNAME=$(curl -X GET \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "Authorization: Bearer $TOKEN" \
  "$KEYCLOAK_URL/realms/$REALM/protocol/openid-connect/userinfo" | jq -r '.username')

echo "$USERNAME"