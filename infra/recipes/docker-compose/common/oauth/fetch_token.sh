#!/bin/bash

# Check if username is provided
if [ -z "$1" ]; then
    echo "Error: Username not provided."
    echo "Usage: $0 <username>"
    exit 1
fi

KEYCLOAK_URL="http://localhost:8085"
REALM="openhouse"
CLIENT_ID="openhouse-services"
USERNAME="$1"
PASSWORD="$1"

TOKEN=$(curl -X POST \
  -d "client_id=$CLIENT_ID" \
  -d "username=$USERNAME" \
  -d "password=$PASSWORD" \
  -d "grant_type=password" \
  "$KEYCLOAK_URL/realms/$REALM/protocol/openid-connect/token" | jq -r '.access_token')

echo "$TOKEN"
