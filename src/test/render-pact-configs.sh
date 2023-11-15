#!/bin/bash

LOCAL_TOKEN=$(cat ~/.vault-token)
VAULT_TOKEN=${1:-$LOCAL_TOKEN}

vault read -field=basic_auth_read_only_username secret/dsp/pact-broker/users/read-only \
    > /tmp/pact-ro-username.key

vault read -field=basic_auth_read_only_password secret/dsp/pact-broker/users/read-only \
    > /tmp/pact-ro-password.key
