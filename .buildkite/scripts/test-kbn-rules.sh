#!/usr/bin/env bash

# Test elastic.pipes.kbn.rules.get against one Kibana, once per Python version.
#
# By default the target is the Elastic Cloud deployment described by
# deployment.json (see ec/deployments/test-create.yaml). To use another Kibana,
# set KBN_URL, KBN_USERNAME and KBN_PASSWORD.

set -euo pipefail

PYTHON_VERSIONS=(3.8 3.15)
KIBANA_TIMEOUT=1200

if [ -z "${KBN_URL:-}" ]; then
  # credentials go through curl's configuration, never through its arguments
  cloud_get() { printf 'header = "Authorization: ApiKey %s"\n' "$EC_API_KEY" | curl --silent --fail --config - "$@"; }
  kibana_get() { printf 'user = "%s:%s"\n' "$KBN_USERNAME" "$KBN_PASSWORD" | curl --silent --fail --config - "$@"; }

  deployment_id=$(jq -r '.id' deployment.json)
  KBN_USERNAME=$(jq -r '.resources[] | select(.kind == "elasticsearch") | .credentials.username' deployment.json)
  KBN_PASSWORD=$(jq -r '.resources[] | select(.kind == "elasticsearch") | .credentials.password' deployment.json)

  echo "~~~ Wait for Kibana"
  deadline=$((SECONDS + KIBANA_TIMEOUT))
  while :; do
    KBN_URL=$(cloud_get "$EC_API_URL/deployments/$deployment_id" | jq -r '.resources.kibana[0].info.metadata.aliased_url // empty' || true)
    if [ -n "$KBN_URL" ] && kibana_get --output /dev/null "$KBN_URL/api/status"; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Kibana did not become ready within $KIBANA_TIMEOUT seconds" >&2
      exit 1
    fi
    sleep 10
  done
  unset EC_API_KEY
fi
export KBN_URL KBN_USERNAME KBN_PASSWORD

echo "~~~ Seed rules"
./kbn/rules/test-create.yaml

failed=()
for version in "${PYTHON_VERSIONS[@]}"; do
  echo "+++ Kibana rules: Python $version"
  venv=$(mktemp -d)
  if uv python install "$version" \
    && uv venv --python "$version" "$venv" \
    && uv pip install --python "$venv/bin/python" . \
    && PATH="$venv/bin:$PATH" ./kbn/rules/test.sh; then
    echo "Python $version: passed"
  else
    echo "Python $version: failed"
    failed+=("$version")
  fi
  rm -rf "$venv"
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "Kibana rules checks failed on Python: ${failed[*]}" >&2
  exit 1
fi
