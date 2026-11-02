#!/usr/bin/env bash

# Exercise elastic.pipes.kbn.rules.get against the rules seeded by test-create.yaml.
# The target comes from the environment: KBN_URL, KBN_USERNAME, KBN_PASSWORD.

set -euo pipefail
cd "$(dirname "$0")"

TAG='alert.attributes.tags: "pipes-py-integration"'
ENABLED="$TAG and alert.attributes.enabled: true"

EQL_ENABLED_1=pipes-py-eql-enabled-1
EQL_ENABLED_2=pipes-py-eql-enabled-2
KUERY_ENABLED=pipes-py-kuery-enabled
EQL_DISABLED=pipes-py-eql-disabled
KUERY_DISABLED=pipes-py-kuery-disabled
MISSING=pipes-py-missing

# get FILTER RULE_IDS ALLOW_MISSING [PER_PAGE]: the rules as JSON on stdout
get() { ./test.yaml -a "filter=$1" -a "rule_ids=$2" -a "allow_missing=$3" -a "per_page=${4:-100}"; }
# ids ...: the sorted rule IDs retrieved by get, space separated
ids() { get "$@" | jq -r '[.[].rule_id] | sort | join(" ")'; }
sorted() { printf '%s\n' "$@" | sort | paste -sd' ' -; }

check() {
  if [ "$2" = "$3" ]; then
    echo "ok: $1"
  else
    echo "FAIL: $1" >&2
    echo "  expected: $2" >&2
    echo "  actual:   $3" >&2
    exit 1
  fi
}

check_fails() {
  local what=$1
  shift
  if "$@" >/dev/null 2>&1; then
    echo "FAIL: $what unexpectedly succeeded" >&2
    exit 1
  fi
  echo "ok: $what"
}

ALL=$(sorted $EQL_ENABLED_1 $EQL_ENABLED_2 $KUERY_ENABLED $EQL_DISABLED $KUERY_DISABLED)
ENABLED_ONLY=$(sorted $EQL_ENABLED_1 $EQL_ENABLED_2 $KUERY_ENABLED)

# the deployment may hold other rules: every seeded rule is discovered
DISCOVERED=" $(ids None None True) "
for rule_id in $ALL; do
  case "$DISCOVERED" in
    *" $rule_id "*) ;;
    *) echo "FAIL: discovery misses $rule_id" >&2; exit 1 ;;
  esac
done
echo "ok: unfiltered discovery"

check "filter by tag" "$ALL" "$(ids "$TAG" None True)"
check "filter by enabled" "$ENABLED_ONLY" "$(ids "$ENABLED" None True)"
check "pagination across several pages" "$ALL" "$(ids "$TAG" None True 2)"

check "explicit IDs, duplicates and a missing ID ignored" "$EQL_ENABLED_1" \
  "$(ids None "[\"$EQL_ENABLED_1\",\"$EQL_ENABLED_1\",\"$MISSING\"]" True)"
warning=$(get None "[\"$MISSING\"]" True 2>&1 >/dev/null || true)
case "$warning" in
  *"configured Detection Engine rule was not found"*) echo "ok: missing ID is logged" ;;
  *) echo "FAIL: missing ID is not logged" >&2; exit 1 ;;
esac
check_fails "missing ID rejected when not allowed" get None "[\"$EQL_ENABLED_1\",\"$MISSING\"]" False

check "explicit IDs intersected with the filter" "$EQL_ENABLED_1" \
  "$(ids "$ENABLED" "[\"$EQL_ENABLED_1\",\"$EQL_DISABLED\"]" True)"
check_fails "ID outside the filter rejected when not allowed" \
  get "$ENABLED" "[\"$EQL_ENABLED_1\",\"$EQL_DISABLED\"]" False

check_fails "invalid filter rejected by Kibana" get ":" None True

echo "All Kibana rules checks passed"
