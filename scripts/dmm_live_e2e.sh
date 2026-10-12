#!/usr/bin/env bash
# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Live Entropy Data CE (Data Mesh Manager) end-to-end check for forge-cli's
# ODCS/ODPS export path. Opt-in and manual — never run by CI.
#
# What it verifies, against a real Entropy CE instance:
#   1. `fluid publish --catalog datamesh-manager`  (CatalogTarget path)
#   2. `fluid dmm publish --with-contract`          (direct DMM provider path)
#   ...each at the default ODCS emit (v3.1.0) and with ODCS_API_VERSION=v3.2.0,
#   asserting via the DMM REST API that the product + one contract per port
#   landed, and that each contract carries the expected apiVersion.
#
# Last verified green: Entropy CE 2.1.x, both paths, v3.1.0 and v3.2.0.
#
# Prereqs: docker + compose plugin, python3, git, curl, and forge-cli installed
# from your checkout (`pip install -e .`).
#
# Usage:
#   scripts/dmm_live_e2e.sh up        # start Entropy CE (compose), wait until healthy
#   scripts/dmm_live_e2e.sh run       # needs DMM_API_KEY exported (see bootstrap below)
#   scripts/dmm_live_e2e.sh down      # stop + remove the stack (keeps images)
#
# Environment:
#   DMM_API_KEY        required for `run` — an organisation API key with write access
#   DMM_API_URL        default http://localhost:8081
#   CE_DIR             where the entropy-data-ce compose repo is cloned
#   FLUID_CHECKOUT     forge-cli checkout (default: git toplevel of this script)
#   DMM_PRODUCT_SPEC   if set, passed as --data-product-spec on path 2
#                      (default: unset, i.e. exercise fluid's own default — ODPS)
#
# Side effects: none on your home directory. The fluid commands run with HOME
# pointed at a private temp dir that holds the generated
# ~/.fluid/config.yaml, so your real ~/.fluid/config.yaml / ~/.fluidrc.yaml
# are never read or modified. The API key is passed to curl through a 0600
# header file, never on the command line.
set -euo pipefail

CE_REPO=https://github.com/entropy-data/entropy-data-ce.git
CE_DIR="${CE_DIR:-$HOME/.cache/entropy-data-ce}"
DMM_API_URL="${DMM_API_URL:-http://localhost:8081}"
FLUID_CHECKOUT="${FLUID_CHECKOUT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || pwd)}"
FIXTURE="$FLUID_CHECKOUT/tests/fixtures/fluid/contract-multi-expose.fluid.yaml"
PRODUCT_ID="commerce.orders-product"
PORTS=(daily_orders hourly_orders)
# Private, unpredictable work dir (mktemp -d creates it 0700 and fails if it
# already exists — no symlink games in a shared /tmp).
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dmm_live_e2e.XXXXXXXX")
trap 'rm -rf "$WORK"' EXIT
# Isolated HOME for the fluid commands; the generated config lives here.
FLUID_HOME="$WORK/home"
FLUID_CFG="$FLUID_HOME/.fluid/config.yaml"
HDR_FILE="$WORK/api-key.header"
# Keep `pip install --user` installs importable under the isolated HOME.
PY_USER_BASE="${PYTHONUSERBASE:-$(python3 -m site --user-base 2>/dev/null || true)}"

log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
ok()   { printf '   \033[1;32m✓\033[0m %s\n' "$*"; }
fail() { printf '   \033[1;31m✗ %s\033[0m\n' "$*"; FAILED=1; }
FAILED=0

write_api_key_header() { # header file read by `curl -H @file` (0600, printf is a builtin)
  : "${DMM_API_KEY:?export DMM_API_KEY first}"
  (umask 077; printf 'x-api-key: %s\n' "$DMM_API_KEY" > "$HDR_FILE")
}

api() { # api <METHOD> <path>  → prints HTTP code; body in $WORK/body.json
  local m=$1 p=$2
  [ -f "$HDR_FILE" ] || write_api_key_header
  curl -sS -o "$WORK/body.json" -w '%{http_code}' -X "$m" \
       -H @"$HDR_FILE" \
       -H 'Accept: application/json' "$DMM_API_URL$p"
}

fluid_isolated() { # run fluid with HOME pointed at the private config dir
  HOME="$FLUID_HOME" PYTHONUSERBASE="$PY_USER_BASE" fluid "$@"
}

cmd_up() {
  log "Cloning/updating Entropy CE compose repo → $CE_DIR"
  if [ -d "$CE_DIR/.git" ]; then git -C "$CE_DIR" pull -q; else git clone -q "$CE_REPO" "$CE_DIR"; fi
  log "docker compose up (app :8081, pgvector pg16, mailhog :8025)"
  docker compose -f "$CE_DIR/docker-compose.yaml" up -d
  log "Waiting for the app (Spring Boot + DB init can take ~60–120s)"
  for i in $(seq 1 60); do
    if curl -sS -o /dev/null -w '%{http_code}' "$DMM_API_URL/" 2>/dev/null | grep -qE '^(200|302|401)$'; then ok "up after $((i*3))s"; break; fi
    sleep 3; [ "$i" = 60 ] && { fail "app not reachable at $DMM_API_URL"; exit 1; }
  done
  cat <<EOF

Bootstrap (one-time, via the UI — CE does not document a headless path):
  1. Open $DMM_API_URL and complete first-run sign-up (creates the organization + admin user).
     Mail goes to mailhog at http://localhost:8025 if a verification link is required.
  2. Organization settings → API keys → create a key with write access.
  3. export DMM_API_KEY='<the key>'
  4. scripts/dmm_live_e2e.sh run
EOF
}

write_fluid_config() {
  mkdir -p "$(dirname "$FLUID_CFG")"
  (umask 077; cat > "$FLUID_CFG") <<EOF
catalogs:
  datamesh-manager:
    endpoint: $DMM_API_URL
    auth:
      type: api_key
      api_key: \${DMM_API_KEY}
    enabled: true
EOF
  ok "wrote $FLUID_CFG under the isolated HOME (api_key via \${DMM_API_KEY} expansion)"
}

assert_landed() { # assert_landed <label> <expected_api_version>
  local label=$1 want=$2 code
  code=$(api GET "/api/dataproducts/$PRODUCT_ID")
  [ "$code" = 200 ] && ok "$label: product $PRODUCT_ID → HTTP 200" || fail "$label: product GET → HTTP $code: $(head -c 300 "$WORK/body.json")"
  for port in "${PORTS[@]}"; do
    local cid="$PRODUCT_ID.$port"
    code=$(api GET "/api/datacontracts/$cid")
    if [ "$code" != 200 ]; then fail "$label: contract $cid GET → HTTP $code: $(head -c 300 "$WORK/body.json")"; continue; fi
    local got; got=$(python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(d.get('apiVersion') or (d.get('spec') or {}).get('apiVersion') or '?')" "$WORK/body.json")
    [ "$got" = "$want" ] && ok "$label: contract $cid → HTTP 200, apiVersion=$got" || fail "$label: contract $cid apiVersion=$got (wanted $want)"
  done
}

cleanup_product() {
  api DELETE "/api/dataproducts/$PRODUCT_ID" >/dev/null 2>&1 || true
  for port in "${PORTS[@]}"; do api DELETE "/api/datacontracts/$PRODUCT_ID.$port" >/dev/null 2>&1 || true; done
}

cmd_run() {
  : "${DMM_API_KEY:?export DMM_API_KEY first (see 'scripts/dmm_live_e2e.sh up' output)}"
  [ -f "$FIXTURE" ] || { echo "fixture not found: $FIXTURE (set FLUID_CHECKOUT)"; exit 1; }
  log "Sanity: DMM API reachable with this key"
  code=$(api GET /api/dataproducts); [ "$code" = 200 ] && ok "GET /api/dataproducts → 200" || { fail "GET /api/dataproducts → $code: $(head -c 300 "$WORK/body.json")"; exit 1; }
  log "fluid version"; fluid_isolated --version | head -1
  write_fluid_config

  # Path 2 exercises fluid's own default spec (ODPS) unless DMM_PRODUCT_SPEC pins one.
  SPEC_FLAG=(); [ -n "${DMM_PRODUCT_SPEC:-}" ] && SPEC_FLAG=(--data-product-spec "$DMM_PRODUCT_SPEC")

  for ver in "" v3.2.0; do
    label="ODCS ${ver:-v3.1.0 (default)}"; want="${ver:-v3.1.0}"
    export ODCS_API_VERSION="$ver"; [ -z "$ver" ] && unset ODCS_API_VERSION

    log "[$label] path 1: fluid publish --catalog datamesh-manager"
    cleanup_product
    if fluid_isolated publish "$FIXTURE" --catalog datamesh-manager --format json | tee "$WORK/publish.json" \
         | python3 -c "import json,sys;r=json.load(sys.stdin);sys.exit(0 if all(x['success'] for x in r) else 1)"; then
      ok "$label: fluid publish reported success"; else fail "$label: fluid publish reported failure — see $WORK/publish.json"; fi
    assert_landed "$label / catalog" "$want"

    log "[$label] path 2: fluid dmm publish --with-contract (strict, fail-on-error)"
    cleanup_product
    if fluid_isolated dmm publish "$FIXTURE" --api-url "$DMM_API_URL" --with-contract --contract-format odcs \
         --validate-generated-contracts --validation-mode strict --fail-on-contract-error "${SPEC_FLAG[@]}"; then
      ok "$label: fluid dmm publish exit 0"; else fail "$label: fluid dmm publish non-zero exit"; fi
    assert_landed "$label / dmm" "$want"
  done

  log "Result"
  if [ "$FAILED" = 0 ]; then
    echo "ALL CHECKS PASSED — Entropy CE accepted both paths at v3.1.0 and v3.2.0."
  else
    echo "SOME CHECKS FAILED (see ✗ lines)."
    # Keep the response bodies for debugging, but never the API key.
    rm -f "$HDR_FILE"; trap - EXIT; echo "Bodies kept in $WORK"; exit 1
  fi
}

cmd_down() { docker compose -f "$CE_DIR/docker-compose.yaml" down; }

case "${1:-}" in up) cmd_up ;; run) cmd_run ;; down) cmd_down ;; *) sed -n '16,48p' "$0"; exit 2 ;; esac
