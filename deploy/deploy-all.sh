#!/usr/bin/env bash
set -euo pipefail

# Deploy geyser-proxy to all 3 VPS instances
# Usage: ./deploy-all.sh
# Optional env file path: GEYSER_DEPLOY_ENV=/path/to/.env ./deploy-all.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${GEYSER_DEPLOY_ENV:-$SCRIPT_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$ENV_FILE"
    set +a
fi

build_target_from_parts() {
    local user_var="$1"
    local host_var="$2"
    local user="${!user_var:-root}"
    local host="${!host_var:-}"
    if [[ -z "$host" ]]; then
        return 0
    fi
    echo "${user}@${host}"
}

US_WEST="${US_WEST:-$(build_target_from_parts US_WEST_USER US_WEST_HOST)}"
ASIA="${ASIA:-$(build_target_from_parts ASIA_USER ASIA_HOST)}"
EUROPE="${EUROPE:-$(build_target_from_parts EUROPE_USER EUROPE_HOST)}"

US_WEST_LABEL="${US_WEST_LABEL:-us-west}"
ASIA_LABEL="${ASIA_LABEL:-asia}"
EUROPE_LABEL="${EUROPE_LABEL:-europe}"

validate_target() {
    local name="$1"
    local value="$2"
    if [[ -z "$value" || "$value" == *"YOUR_"* || "$value" == *"<"* ]]; then
        echo "ERROR: Invalid ${name} target: '${value}'"
        echo "Configure $ENV_FILE (copy from deploy/.env.example) or export ${name}=user@host."
        exit 1
    fi
}

validate_target "US_WEST" "$US_WEST"
validate_target "ASIA" "$ASIA"
validate_target "EUROPE" "$EUROPE"

echo "=== Deploying Geyser Proxy to 3 regions ==="
echo ""
PARALLEL="${GEYSER_PARALLEL:-1}"
entries=(
    "$US_WEST:$US_WEST_LABEL"
    "$ASIA:$ASIA_LABEL"
    "$EUROPE:$EUROPE_LABEL"
)

if [[ "$PARALLEL" == "1" ]]; then
    echo "Mode: parallel (set GEYSER_PARALLEL=0 for sequential)"
    echo ""
    LOG_DIR="${SCRIPT_DIR}/.deploy-logs"
    mkdir -p "$LOG_DIR"

    pids=()
    labels=()
    logs=()

    for entry in "${entries[@]}"; do
        HOST="${entry%%:*}"
        LABEL="${entry##*:}"
        LOG_FILE="${LOG_DIR}/deploy-${LABEL}.log"
        labels+=("$LABEL")
        logs+=("$LOG_FILE")
        echo ">>> Launching $LABEL ($HOST)"
        (
            "$SCRIPT_DIR/setup-remote-build.sh" "$HOST" "$LABEL"
        ) >"$LOG_FILE" 2>&1 &
        pids+=("$!")
    done

    echo ""
    rc=0
    for i in "${!pids[@]}"; do
        LABEL="${labels[$i]}"
        PID="${pids[$i]}"
        if wait "$PID"; then
            echo "[ok] $LABEL deployed"
        else
            echo "[fail] $LABEL failed (see ${logs[$i]})"
            rc=1
        fi
    done

    echo ""
    echo "Logs:"
    for i in "${!labels[@]}"; do
        echo "  ${labels[$i]} -> ${logs[$i]}"
    done

    if [[ "$rc" -ne 0 ]]; then
        exit "$rc"
    fi
else
    for entry in "${entries[@]}"; do
        HOST="${entry%%:*}"
        LABEL="${entry##*:}"
        echo ">>> Deploying to $LABEL ($HOST)"
        "$SCRIPT_DIR/setup-remote-build.sh" "$HOST" "$LABEL"
        echo ""
    done
fi

host_only() {
    echo "${1#*@}"
}

echo ""
echo "=== All deployments complete ==="
echo ""
echo "Health checks:"
echo "  curl http://$(host_only "$US_WEST"):8080/health"
echo "  curl http://$(host_only "$ASIA"):8080/health"
echo "  curl http://$(host_only "$EUROPE"):8080/health"
