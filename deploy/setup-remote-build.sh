#!/usr/bin/env bash
set -euo pipefail

# Geyser Proxy — Remote Build + Setup (no cross-compilation needed)
# Builds directly on the VPS. Slower first run, but no cross-compile toolchain required.
#
# Usage: ./setup-remote-build.sh <user@host> [region-label]
# Example: ./setup-remote-build.sh root@45.77.x.x us-west

HOST="${1:?Usage: ./setup-remote-build.sh <user@host> [region-label]}"
LABEL="${2:-unknown}"
PRESERVE_REMOTE_CONFIG="${GEYSER_PRESERVE_REMOTE_CONFIG:-1}"
CONFIG_SRC="${GEYSER_CONFIG_SRC:-config/geyser.toml}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── 1. Sync source to VPS ───────────────────────────────────────────────────
echo "==> Syncing source to $HOST..."
rsync -az --delete \
    --exclude target/ \
    --exclude .git/ \
    --exclude node_modules/ \
    "$REPO_ROOT/" "$HOST:/tmp/geyser-build/"

# ── 2. Build + install on VPS ────────────────────────────────────────────────
echo "==> Building on $HOST (this may take a few minutes on first run)..."
ssh "$HOST" bash -s "$LABEL" "$PRESERVE_REMOTE_CONFIG" "$CONFIG_SRC" <<'REMOTE'
set -euo pipefail
LABEL="$1"
PRESERVE_REMOTE_CONFIG="$2"
CONFIG_SRC="$3"

# Install Rust if needed
if ! command -v cargo &>/dev/null; then
    echo "    Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
fi

# Install build deps (Debian/Ubuntu)
if command -v apt-get &>/dev/null; then
    apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev protobuf-compiler >/dev/null 2>&1
fi

# Build
cd /tmp/geyser-build
source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -p geyser-proxy

# Resolve config source:
# 1) deploy/configs/<label>.toml (if present)
# 2) path provided by GEYSER_CONFIG_SRC (default: config/geyser.toml)
LABEL_CONFIG="/tmp/geyser-build/deploy/configs/${LABEL}.toml"
DEFAULT_CONFIG="/tmp/geyser-build/${CONFIG_SRC}"
if [[ -f "$LABEL_CONFIG" ]]; then
    SELECTED_CONFIG="$LABEL_CONFIG"
elif [[ -f "$DEFAULT_CONFIG" ]]; then
    SELECTED_CONFIG="$DEFAULT_CONFIG"
else
    echo "Missing config file. Checked:"
    echo "  $LABEL_CONFIG"
    echo "  $DEFAULT_CONFIG"
    exit 1
fi

# Install (stop service first to avoid "Text file busy")
mkdir -p /opt/geyser
systemctl stop geyser 2>/dev/null || true
cp target/release/geyser-proxy /opt/geyser/geyser-proxy
if [[ "$PRESERVE_REMOTE_CONFIG" == "1" && -f /opt/geyser/geyser.toml ]]; then
    echo "Preserving existing /opt/geyser/geyser.toml (GEYSER_PRESERVE_REMOTE_CONFIG=1)"
else
    cp "$SELECTED_CONFIG" /opt/geyser/geyser.toml
fi
cp deploy/geyser.service /etc/systemd/system/geyser.service

# Create service user if needed
if ! id geyser &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin geyser
fi

chown -R geyser:geyser /opt/geyser
chmod +x /opt/geyser/geyser-proxy

# Raise file limits
if ! grep -q "geyser" /etc/security/limits.conf 2>/dev/null; then
    echo "geyser soft nofile 65535" >> /etc/security/limits.conf
    echo "geyser hard nofile 65535" >> /etc/security/limits.conf
fi

# Enable and start
systemctl daemon-reload
systemctl enable geyser
systemctl restart geyser

sleep 2
if systemctl is-active --quiet geyser; then
    echo "==> Geyser proxy is RUNNING on $LABEL"
else
    echo "==> FAILED to start. Logs:"
    journalctl -u geyser -n 20 --no-pager
    exit 1
fi

# Cleanup build dir
rm -rf /tmp/geyser-build
REMOTE

echo ""
echo "Done! Geyser proxy deployed to $HOST ($LABEL)"
echo "  Health check:  ssh $HOST curl -s http://localhost:8080/health"
echo "  View logs:     ssh $HOST journalctl -u geyser -f"
echo "  Restart:       ssh $HOST systemctl restart geyser"
