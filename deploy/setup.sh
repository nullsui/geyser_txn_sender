#!/usr/bin/env bash
set -euo pipefail

# Geyser Proxy — Remote VPS Setup Script
# Usage: ./setup.sh <user@host> [region-label]
# Example: ./setup.sh root@45.77.x.x us-west

HOST="${1:?Usage: ./setup.sh <user@host> [region-label]}"
LABEL="${2:-unknown}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="$REPO_ROOT/target/x86_64-unknown-linux-gnu/release/geyser-proxy"
CONFIG="$REPO_ROOT/config/geyser.toml"
SERVICE="$REPO_ROOT/deploy/geyser.service"

# ── 1. Cross-compile for Linux ──────────────────────────────────────────────
echo "==> Building geyser-proxy for linux/amd64..."
if ! rustup target list --installed | grep -q x86_64-unknown-linux-gnu; then
    echo "    Adding cross-compilation target..."
    rustup target add x86_64-unknown-linux-gnu
fi

# Check for linker (brew install filosottile/musl-cross/musl-cross on macOS)
if command -v x86_64-linux-gnu-gcc &>/dev/null; then
    export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
elif command -v x86_64-linux-musl-gcc &>/dev/null; then
    export CC_x86_64_unknown_linux_gnu=x86_64-linux-musl-gcc
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-musl-gcc
else
    echo "ERROR: No cross-linker found. Install one of:"
    echo "  brew install filosottile/musl-cross/musl-cross"
    echo "  -- OR --"
    echo "  Use './deploy/setup-remote-build.sh' to build on the VPS directly"
    exit 1
fi

cd "$REPO_ROOT"
cargo build --release --target x86_64-unknown-linux-gnu -p geyser-proxy

echo "    Binary: $BINARY ($(du -h "$BINARY" | cut -f1))"

# ── 2. Upload to VPS ────────────────────────────────────────────────────────
echo "==> Uploading to $HOST..."
ssh "$HOST" "mkdir -p /opt/geyser"
scp "$BINARY" "$HOST:/opt/geyser/geyser-proxy"
scp "$CONFIG" "$HOST:/opt/geyser/geyser.toml"
scp "$SERVICE" "$HOST:/etc/systemd/system/geyser.service"

# ── 3. Configure & start service ────────────────────────────────────────────
echo "==> Configuring service on $HOST ($LABEL)..."
ssh "$HOST" bash -s "$LABEL" <<'REMOTE'
set -euo pipefail
LABEL="$1"

# Create service user if needed
if ! id geyser &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin geyser
fi

chown -R geyser:geyser /opt/geyser
chmod +x /opt/geyser/geyser-proxy

# Raise system-wide file limits
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
    echo "    Status: $(systemctl is-active geyser)"
    echo "    Health: curl http://localhost:8080/health"
else
    echo "==> FAILED to start. Logs:"
    journalctl -u geyser -n 20 --no-pager
    exit 1
fi
REMOTE

echo ""
echo "Done! Geyser proxy deployed to $HOST ($LABEL)"
echo "  Health check:  ssh $HOST curl -s http://localhost:8080/health"
echo "  View logs:     ssh $HOST journalctl -u geyser -f"
echo "  Restart:       ssh $HOST systemctl restart geyser"
