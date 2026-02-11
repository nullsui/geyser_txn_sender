# Geyser CDN — Global Transaction Relay

Deploy Geyser proxy instances worldwide so transactions reach validators as fast as possible, regardless of where the user is.

## Architecture

```
User (anywhere)
  │
  ▼
Cloudflare Worker (geo-router)
  │
  ├── Americas  → Geyser Proxy (US-West)  → Sui Validators
  ├── Europe    → Geyser Proxy (EU)        → Sui Validators
  └── Asia      → Geyser Proxy (Asia)      → Sui Validators
```

The Cloudflare Worker reads the user's continent from `request.cf.continent` and routes to the nearest Geyser proxy. If the primary backend is down, it automatically fails over to the next closest region.

## Step 1: Find Optimal Locations

Use `geyser-map` to discover where Sui validators are concentrated:

```bash
cargo run --release --bin geyser-map
```

This fetches all active validators, resolves their IPs, geolocates them, and outputs a stake-weighted datacenter heatmap with placement recommendations:

```
Region / Provider                         Validators   Stake%   Recommendation
-------------------------------------------------------------------------------------
Latitude.sh, City of London, United Ki…            7     6.5%   <-- EUROPE instance here
TeraSwitch Networks, Tokyo, Japan                  4     4.0%   <-- ASIA instance here
Webnx Inc, Salt Lake City, United Stat…            2     2.6%   <-- US-WEST instance here
...
```

Add `--probe` to also measure latency from your machine, or `--output json` for scripting.

You can deploy as many or as few regions as you want. The top 3 (US-West, Europe, Asia) cover the majority of validator stake. Some operators may want US-East or Singapore as a 4th/5th region — the map data tells you where the stake is.

## Step 2: Provision VPS Instances

Any cheap VPS works — Geyser is I/O-bound (gRPC connections), not CPU-bound. Recommended:

| Provider | Plan | Cost | Notes |
|----------|------|------|-------|
| Vultr | High Perf 1 vCPU / 1GB | $6/mo | NVMe, 2TB bandwidth, AMD EPYC |
| Hetzner | CX22 | ~$4/mo | Great EU peering |
| OVH | VPS Starter | ~$7/mo | Many validators on OVH |

Pick locations based on your `geyser-map` output. Co-locating on the same provider as validators minimizes hop distance.

## Step 3: Deploy

### Option A: Remote Build (recommended)

Builds directly on the VPS — no cross-compilation needed:

```bash
./deploy/setup-remote-build.sh user@YOUR_VPS_IP region-label
```

### Option B: Deploy All

Copy the template and fill in your private hosts:

```bash
cp deploy/.env.example deploy/.env
# edit deploy/.env
```

Then run:

```bash
./deploy/deploy-all.sh
```

`deploy/.env` is gitignored and should stay local-only.

Both scripts will:
1. Install Rust toolchain and build dependencies
2. Build `geyser-proxy` in release mode
3. Install to `/opt/geyser/` with a `geyser.toml` config
4. Create a systemd service with auto-restart and hardening
5. Start the service

### What gets installed

```
/opt/geyser/
├── geyser-proxy    # Binary
└── geyser.toml     # Configuration

/etc/systemd/system/
└── geyser.service  # Auto-start, auto-restart, hardened
```

## Step 4: Geo-Router (Cloudflare Worker)

The Cloudflare Worker routes users to the nearest backend automatically.

### Setup

1. Add your domain to Cloudflare

2. Create DNS A records for each backend (proxy OFF / gray cloud):
   ```
   be-us    → YOUR_US_VPS_IP
   be-eu    → YOUR_EU_VPS_IP
   be-asia  → YOUR_ASIA_VPS_IP
   ```

3. Create a proxied AAAA record for your public endpoint:
   ```
   txn  → 100::  (proxy ON / orange cloud)
   ```

4. Edit `deploy/cloudflare-worker/wrangler.toml`:
   ```toml
   account_id = "YOUR_ACCOUNT_ID"
   routes = [
     { pattern = "txn.yourdomain.com/*", zone_id = "YOUR_ZONE_ID" }
   ]
   ```

5. Edit backend URLs in `deploy/cloudflare-worker/src/worker.js`

6. Deploy:
   ```bash
   cd deploy/cloudflare-worker
   npm install
   npx wrangler login
   npx wrangler deploy
   ```

### Endpoints

| Path | Description |
|------|-------------|
| `https://txn.yourdomain.com/` | POST JSON-RPC transactions — routed to nearest proxy |
| `https://txn.yourdomain.com/health` | Backend health check (proxied) |
| `https://txn.yourdomain.com/stats` | Latency percentiles (proxied) |
| `https://txn.yourdomain.com/router/status` | Shows your geo info and which backend you'd hit |

### How Routing Works

| Continent | Routed To |
|-----------|-----------|
| North America, South America | US-West backend |
| Europe, Africa | Europe backend |
| Asia, Oceania | Asia backend |

If the primary backend is unreachable, the worker automatically tries the remaining backends in order.

Response headers include:
- `X-Geyser-Region` — which backend handled the request
- `X-Geyser-Backend` — backend URL
- `X-Geyser-Fallback: true` — if a fallback was used

## Management

```bash
# Check health
curl https://txn.yourdomain.com/health

# View logs on a VPS
ssh root@YOUR_VPS journalctl -u geyser -f

# Restart
ssh root@YOUR_VPS systemctl restart geyser

# Redeploy after code changes
./deploy/setup-remote-build.sh root@YOUR_VPS region-label
```

## Configuration

See `config/geyser.toml` for all options. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `engine.mode` | `Hybrid` | `DirectValidator` / `FullnodeProxy` / `Hybrid` |
| `timing.health_ping_interval_secs` | `10` | How often to probe validator latency |
| `quorum.max_amplification` | `8` | Max validators contacted per transaction |
