# Geyser

A high-performance Sui transaction submission engine that bypasses fullnodes entirely.

Every app, wallet, and SDK on Sui submits transactions the same way: send to a fullnode, which validates, forwards to validators, waits for execution, and relays the response back. Whether you use JSON-RPC or gRPC, the fullnode is still a relay hop between you and the validators that actually execute your transaction.

**Geyser skips the fullnode.** It discovers validator endpoints from the on-chain committee each epoch, monitors their latency in real time with EMA tracking, and races your signed transaction directly to the fastest validators simultaneously. First response wins. No fullnode relay. No single point of failure.

```
Through a fullnode (what everyone does -- protocol doesn't matter):
  Client -> Fullnode -> Validators -> Fullnode -> Client

Geyser direct (skip the fullnode):
  Client -> Race N validators directly -> First response wins

Geyser CDN (datacenter proxy + skip the fullnode):
  Client -> CDN -> Datacenter proxy -> Race N validators (pre-warmed) -> Raw BCS effects
```

## Architecture

```
  How everyone else does it:          How Geyser does it:

  +--------+                          +--------+
  | Client |                          | Client |
  +---+----+                          +---+----+
      |                                   |
  +---v--------+                +---------+---------+
  |  Fullnode  |  JSON-RPC      |         |         |        sui.validator.Validator
  | (relay hop)|  or gRPC       v         v         v        /SubmitTransaction (port 8080)
  +---+--------+          +----+---+ +---+----+ +--+-----+
      |                   |Valid. 1| |Valid. 2| |Valid. N|
  +---v--------+          +--------+ +--------+ +--------+
  | Validators |
  +------------+              or with CDN proxy:

                            +--------+
                            | Client |
                            +---+----+
                                |
                            +---v----+
                            |CDN / LB|  Cloudflare geo-routed
                            +---+----+
                                |
                           +----+----+
                           |  Proxy  |  datacenter colocated
                           | (geyser)|  with validators
                           +----+----+
                                |
                      +---------+---------+
                      |         |         |     pre-warmed gRPC
                      v         v         v     (race N validators)
                 +----+---+ +--+-----+ +-+------+
                 |Valid. 1| |Valid. 2| |Valid. N|
                 +--------+ +--------+ +--------+
```

## How It Works

Sui transactions are idempotent -- the same signed transaction produces the same digest regardless of which node processes it. Geyser exploits this by submitting the same signed TX to multiple validators simultaneously. The first successful response wins.

### The Pipeline

1. **Analyze** -- Classify the transaction as owned-only (fast path, no consensus) or shared (consensus path)
2. **Select Validators** -- Pick the best validators by EMA latency, filtering unhealthy ones (5+ consecutive failures), shuffling the top tier (within 2% of fastest) for load balancing
3. **Submit** -- Race the transaction across N validators concurrently via `sui.validator.Validator/SubmitTransaction` on port 8080, using pre-warmed gRPC channels
4. **Certify** -- With Mysticeti v2, validators typically return `Executed` with effects inline at datacenter distances. For `Submitted` responses, the EffectsCertifier fans out `WaitForEffects` calls and collects from a quorum (2/3+1 stake weight) with Byzantine digest verification
5. **Return** -- First certified response wins

### Validator Selection

Geyser replicates Sui's own validator selection strategy:

```
1. Sort all healthy validators by EMA latency (ascending)
2. Find the fastest latency
3. Select all validators within latency_delta (2%) of the fastest
4. Shuffle that tier randomly (load balancing)
5. Merge with consensus-optimal ordering (seeded by TX digest)
   -- Validators in BOTH latency-top and optimal get priority
6. Submit to top N (amplification factor: 3 for owned, up to max_amplification for shared)
```

### Submission Protocols

Geyser uses two different gRPC protocols depending on the target:

| Target | Protocol | Port | Service |
|--------|----------|------|---------|
| **Validators** | Sui's internal validator protocol | 8080 | `sui.validator.Validator/SubmitTransaction` + `WaitForEffects` |
| **Fullnodes** | gRPC V2 (`TransactionExecutionService`) | 9002 | `ExecuteTransaction` (single call, effects included) |

The validator protocol is a manually-implemented tonic client (`validator_proto.rs`) that speaks Sui's internal `SubmitTransaction` / `WaitForEffects` RPC -- this is NOT the public gRPC V2 `TransactionExecutionService`. The fullnode gRPC V2 path uses the generated proto stubs and is only used as a fallback or for benchmarking comparison.

## CDN Proxy

For even lower latency, Geyser proxies run in datacenters close to validators. The CDN layer is a set of geo-distributed proxy servers behind Cloudflare:

| Region | Location |
|--------|----------|
| SJC | San Jose, CA |
| NRT | Tokyo, JP |
| LHR | London, UK |

Clients send standard JSON-RPC `sui_executeTransactionBlock` requests. The proxy races validators server-side and returns a standard Sui JSON response by default (BCS effects decoded on the server). Server-side timing metadata is included (`geyser.latencyMs`, `geyser.submitMs`, `geyser.effectsMs`).

To skip server-side BCS decoding and decode client-side instead, set `X-Geyser-Fast: true`. The proxy returns raw BCS effects as base64 in a compact JSON wrapper — useful when you don't need the full decoded response or want to minimize server overhead.

At datacenter distances, Mysticeti v2 returns `Executed` with effects inline for ALL transaction types (owned and shared). The proxy-side latency (proxy -> validators -> proxy) can be as low as ~55-120ms; the rest of the client-measured time is the network round trip between the client and the CDN.

### Per-request latency overrides

You can tune direct-submit behavior per request without redeploying by passing a `geyser` override object inside `options` (param index `2`) for `sui_executeTransactionBlock`.

Supported override fields:

| Field | Type | Purpose | Cap |
|------|------|---------|-----|
| `maxAmplification` | number | Max validators raced (shared path) | `128` |
| `ownedAmplification` | number | Max validators raced (owned path) | `128` |
| `effectsFallbackFanout` | number | Validators queried during `WaitForEffects` fallback | `128` |
| `submitTimeoutMs` | number | Per-validator submit timeout | `30000` |
| `effectsTimeoutMs` | number | Effects quorum collection timeout | `30000` |
| `initialRetryDelayMs` | number | Initial retry backoff | `10000` |
| `maxRetryDelayMs` | number | Max retry backoff | `10000` |
| `effectsFallbackDelayMs` | number | Delay before effects fallback fanout | `10000` |
| `maxRetries` | number | Submit retry attempts | `8` |

Headers are also supported (and take precedence): `X-Geyser-Max-Amplification`, `X-Geyser-Owned-Amplification`, `X-Geyser-Effects-Fallback-Fanout`, `X-Geyser-Submit-Timeout-Ms`, `X-Geyser-Effects-Timeout-Ms`, `X-Geyser-Initial-Retry-Delay-Ms`, `X-Geyser-Max-Retry-Delay-Ms`, `X-Geyser-Effects-Fallback-Delay-Ms`, `X-Geyser-Max-Retries`.

Example:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "sui_executeTransactionBlock",
  "params": [
    "<txBytesBase64>",
    ["<signatureBase64>"],
    {
      "showEffects": true,
      "geyser": {
        "maxAmplification": 124,
        "ownedAmplification": 20,
        "effectsFallbackFanout": 120,
        "effectsFallbackDelayMs": 20,
        "submitTimeoutMs": 10000,
        "effectsTimeoutMs": 10000,
        "initialRetryDelayMs": 10,
        "maxRetryDelayMs": 5000,
        "maxRetries": 3
      }
    }
  ]
}
```

## Benchmarking

`geyser-bench` runs real on-chain transactions comparing four submission paths:

| Mode | Description |
|------|-------------|
| **Direct** | Race across N validators directly via `sui.validator.Validator` (port 8080) from the bench client |
| **CDN Fast** | JSON-RPC to Geyser CDN proxy with `X-Geyser-Fast: true` (skip server-side decode), races validators server-side |
| **gRPC** | gRPC V2 `TransactionExecutionService` to fullnode (fullnode baseline) |
| **Vanilla** | JSON-RPC to fullnode (what everyone else does) |

Reports include per-mode P50/P95/P99 latency, submission vs decode breakdown, server-side timing from CDN responses, and per-server race statistics when multiple CDN proxies are configured.

### Workloads

| Workload | Object Type | Path | What It Does |
|----------|-------------|------|--------------|
| **transfer** | Owned (gas coin) | Fast path | SUI self-transfer (1 MIST to self) |
| **counter** | Shared | Consensus | Increment a shared counter object |
| **mint** | Owned (new) | Fast path | Mint a BenchToken via Move call |

### Running

```bash
# Transfer only (no contract deployment needed)
cargo run --release --bin geyser-bench -- \
  --grpc-endpoints <GRPC_FULLNODE_URL> \
  --workload transfer \
  --iterations 20

# All workloads with CDN (per-server race comparison)
cargo run --release --bin geyser-bench -- \
  --grpc-endpoints <GRPC_FULLNODE_URL> \
  --cdn-url "sjc=<SJC_PROXY_URL>,nrt=<NRT_PROXY_URL>,lhr=<LHR_PROXY_URL>" \
  --package-id <DEPLOYED_PACKAGE_ID> \
  --counter-id <SHARED_COUNTER_OBJECT_ID> \
  --workload all \
  --mode all \
  --iterations 15 \
  --warmup 3
```

### Results

Mainnet benchmark from Los Angeles, CA on **February 11, 2026** (15 iterations, 3 warmup, epoch 1035).  
Run used:

- Optimized direct path: `--submit-all --owned-amplification 20 --effects-fallback-fanout 120 --effects-fallback-delay-ms 20`
- Optimized CDN path: per-request proxy overrides via `X-Geyser-*` headers (same amplification/fanout tuning)
- Baselines: gRPC fullnode and vanilla JSON-RPC fullnode

```
Test                           N    SubP50     DecP50      Min      P50      P95      P99      Max      Avg
----------------------------------------------------------------------------------------------------------------------
direct-transfer               15    516.4ms     0.043ms    22.0ms   516.4ms   995.8ms   995.8ms   995.8ms   533.8ms
cdn-fast-transfer             15    515.3ms     0.063ms   436.5ms   515.3ms  1086.5ms  1086.5ms  1086.5ms   592.7ms
                       └─  Network: 27ms | Submit: 103ms | Effects: 360ms
                       ├─  sjc   wins:14/15 p50:515ms avg:593ms (sub:103 eff:360)
                       ├─  lhr   wins: 1/15 p50:929ms avg:929ms (sub:114 eff:630)
grpc-transfer                 15    719.4ms     0.046ms   629.8ms   719.4ms  1271.8ms  1271.8ms  1271.8ms   811.5ms
vanilla-transfer              15    800.3ms     0.001ms   658.4ms   800.3ms  1234.9ms  1234.9ms  1234.9ms   832.0ms

Comparison           Direct P50   CDN-Fast P50     gRPC P50  Vanilla P50   Direct vs Van.   CDN-Fast vs Van.     gRPC vs Van.
-----------------------------------------------------------------------------------------------------------------------------
transfer                516.4ms        515.3ms      719.4ms       800.3ms           +35.5%             +35.6%           +10.1%
```

**Key takeaways:**

- **Optimized CDN and optimized direct are effectively tied at P50**: `515.3ms` (CDN) vs `516.4ms` (direct).
- **Both optimized paths are much faster than vanilla**: `+35.6%` (CDN) and `+35.5%` (direct) vs vanilla P50.
- **gRPC beats vanilla but trails optimized paths**: `719.4ms` P50 (`+10.1%` vs vanilla).
- **Tail latency still matters**: CDN P95 is `1086.5ms`, so network/validator variance remains the limiting factor.

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-endpoints` | -- | gRPC V2 fullnode endpoints (comma-separated) |
| `--rpc-url` | `https://fullnode.mainnet.sui.io:443` | JSON-RPC for PTB building + vanilla baseline |
| `--cdn-url` | -- | CDN proxy URLs, supports labels: `"sjc=http://...,nrt=http://..."` |
| `--package-id` | -- | Deployed geyser Move package |
| `--counter-id` | -- | Shared counter object ID |
| `--keystore` | `~/.sui/sui_config/sui.keystore` | Sui keystore path |
| `--sender` | First ed25519 key | Sender address |
| `--iterations` | 20 | Measured iterations per workload |
| `--warmup` | 3 | Warmup iterations (discarded) |
| `--delay-ms` | 1000 | Delay between transactions (ms) |
| `--workload` | `all` | `transfer`, `counter`, `mint`, or `all` |
| `--mode` | `all` | `direct`, `grpc`, `vanilla`, `cdn-fast`, `cdn-per-server`, or `all` |
| `--max-validators` | 8 | How many validators to race for direct path |
| `--submit-all` | `false` | Race all validators in the current committee |
| `--owned-amplification` | 5 | Owned-object fanout in direct mode |
| `--effects-fallback-delay-ms` | 20 | Delay before effects fallback fan-out |
| `--effects-fallback-fanout` | 8 | Additional validators queried for `WaitForEffects` |
| `--output` | `table` | `table` or `json` |

## Direct Submit CLI

`geyser-submit` submits an already-built signed transaction directly to validators from raw `tx_bytes` and signatures.

### Examples

```bash
# Submit with explicit tx bytes + signature (base64)
cargo run --release -p geyser-bench --bin geyser-submit -- \
  --rpc-url https://fullnode.mainnet.sui.io:443 \
  --tx-bytes-b64 "<TX_BYTES_B64>" \
  --signature-b64 "<SIGNATURE_B64>"

# Submit tx bytes and auto-sign from keystore (first ed25519 key by default)
cargo run --release -p geyser-bench --bin geyser-submit -- \
  --tx-bytes-hex "<TX_BYTES_HEX>" \
  --keystore ~/.sui/sui_config/sui.keystore

# Push amplification to all validators in current committee
cargo run --release -p geyser-bench --bin geyser-submit -- \
  --tx-bytes-b64 "<TX_BYTES_B64>" \
  --signature-b64 "<SIGNATURE_B64>" \
  --submit-all

# Return after SubmitTransaction acceptance only (skip quorum effects cert)
cargo run --release -p geyser-bench --bin geyser-submit -- \
  --tx-bytes-b64 "<TX_BYTES_B64>" \
  --signature-b64 "<SIGNATURE_B64>" \
  --submit-only
```

### Key Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--tx-bytes-b64` / `--tx-bytes-hex` / `--tx-bytes-file` | -- | Input transaction bytes (choose one) |
| `--signature-b64` / `--signature-hex` | -- | One or more signatures (repeat for multisig) |
| `--keystore` | `~/.sui/sui_config/sui.keystore` | Used for signing when no signatures are provided |
| `--amplification` | auto | Force a fixed validator fan-out |
| `--submit-all` | `false` | Race all validators in committee |
| `--owned-amplification` | `5` | Owned-object fanout when auto amplification is used |
| `--max-validators` | `8` | Max fan-out used by auto mode |
| `--effects-fallback-fanout` | `8` | Additional validators queried during `WaitForEffects` fallback |
| `--submit-only` | `false` | Skip effects quorum cert and return after submit acceptance |
| `--output` | `pretty` | `pretty` or `json` |

## Move Contracts

Two benchmark contracts deployed to Sui mainnet.

### `counter.move` -- Shared Object (Consensus Path)

A shared counter that anyone can increment. Because the `Counter` object is shared via `transfer::share_object`, every `increment` call goes through Sui's consensus protocol.

```move
public struct Counter has key {
    id: UID,
    value: u64,
    owner: address,
}

public fun create(ctx: &mut TxContext);           // Creates and shares
public fun increment(counter: &mut Counter, ...); // Consensus path
public fun value(counter: &Counter): u64;
public fun destroy(counter: Counter, ...);        // Owner only
```

### `token.move` -- Owned Object (Fast Path)

A simple token that stays owned by the sender. All operations use only owned objects, taking Sui's fast path.

```move
public struct BenchToken has key, store {
    id: UID,
    value: u64,
}

public fun mint(value: u64, ctx: &mut TxContext);     // Fast path
public fun burn(token: BenchToken);                    // Fast path
public fun send(token: BenchToken, recipient: address); // Fast path
public fun value(token: &BenchToken): u64;
```

### Deploying

```bash
cd contracts/geyser
sui client publish --gas-budget 100000000
# Note the package ID from output

sui client call \
  --package <PACKAGE_ID> \
  --module counter \
  --function create \
  --gas-budget 10000000
# Note the shared counter object ID from output
```

Pass `--package-id` and `--counter-id` to `geyser-bench` for counter and mint workloads.

## Configuration

Geyser loads config from `GEYSER_CONFIG` env var or `geyser.toml` in the working directory.

```toml
[server]
port = 8080

[engine]
mode = "Hybrid"                      # DirectValidator | FullnodeProxy | Hybrid
validator_endpoints = []             # Auto-discovered from committee if empty
fullnode_endpoints = [
    "https://fullnode.mainnet.sui.io:443",
]

[gas_pool]
pool_size = 10                       # Pre-split gas coins for parallel TXs
min_balance_mist = 100_000_000       # 0.1 SUI minimum per coin

[timing]
health_ping_interval_secs = 10       # Validator health check interval
submit_timeout_secs = 5              # Per-submission timeout
effects_timeout_secs = 10            # Effects collection timeout
initial_retry_delay_ms = 10          # First retry delay (Sui default: 100ms)
max_retry_delay_secs = 5             # Exponential backoff cap
effects_fallback_delay_ms = 20       # Fan-out delay for effects (Sui default: 200ms)

[quorum]
max_amplification = 8                # Max concurrent validator submissions
owned_amplification = 5              # Owned-object validator fanout
effects_fallback_fanout = 8          # Extra validators for effects fallback
latency_delta = 0.02                 # 2% -- validator selection tier threshold
read_mask = ["effects"]              # Fields to request in gRPC response
```

### Aggressive Timing

Geyser uses more aggressive timing defaults than Sui's standard client:

| Parameter | Geyser | Sui Default | Why |
|-----------|--------|-------------|-----|
| Initial retry delay | 10ms | 100ms | Faster recovery from transient failures |
| Effects fallback delay | 20ms | 200ms | 10x earlier fan-out to additional validators |
| Submit timeout | 5s | 10s | Fail fast on unresponsive validators |
| Health ping interval | 10s | -- | Continuous latency tracking for selection |

## Building

```bash
# Prerequisites
brew install protobuf    # protoc for gRPC code generation

# Build all crates
cargo build --release

# Run tests
cargo test
cd contracts/geyser && sui move test
```

## Key Components

**ValidatorClient** (`validator_proto.rs`) -- Hand-rolled tonic client for Sui's internal `sui.validator.Validator` gRPC service on port 8080. Implements `SubmitTransaction` (send TX + signatures, get back status + optional effects) and `WaitForEffects` (poll for effects by digest + consensus position). This is NOT the public gRPC V2 API.

**FullnodeRacer** -- gRPC V2 race engine for fullnodes. Maintains cached `tonic::Channel` connections per endpoint. Races `ExecuteTransaction` requests across configured gRPC endpoints, returns the first successful response. Used for fullnode fallback path and benchmarking.

**TxSubmitter** -- Parallel validator submission with configurable amplification (3 for owned, up to `max_amplification` for shared). Merges latency-ranked and consensus-optimal validator orderings. Implements retry with exponential backoff (10ms initial, 5s max). Aborts early if f+1 validators return non-retriable errors.

**EffectsCertifier** -- Collects transaction effects from a quorum of validators. For owned-object transactions, effects come back inline. For shared-object transactions, the certifier queries the initial validator immediately, then after a 20ms fallback delay fans out to additional validators. Collects until 2/3+1 stake weight is reached, with Byzantine digest matching.

**ValidatorMonitor** -- Per-validator health state across 5 operation types (Submit, Effects, FastPath, Consensus, Ping). Uses EMA (alpha=0.3) for latency tracking and a 100-entry sliding window for percentile calculation. Background task pings all validators every 10s with 0-5s random jitter.

**QuorumDriver** -- Orchestrator: analyze -> select validators -> submit -> certify -> return. The full production pipeline.

**CommitteeManager** -- Fetches the active validator set from `suix_getLatestSuiSystemState`. Parses protocol public keys, stake weights, and network addresses (`/dns/.../tcp/8080/http` multiaddr format). Auto-refreshes on epoch transitions.

## License

MIT
