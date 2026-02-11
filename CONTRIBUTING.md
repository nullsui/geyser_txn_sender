# Contributing

## Setup

1. Install Rust stable and `protobuf` (`protoc`).
2. Clone the repo.
3. Build the workspace:

```bash
cargo build
```

## Development Flow

1. Create a branch from `main`.
2. Make focused changes with tests where practical.
3. Run local checks before opening a PR:

```bash
cargo fmt --all
cargo check --workspace
cargo test
```

4. Open a pull request with:
- Problem statement
- Approach and tradeoffs
- Validation performed (benchmarks/tests)

## Benchmarks

When changing submission logic, include before/after benchmark output from `geyser-bench` and document command-line flags used.

## Security

Do not commit secrets, private keys, passwords, or production endpoints.
Use `deploy/.env` for local deploy credentials; this file is gitignored.
