/// BenchToken — an owned object for fast-path benchmarking.
/// Mint, burn, and send are all owned-object operations (no consensus needed).
module geyser::token;

/// A benchmark token (owned object, fast path).
public struct BenchToken has key, store {
    id: UID,
    value: u64,
}

/// Emitted when a token is minted.
public struct TokenMinted has copy, drop {
    token_id: ID,
    value: u64,
    to: address,
}

/// Emitted when a token is burned.
public struct TokenBurned has copy, drop {
    token_id: ID,
    value: u64,
}

/// Mint a new BenchToken with the given value (owned by sender).
public fun mint(value: u64, ctx: &mut TxContext) {
    let token = BenchToken {
        id: object::new(ctx),
        value,
    };
    sui::event::emit(TokenMinted {
        token_id: object::id(&token),
        value,
        to: ctx.sender(),
    });
    transfer::transfer(token, ctx.sender());
}

/// Burn a BenchToken.
public fun burn(token: BenchToken) {
    let BenchToken { id, value } = token;
    sui::event::emit(TokenBurned {
        token_id: id.to_inner(),
        value,
    });
    object::delete(id);
}

/// Send a BenchToken to another address.
public fun send(token: BenchToken, recipient: address) {
    transfer::transfer(token, recipient);
}

/// Read the value.
public fun value(token: &BenchToken): u64 {
    token.value
}
