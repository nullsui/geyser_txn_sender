/// SharedCounter — a shared object that requires consensus path.
/// Used for benchmarking consensus-path (shared object) transaction latency.
module geyser::counter;

/// A shared counter object that anyone can increment.
public struct Counter has key {
    id: UID,
    value: u64,
    owner: address,
}

/// Emitted on every increment for benchmarking.
public struct CounterIncremented has copy, drop {
    counter_id: ID,
    new_value: u64,
    by: address,
}

/// Create a new shared counter starting at 0.
public fun create(ctx: &mut TxContext) {
    let counter = Counter {
        id: object::new(ctx),
        value: 0,
        owner: ctx.sender(),
    };
    transfer::share_object(counter);
}

/// Increment the counter by 1. Requires consensus since Counter is shared.
public fun increment(counter: &mut Counter, ctx: &TxContext) {
    counter.value = counter.value + 1;
    sui::event::emit(CounterIncremented {
        counter_id: object::id(counter),
        new_value: counter.value,
        by: ctx.sender(),
    });
}

/// Read the current value.
public fun value(counter: &Counter): u64 {
    counter.value
}

/// Destroy the counter (only owner).
public fun destroy(counter: Counter, ctx: &TxContext) {
    assert!(counter.owner == ctx.sender(), 0);
    let Counter { id, value: _, owner: _ } = counter;
    object::delete(id);
}
