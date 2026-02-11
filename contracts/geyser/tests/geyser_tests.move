#[test_only]
module geyser::geyser_tests;

use sui::test_scenario;
use geyser::counter::{Self, Counter};
use geyser::token::{Self, BenchToken};

#[test]
fun test_create_and_increment() {
    let alice = @0xA;
    let mut scenario = test_scenario::begin(alice);

    // Create counter
    counter::create(scenario.ctx());
    scenario.next_tx(alice);

    // Increment it
    let mut ctr = scenario.take_shared<Counter>();
    assert!(counter::value(&ctr) == 0);
    counter::increment(&mut ctr, scenario.ctx());
    assert!(counter::value(&ctr) == 1);
    counter::increment(&mut ctr, scenario.ctx());
    assert!(counter::value(&ctr) == 2);
    test_scenario::return_shared(ctr);

    scenario.end();
}

#[test]
fun test_counter_destroy() {
    let alice = @0xA;
    let mut scenario = test_scenario::begin(alice);

    counter::create(scenario.ctx());
    scenario.next_tx(alice);

    let ctr = scenario.take_shared<Counter>();
    counter::destroy(ctr, scenario.ctx());

    scenario.end();
}

#[test]
fun test_mint_and_burn() {
    let alice = @0xA;
    let mut scenario = test_scenario::begin(alice);

    // Mint
    token::mint(1000, scenario.ctx());
    scenario.next_tx(alice);

    // Burn
    let tok = scenario.take_from_sender<BenchToken>();
    assert!(token::value(&tok) == 1000);
    token::burn(tok);

    scenario.end();
}

#[test]
fun test_mint_and_send() {
    let alice = @0xA;
    let bob = @0xB;
    let mut scenario = test_scenario::begin(alice);

    // Alice mints
    token::mint(500, scenario.ctx());
    scenario.next_tx(alice);

    // Alice sends to Bob
    let tok = scenario.take_from_sender<BenchToken>();
    token::send(tok, bob);
    scenario.next_tx(bob);

    // Bob has it
    let tok = scenario.take_from_sender<BenchToken>();
    assert!(token::value(&tok) == 500);
    token::burn(tok);

    scenario.end();
}
