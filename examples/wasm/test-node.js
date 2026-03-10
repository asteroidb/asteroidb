#!/usr/bin/env node

/**
 * AsteroidDB WASM — Node.js test script
 *
 * Verifies that the WASM module loads correctly in Node.js and that
 * all CRDT operations produce the expected results.
 *
 * Usage:
 *   cd examples/wasm
 *   wasm-pack build --target nodejs
 *   node test-node.js
 */

const assert = require("assert");

async function main() {
  // wasm-pack with --target nodejs outputs to pkg/
  const wasm = require("./pkg/asteroidb_wasm_example.js");

  let passed = 0;
  let failed = 0;

  function test(name, fn) {
    try {
      fn();
      console.log(`  \u2713 ${name}`);
      passed++;
    } catch (e) {
      console.error(`  \u2717 ${name}: ${e.message}`);
      failed++;
    }
  }

  console.log("\n=== AsteroidDB WASM Node.js Tests ===\n");

  // -----------------------------------------------------------------------
  // PnCounter
  // -----------------------------------------------------------------------
  console.log("PnCounter:");

  test("new counter starts at 0", () => {
    const c = new wasm.WasmPnCounter("node-a");
    assert.strictEqual(c.value(), BigInt(0));
  });

  test("increment increases value", () => {
    const c = new wasm.WasmPnCounter("node-a");
    c.increment();
    c.increment();
    assert.strictEqual(c.value(), BigInt(2));
  });

  test("decrement decreases value", () => {
    const c = new wasm.WasmPnCounter("node-a");
    c.increment();
    c.increment();
    c.decrement();
    assert.strictEqual(c.value(), BigInt(1));
  });

  test("merge combines independent counters", () => {
    const a = new wasm.WasmPnCounter("node-a");
    const b = new wasm.WasmPnCounter("node-b");
    a.increment();
    a.increment();
    b.increment();
    b.decrement();
    a.merge(b);
    // a: P{node-a: 2}, N{} = 2
    // b: P{node-b: 1}, N{node-b: 1} = 0
    // merged: P{node-a: 2, node-b: 1}, N{node-b: 1} = 2
    assert.strictEqual(a.value(), BigInt(2));
  });

  test("merge is idempotent", () => {
    const a = new wasm.WasmPnCounter("node-a");
    const b = new wasm.WasmPnCounter("node-b");
    a.increment();
    b.increment();
    a.merge(b);
    const v1 = a.value();
    a.merge(b);
    assert.strictEqual(a.value(), v1);
  });

  test("to_json produces valid JSON", () => {
    const c = new wasm.WasmPnCounter("node-a");
    c.increment();
    const json = c.to_json();
    const parsed = JSON.parse(json);
    assert.ok(parsed.p !== undefined);
  });

  // -----------------------------------------------------------------------
  // OrSet
  // -----------------------------------------------------------------------
  console.log("\nOrSet:");

  test("new set is empty", () => {
    const s = new wasm.WasmOrSet("node-a");
    assert.strictEqual(s.len(), 0);
    assert.strictEqual(s.is_empty(), true);
  });

  test("add inserts element", () => {
    const s = new wasm.WasmOrSet("node-a");
    s.add("apple");
    assert.strictEqual(s.contains("apple"), true);
    assert.strictEqual(s.len(), 1);
  });

  test("remove deletes element", () => {
    const s = new wasm.WasmOrSet("node-a");
    s.add("apple");
    s.remove("apple");
    assert.strictEqual(s.contains("apple"), false);
    assert.strictEqual(s.len(), 0);
  });

  test("merge with add-wins semantics", () => {
    const a = new wasm.WasmOrSet("node-a");
    const b = new wasm.WasmOrSet("node-b");
    a.add("apple");
    b.add("apple");
    a.remove("apple"); // a removes, but b still has it
    a.merge(b);
    // add-wins: b's concurrent add should survive
    assert.strictEqual(a.contains("apple"), true);
  });

  test("merge combines disjoint elements", () => {
    const a = new wasm.WasmOrSet("node-a");
    const b = new wasm.WasmOrSet("node-b");
    a.add("apple");
    b.add("banana");
    a.merge(b);
    assert.strictEqual(a.contains("apple"), true);
    assert.strictEqual(a.contains("banana"), true);
    assert.strictEqual(a.len(), 2);
  });

  test("elements_json returns valid JSON array", () => {
    const s = new wasm.WasmOrSet("node-a");
    s.add("x");
    s.add("y");
    const elems = JSON.parse(s.elements_json());
    assert.ok(Array.isArray(elems));
    assert.strictEqual(elems.length, 2);
  });

  // -----------------------------------------------------------------------
  // LwwRegister
  // -----------------------------------------------------------------------
  console.log("\nLwwRegister:");

  test("new register is empty", () => {
    const r = new wasm.WasmLwwRegister("node-a");
    assert.strictEqual(r.get(), undefined);
  });

  test("set updates value", () => {
    const r = new wasm.WasmLwwRegister("node-a");
    r.set("hello");
    assert.strictEqual(r.get(), "hello");
  });

  test("later set overwrites earlier", () => {
    const r = new wasm.WasmLwwRegister("node-a");
    r.set("first");
    r.set("second");
    assert.strictEqual(r.get(), "second");
  });

  test("merge picks later timestamp", () => {
    const a = new wasm.WasmLwwRegister("node-a");
    const b = new wasm.WasmLwwRegister("node-b");
    a.set("early");
    b.set("late-1");
    b.set("late-2"); // b has higher logical counter
    a.merge(b);
    assert.strictEqual(a.get(), "late-2");
  });

  // -----------------------------------------------------------------------
  // Store
  // -----------------------------------------------------------------------
  console.log("\nStore:");

  test("new store is empty", () => {
    const s = new wasm.WasmStore();
    assert.strictEqual(s.len(), 0);
    assert.strictEqual(s.is_empty(), true);
  });

  test("put_counter and get_json", () => {
    const s = new wasm.WasmStore();
    const c = new wasm.WasmPnCounter("node-a");
    c.increment();
    s.put_counter("visits", c);
    assert.strictEqual(s.len(), 1);
    const json = s.get_json("visits");
    assert.ok(json !== "null");
  });

  test("put_set and get_json", () => {
    const s = new wasm.WasmStore();
    const set = new wasm.WasmOrSet("node-a");
    set.add("item1");
    s.put_set("items", set);
    const json = s.get_json("items");
    assert.ok(json.includes("item1"));
  });

  test("delete removes key", () => {
    const s = new wasm.WasmStore();
    const c = new wasm.WasmPnCounter("node-a");
    s.put_counter("k", c);
    assert.strictEqual(s.delete("k"), true);
    assert.strictEqual(s.contains_key("k"), false);
  });

  test("keys_json returns all keys", () => {
    const s = new wasm.WasmStore();
    const c = new wasm.WasmPnCounter("node-a");
    s.put_counter("a", c);
    s.put_counter("b", c);
    const keys = JSON.parse(s.keys_json());
    assert.strictEqual(keys.length, 2);
  });

  test("snapshot save and load round-trip", () => {
    const s = new wasm.WasmStore();
    const c = new wasm.WasmPnCounter("node-a");
    c.increment();
    c.increment();
    s.put_counter("visits", c);
    s.save_snapshot();
    // Modify state after save
    s.delete("visits");
    assert.strictEqual(s.len(), 0);
    // Load restores saved state
    s.load_snapshot();
    assert.strictEqual(s.len(), 1);
    const json = s.get_json("visits");
    assert.ok(json !== "null");
  });

  // -----------------------------------------------------------------------
  // self_test
  // -----------------------------------------------------------------------
  console.log("\nself_test():");

  test("self_test runs without error", () => {
    const result = wasm.self_test();
    assert.ok(result.length > 0);
    assert.ok(result.includes("PnCounter"));
    assert.ok(result.includes("OrSet"));
    assert.ok(result.includes("LwwRegister"));
    assert.ok(result.includes("Store"));
    console.log("    " + result.replace(/\n/g, "\n    "));
  });

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===\n`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
