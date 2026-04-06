# alloclab

A small Rust workspace with two crates:

- `trace-gen` generates a synthetic allocation trace with CPU-local activity,
  mixed object sizes, and a bounded memory arena.
- `malloc-sim` is the simulator side. Right now it is still a stub.

Here, "arena" just means the total simulated memory region that the generator
is allowed to hand out addresses from. If you set the arena to `8GB`, emitted
addresses stay within that 8 GiB address space and freed regions can be reused.

Most of the time, you will work from the repository root and use `cargo run`.

## Common Commands

Run the trace generator:

```bash
cargo run -p trace-gen -- 100 4
```

Optionally pass the arena size as bytes or with human-readable units:

```bash
cargo run -p trace-gen -- 100 4 8GB
cargo run -p trace-gen -- 100 4 4Gb
cargo run -p trace-gen -- 100 4 512MiB
cargo run -p trace-gen -- 100 4 549755813888
```

It emits a trace of format
```
<ts> <cpu> alloc <addr> <size>
<ts> <cpu> free <addr>
```

Run the simulator:

```bash
cargo run -p malloc-sim
```

If you want an optimized build, add `--release`:

```bash
cargo run --release -p trace-gen -- 100 4
```

The trace format is plain text, one event per line:

```text
<ts> <cpu> alloc <addr> <size>
<ts> <cpu> free <addr>
```
Usually, use

`cargo run` 
while developing and testing logic;

`cargo run --release` 
when measuring performance or preparing something closer to production

## `target/debug` and `target/release`

Cargo writes compiled binaries under `target/`.

- `target/debug/` is the default build output. It compiles faster and is what you get from plain `cargo build` or `cargo run`.
- `target/release/` is the optimized build output. It compiles more slowly, but the resulting binary is faster.

So these produce different binaries:

```bash
cargo run -p malloc-sim
cargo run --release -p malloc-sim
```

The first uses `target/debug/malloc-sim`. The second uses `target/release/malloc-sim`.
