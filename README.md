# alloclab

A small Rust workspace with two crates:

- `trace-gen` generates a synthetic allocation trace with CPU-local activity,
  mixed object sizes, and a bounded memory arena.
- `malloc-sim` simulates a TCMalloc-inspired allocator hierarchy on top of
  a trace and reports cache-path, fragmentation, and backend metrics.

Here, "arena" just means the total simulated memory region that the generator
is allowed to hand out addresses from. If you set the arena to `8GB`, emitted
addresses stay within that 8 GiB address space and freed regions can be reused.

Most of the time, you will work from the repository root and use `cargo run`.

## Common Commands

### Run the trace generator

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

If you want an optimized build, add `--release`:

```bash
cargo run --release -p trace-gen -- 1000000 8 2GB > trace1.txt
```

The trace format is plain text, one event per line:

```text
<ts> <cpu> alloc <addr> <size>
<ts> <cpu> free <addr>
```

## Run the simulator

```bash
cargo run -p malloc-sim -- trace1.txt
```

An example with several knobs enabled:

```bash
cargo run -p malloc-sim -- trace1.txt \
  --dynamic-per-cpu on \
  --transfer-mode nuca \
  --central-span-priority on \
  --lifetime-aware-pageheap on
```

`malloc-sim` models four allocator layers:

- per-CPU front-end caches
- transfer caches in `global` or `nuca` mode
- central free lists with optional occupancy-based span prioritization
- a hugepage-aware page heap with optional lifetime-aware span placement

The simulator reports metrics such as:

- allocation path mix across `per_cpu`, `transfer`, `central`, `pageheap`, and `mmap`
- estimated allocator latency based on tier hit paths
- per-CPU miss rate and remote transfer fetches
- internal and external fragmentation, including a tier breakdown
- hugepage coverage proxy, hugepages mapped/released, and span churn

Useful knobs:

- `--per-cpu-cache 1.5MiB`
- `--dynamic-per-cpu on`
- `--resize-interval 5000`
- `--transfer-mode global|nuca`
- `--cpus-per-domain 4`
- `--transfer-cache 256KiB`
- `--central-span-priority on`
- `--central-span-lists 8`
- `--large-object-threshold 256KiB`
- `--lifetime-aware-pageheap on`
- `--lifetime-threshold-capacity 16`

See the built-in help for the full CLI:

```bash
cargo run -p malloc-sim -- --help
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
