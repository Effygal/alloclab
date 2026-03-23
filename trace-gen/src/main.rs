// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

mod gen;

use gen::{Cfg, run};

fn main() {
    // args: n_events n_threads
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("usage: trace-gen <n_events> <n_threads>");
        return;
    }

    let n_events: u64 = args[1].parse().unwrap();
    let n_threads: u32 = args[2].parse().unwrap();

    let cfg = Cfg { n_events, n_threads };

    let trace = run(cfg);

    for e in trace {
        match e.ev {
            gen::Event::Alloc { addr, sz } => {
                println!("{} {} alloc {} {}", e.ts, e.tid, addr, sz);
            }
            gen::Event::Free { addr } => {
                println!("{} {} free {}", e.ts, e.tid, addr);
            }
        }
    }
}
