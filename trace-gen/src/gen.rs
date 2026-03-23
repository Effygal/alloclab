// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;

#[derive(Clone, Copy)]
pub enum Event {
    Alloc { addr: u64, sz: usize },
    Free { addr: u64 },
}

#[derive(Clone, Copy)]
pub struct Ent {
    pub ts: u64,
    pub tid: u32,
    pub ev: Event,
}

pub struct Cfg {
    pub n_events: u64,
    pub n_threads: u32,
}

struct Obj {
    addr: u64,
    free_at: u64,
}

pub fn run(cfg: Cfg) -> Vec<Ent> {
    let mut evs = Vec::with_capacity((cfg.n_events * 2) as usize);
    let mut next_addr: u64 = 1;

    let mut active: Vec<VecDeque<Obj>> = (0..cfg.n_threads as usize)
        .map(|_| VecDeque::new())
        .collect();

    for ts in 0..cfg.n_events {
        let tid = (ts % cfg.n_threads as u64) as u32;

        // free expired
        while let Some(obj) = active[tid as usize].front() {
            if obj.free_at > ts {
                break;
            }
            let obj = active[tid as usize].pop_front().unwrap();
            evs.push(Ent {
                ts,
                tid,
                ev: Event::Free { addr: obj.addr },
            });
        }

        // alloc
        let addr = next_addr;
        next_addr += 1;

        let sz = 64;   // placeholder
        let life = 10; // placeholder

        evs.push(Ent {
            ts,
            tid,
            ev: Event::Alloc { addr, sz },
        });

        active[tid as usize].push_back(Obj {
            addr,
            free_at: ts + life,
        });
    }

    evs
}
