// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::dist::{self, Rng};

const ARENA_BASE: u64 = 0x1000;

#[derive(Clone, Copy)]
pub enum Event {
    Alloc { addr: u64, sz: usize },
    Free { addr: u64 },
}

#[derive(Clone, Copy)]
pub struct Ent {
    pub ts: u64,
    pub cpu: u32,
    pub ev: Event,
}

pub struct Cfg {
    pub n_events: u64,
    pub n_cpus: u32,
    pub mem_size_bytes: u64,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct Obj {
    free_at: u64,
    addr: u64,
    size: usize,
}

#[derive(Clone, Copy)]
struct Region {
    addr: u64,
    len: u64,
}

pub fn run(cfg: Cfg) -> Vec<Ent> {
    let mut evs = Vec::with_capacity((cfg.n_events * 2) as usize);
    let mut rng = Rng::new(seed(&cfg));

    let mut active: Vec<BinaryHeap<Reverse<Obj>>> = (0..cfg.n_cpus as usize)
        .map(|_| BinaryHeap::new())
        .collect();
    let mut free_regions = Vec::new();

    let arena_limit = ARENA_BASE + cfg.mem_size_bytes;
    let mut bump = ARENA_BASE;

    for ts in 0..cfg.n_events {
        drain_expired(ts, &mut active, &mut free_regions, &mut evs);

        let cpu = dist::choose_cpu(&mut rng, cfg.n_cpus);
        let size = dist::sample_size(&mut rng, cfg.mem_size_bytes);
        let life = dist::sample_lifetime(&mut rng);

        let addr = loop {
            if let Some(addr) = alloc_from_arena(
                size.size as u64,
                size.align,
                arena_limit,
                &mut bump,
                &mut free_regions,
            ) {
                break addr;
            }

            if !force_free(ts, &mut active, &mut free_regions, &mut evs) {
                panic!(
                    "unable to allocate {0} bytes within {1} byte arena",
                    size.size, cfg.mem_size_bytes
                );
            }
        };

        evs.push(Ent {
            ts,
            cpu,
            ev: Event::Alloc {
                addr,
                sz: size.size,
            },
        });

        active[cpu as usize].push(Reverse(Obj {
            free_at: ts + life,
            addr,
            size: size.size,
        }));
    }

    evs
}

fn drain_expired(
    ts: u64,
    active: &mut [BinaryHeap<Reverse<Obj>>],
    free_regions: &mut Vec<Region>,
    evs: &mut Vec<Ent>,
) {
    for (cpu, queue) in active.iter_mut().enumerate() {
        while matches!(queue.peek(), Some(Reverse(obj)) if obj.free_at <= ts) {
            let obj = queue.pop().unwrap().0;
            evs.push(Ent {
                ts,
                cpu: cpu as u32,
                ev: Event::Free { addr: obj.addr },
            });
            insert_free_region(free_regions, obj.addr, obj.size as u64);
        }
    }
}

fn force_free(
    ts: u64,
    active: &mut [BinaryHeap<Reverse<Obj>>],
    free_regions: &mut Vec<Region>,
    evs: &mut Vec<Ent>,
) -> bool {
    let victim_cpu = active
        .iter()
        .enumerate()
        .filter_map(|(cpu, queue)| queue.peek().map(|Reverse(obj)| (cpu, *obj)))
        .min_by_key(|(_, obj)| obj.free_at)
        .map(|(cpu, _)| cpu);

    let Some(cpu) = victim_cpu else {
        return false;
    };

    let obj = active[cpu].pop().unwrap().0;
    evs.push(Ent {
        ts,
        cpu: cpu as u32,
        ev: Event::Free { addr: obj.addr },
    });
    insert_free_region(free_regions, obj.addr, obj.size as u64);
    true
}

fn alloc_from_arena(
    size: u64,
    align: u64,
    arena_limit: u64,
    bump: &mut u64,
    free_regions: &mut Vec<Region>,
) -> Option<u64> {
    for idx in 0..free_regions.len() {
        let region = free_regions[idx];
        let addr = align_up(region.addr, align);
        let padding = addr - region.addr;

        if padding + size > region.len {
            continue;
        }

        free_regions.remove(idx);

        if padding != 0 {
            free_regions.insert(
                idx,
                Region {
                    addr: region.addr,
                    len: padding,
                },
            );
        }

        let tail_addr = addr + size;
        let used = padding + size;
        let tail_len = region.len - used;

        if tail_len != 0 {
            let insert_at = idx + usize::from(padding != 0);
            free_regions.insert(
                insert_at,
                Region {
                    addr: tail_addr,
                    len: tail_len,
                },
            );
        }

        return Some(addr);
    }

    let addr = align_up(*bump, align);
    if addr + size > arena_limit {
        return None;
    }

    *bump = addr + size;
    Some(addr)
}

fn insert_free_region(free_regions: &mut Vec<Region>, addr: u64, len: u64) {
    let mut insert_at = free_regions.partition_point(|region| region.addr < addr);
    let mut merged_addr = addr;
    let mut merged_len = len;

    if insert_at > 0 {
        let prev = free_regions[insert_at - 1];
        if prev.addr + prev.len == addr {
            merged_addr = prev.addr;
            merged_len += prev.len;
            free_regions.remove(insert_at - 1);
            insert_at -= 1;
        }
    }

    while insert_at < free_regions.len() {
        let next = free_regions[insert_at];
        if merged_addr + merged_len != next.addr {
            break;
        }

        merged_len += next.len;
        free_regions.remove(insert_at);
    }

    free_regions.insert(
        insert_at,
        Region {
            addr: merged_addr,
            len: merged_len,
        },
    );
}

fn align_up(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (value + (align - 1)) & !(align - 1)
}

fn seed(cfg: &Cfg) -> u64 {
    cfg.n_events
        ^ ((cfg.n_cpus as u64) << 32)
        ^ cfg.mem_size_bytes.rotate_left(13)
        ^ 0xa076_1d64_78bd_642f
}
