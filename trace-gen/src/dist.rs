// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

pub const DEFAULT_MEM_SIZE_BYTES: u64 = 512 * 1024 * 1024 * 1024;

const SMALL_ALIGN: u64 = 16;
const MEDIUM_ALIGN: u64 = 64;
const LARGE_ALIGN: u64 = 4096;

#[derive(Clone, Copy)]
pub struct SizeSpec {
    pub size: usize,
    pub align: u64,
}

pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        };

        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn range_u64(&mut self, start: u64, end: u64) -> u64 {
        assert!(start < end);
        start + (self.next_u64() % (end - start))
    }

    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        assert!(num <= den && den != 0);
        self.next_u64() % den < num
    }
}

pub fn choose_cpu(rng: &mut Rng, n_cpus: u32) -> u32 {
    if n_cpus <= 1 {
        return 0;
    }

    let hot_cpus = ((n_cpus as u64 + 3) / 4).max(1) as u32;

    if hot_cpus == n_cpus || rng.chance(4, 5) {
        rng.range_u64(0, hot_cpus as u64) as u32
    } else {
        rng.range_u64(hot_cpus as u64, n_cpus as u64) as u32
    }
}

pub fn sample_size(rng: &mut Rng, mem_size_bytes: u64) -> SizeSpec {
    let raw = match rng.range_u64(0, 100) {
        0..=59 => SizeSpec {
            size: sample_aligned(rng, 16, 256, SMALL_ALIGN),
            align: SMALL_ALIGN,
        },
        60..=84 => SizeSpec {
            size: sample_aligned(rng, 512, 8 * 1024, MEDIUM_ALIGN),
            align: MEDIUM_ALIGN,
        },
        85..=96 => SizeSpec {
            size: sample_aligned(rng, 16 * 1024, 256 * 1024, LARGE_ALIGN),
            align: LARGE_ALIGN,
        },
        _ => SizeSpec {
            size: sample_aligned(rng, 1 * 1024 * 1024, 8 * 1024 * 1024, LARGE_ALIGN),
            align: LARGE_ALIGN,
        },
    };

    fit_to_arena(raw, mem_size_bytes)
}

pub fn sample_lifetime(rng: &mut Rng) -> u64 {
    match rng.range_u64(0, 100) {
        0..=64 => rng.range_u64(1, 33),
        65..=89 => rng.range_u64(33, 513),
        90..=97 => rng.range_u64(513, 8_193),
        _ => rng.range_u64(8_193, 65_537),
    }
}

fn sample_aligned(rng: &mut Rng, min: usize, max: usize, align: u64) -> usize {
    let span = rng.range_u64(min as u64, max as u64 + 1);
    align_up(span, align) as usize
}

fn fit_to_arena(spec: SizeSpec, mem_size_bytes: u64) -> SizeSpec {
    let max_align = if mem_size_bytes >= LARGE_ALIGN {
        LARGE_ALIGN
    } else if mem_size_bytes >= MEDIUM_ALIGN {
        MEDIUM_ALIGN
    } else {
        SMALL_ALIGN
    };

    let align = spec.align.min(max_align);
    let usable = align_down(mem_size_bytes.max(align), align).max(align);
    let size = (spec.size as u64).min(usable);

    SizeSpec {
        size: align_up(size, align) as usize,
        align,
    }
}

fn align_up(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (value + (align - 1)) & !(align - 1)
}

fn align_down(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    value & !(align - 1)
}
