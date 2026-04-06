// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

mod dist;
mod gen;

use gen::{run, Cfg};

fn main() {
    // args: n_events n_cpus [mem_size]
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 && args.len() != 4 {
        eprintln!("usage: trace-gen <n_events> <n_cpus> [mem_size]");
        eprintln!("examples: 8GB, 8GiB, 512MB, 1048576");
        return;
    }

    let n_events: u64 = args[1].parse().unwrap();
    let n_cpus: u32 = args[2].parse().unwrap();
    let mem_size_bytes: u64 = match args.get(3) {
        Some(arg) => match parse_mem_size_bytes(arg) {
            Ok(size) => size,
            Err(err) => {
                eprintln!("invalid mem_size `{arg}`: {err}");
                return;
            }
        },
        None => dist::DEFAULT_MEM_SIZE_BYTES,
    };

    if n_cpus == 0 {
        eprintln!("n_cpus must be greater than zero");
        return;
    }

    if mem_size_bytes < 16 {
        eprintln!("mem_size_bytes must be at least 16");
        return;
    }

    let cfg = Cfg {
        n_events,
        n_cpus,
        mem_size_bytes,
    };

    let trace = run(cfg);

    for e in trace {
        match e.ev {
            gen::Event::Alloc { addr, sz } => {
                println!("{} {} alloc {} {}", e.ts, e.cpu, addr, sz);
            }
            gen::Event::Free { addr } => {
                println!("{} {} free {}", e.ts, e.cpu, addr);
            }
        }
    }
}

fn parse_mem_size_bytes(input: &str) -> Result<u64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let numeric_end = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (num_part, suffix_part) = trimmed.split_at(numeric_end);

    if num_part.is_empty() {
        return Err("missing numeric value".to_string());
    }

    let value: f64 = num_part
        .parse()
        .map_err(|_| "numeric value must be an integer or decimal number".to_string())?;

    if !value.is_finite() || value <= 0.0 {
        return Err("numeric value must be positive".to_string());
    }

    let suffix = suffix_part.trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" => 1024_u64,
        "m" | "mb" => 1024_u64.pow(2),
        "g" | "gb" => 1024_u64.pow(3),
        "t" | "tb" => 1024_u64.pow(4),
        "kib" => 1024_u64,
        "mib" => 1024_u64.pow(2),
        "gib" => 1024_u64.pow(3),
        "tib" => 1024_u64.pow(4),
        _ => {
            return Err("unknown suffix; use B, KB, MB, GB, TB, KiB, MiB, GiB, or TiB".to_string());
        }
    };

    let total = value * multiplier as f64;
    if total > u64::MAX as f64 {
        return Err("size is too large".to_string());
    }

    Ok(total.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::parse_mem_size_bytes;

    #[test]
    fn parses_plain_bytes() {
        assert_eq!(parse_mem_size_bytes("4096").unwrap(), 4096);
    }

    #[test]
    fn parses_human_units_case_insensitively() {
        assert_eq!(parse_mem_size_bytes("8GB").unwrap(), 8 * 1024_u64.pow(3));
        assert_eq!(parse_mem_size_bytes("4Gb").unwrap(), 4 * 1024_u64.pow(3));
        assert_eq!(parse_mem_size_bytes("2GiB").unwrap(), 2 * 1024_u64.pow(3));
    }

    #[test]
    fn parses_decimal_values() {
        assert_eq!(
            parse_mem_size_bytes("1.5GB").unwrap(),
            (1.5_f64 * 1024_f64.powi(3)).round() as u64
        );
    }

    #[test]
    fn rejects_unknown_suffixes() {
        assert!(parse_mem_size_bytes("10XB").is_err());
    }
}
