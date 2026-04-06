// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::path::PathBuf;

const MIB: u64 = 1024 * 1024;
const KIB: u64 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferMode {
    Global,
    Nuca,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub trace_path: PathBuf,
    pub per_cpu_cache_bytes: u64,
    pub dynamic_per_cpu: bool,
    pub resize_interval: u64,
    pub resize_quantum_bytes: u64,
    pub top_cpus_to_grow: usize,
    pub refill_batch: usize,
    pub drain_batch: usize,
    pub transfer_mode: TransferMode,
    pub cpus_per_domain: u32,
    pub transfer_cache_bytes: u64,
    pub central_span_lists: usize,
    pub central_span_prioritization: bool,
    pub page_size: u64,
    pub hugepage_size: u64,
    pub large_object_threshold: u64,
    pub lifetime_aware_pageheap: bool,
    pub lifetime_threshold_capacity: usize,
    pub release_empty_hugepages: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            trace_path: PathBuf::new(),
            per_cpu_cache_bytes: 1536 * KIB,
            dynamic_per_cpu: false,
            resize_interval: 50_000,
            resize_quantum_bytes: 256 * KIB,
            top_cpus_to_grow: 5,
            refill_batch: 32,
            drain_batch: 32,
            transfer_mode: TransferMode::Global,
            cpus_per_domain: 4,
            transfer_cache_bytes: 256 * KIB,
            central_span_lists: 8,
            central_span_prioritization: false,
            page_size: 8 * KIB,
            hugepage_size: 2 * MIB,
            large_object_threshold: 256 * KIB,
            lifetime_aware_pageheap: false,
            lifetime_threshold_capacity: 16,
            release_empty_hugepages: true,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let mut args = env::args().skip(1).peekable();
        if args.peek().is_none() {
            return Err(Self::usage());
        }

        let mut config = Self::default();
        let mut trace_path = None;

        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                return Err(Self::usage());
            }

            if !arg.starts_with("--") {
                if trace_path.is_some() {
                    return Err(format!(
                        "unexpected positional argument `{arg}`\n\n{}",
                        Self::usage()
                    ));
                }
                trace_path = Some(PathBuf::from(arg));
                continue;
            }

            let (key, value) = if let Some((key, value)) = arg.split_once('=') {
                (key.to_string(), value.to_string())
            } else {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for `{arg}`"))?;
                (arg, value)
            };

            match key.as_str() {
                "--per-cpu-cache" => config.per_cpu_cache_bytes = parse_size_bytes(&value)?,
                "--dynamic-per-cpu" => config.dynamic_per_cpu = parse_bool(&value)?,
                "--resize-interval" => config.resize_interval = parse_u64(&value, &key)?,
                "--resize-quantum" => config.resize_quantum_bytes = parse_size_bytes(&value)?,
                "--top-cpus-to-grow" => config.top_cpus_to_grow = parse_usize(&value, &key)?,
                "--refill-batch" => config.refill_batch = parse_usize(&value, &key)?,
                "--drain-batch" => config.drain_batch = parse_usize(&value, &key)?,
                "--transfer-mode" => config.transfer_mode = parse_transfer_mode(&value)?,
                "--cpus-per-domain" => config.cpus_per_domain = parse_u32(&value, &key)?,
                "--transfer-cache" => config.transfer_cache_bytes = parse_size_bytes(&value)?,
                "--central-span-lists" => config.central_span_lists = parse_usize(&value, &key)?,
                "--central-span-priority" => {
                    config.central_span_prioritization = parse_bool(&value)?
                }
                "--page-size" => config.page_size = parse_size_bytes(&value)?,
                "--hugepage-size" => config.hugepage_size = parse_size_bytes(&value)?,
                "--large-object-threshold" => {
                    config.large_object_threshold = parse_size_bytes(&value)?
                }
                "--lifetime-aware-pageheap" => config.lifetime_aware_pageheap = parse_bool(&value)?,
                "--lifetime-threshold-capacity" => {
                    config.lifetime_threshold_capacity = parse_usize(&value, &key)?
                }
                "--release-empty-hugepages" => config.release_empty_hugepages = parse_bool(&value)?,
                _ => {
                    return Err(format!("unknown option `{key}`\n\n{}", Self::usage()));
                }
            }
        }

        config.trace_path = trace_path.ok_or_else(Self::usage)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.per_cpu_cache_bytes == 0 {
            return Err("per-CPU cache capacity must be greater than zero".to_string());
        }
        if self.resize_interval == 0 {
            return Err("resize interval must be greater than zero".to_string());
        }
        if self.resize_quantum_bytes == 0 {
            return Err("resize quantum must be greater than zero".to_string());
        }
        if self.top_cpus_to_grow == 0 {
            return Err("top_cpus_to_grow must be greater than zero".to_string());
        }
        if self.refill_batch == 0 || self.drain_batch == 0 {
            return Err("refill_batch and drain_batch must be greater than zero".to_string());
        }
        if self.cpus_per_domain == 0 {
            return Err("cpus_per_domain must be greater than zero".to_string());
        }
        if self.transfer_cache_bytes == 0 {
            return Err("transfer cache capacity must be greater than zero".to_string());
        }
        if self.central_span_lists == 0 {
            return Err("central_span_lists must be greater than zero".to_string());
        }
        if self.page_size == 0 || !self.page_size.is_power_of_two() {
            return Err("page_size must be a non-zero power of two".to_string());
        }
        if self.hugepage_size < self.page_size || !self.hugepage_size.is_power_of_two() {
            return Err("hugepage_size must be a power of two and at least page_size".to_string());
        }
        if self.large_object_threshold < self.page_size {
            return Err("large_object_threshold must be at least page_size".to_string());
        }
        if self.lifetime_threshold_capacity == 0 {
            return Err("lifetime threshold capacity must be greater than zero".to_string());
        }
        Ok(())
    }

    pub fn usage() -> String {
        let help = r#"usage: malloc-sim <trace_path> [options]

Options:
  --per-cpu-cache <size>              Per-CPU cache capacity. Default: 1.5MiB
  --dynamic-per-cpu <on|off>          Enable usage-based per-CPU resizing. Default: off
  --resize-interval <events>          Resize interval in trace timestamps. Default: 50000
  --resize-quantum <size>             Capacity moved per resize step. Default: 256KiB
  --top-cpus-to-grow <n>              Hot CPUs grown per resize pass. Default: 5
  --refill-batch <n>                  Batch size on front-end refill. Default: 32
  --drain-batch <n>                   Batch size on front-end drain. Default: 32
  --transfer-mode <global|nuca>       Transfer cache topology. Default: global
  --cpus-per-domain <n>               CPUs in a cache domain for NUCA mode. Default: 4
  --transfer-cache <size>             Capacity per transfer cache shard. Default: 256KiB
  --central-span-lists <n>            Occupancy buckets in central freelist. Default: 8
  --central-span-priority <on|off>    Enable span prioritization. Default: off
  --page-size <size>                  Backend page size. Default: 8KiB
  --hugepage-size <size>              Backend hugepage size. Default: 2MiB
  --large-object-threshold <size>     Objects above this bypass front/middle tiers. Default: 256KiB
  --lifetime-aware-pageheap <on|off>  Enable lifetime-aware hugepage placement. Default: off
  --lifetime-threshold-capacity <n>   Span-capacity split for short/long-lived spans. Default: 16
  --release-empty-hugepages <on|off>  Release completely free hugepages. Default: on

Sizes accept raw bytes or units like 8GB, 512MiB, 1536KiB.
"#;

        help.to_string()
    }
}

fn parse_bool(input: &str) -> Result<bool, String> {
    match input.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(format!("invalid boolean value `{input}`")),
    }
}

fn parse_transfer_mode(input: &str) -> Result<TransferMode, String> {
    match input.trim().to_ascii_lowercase().as_str() {
        "global" => Ok(TransferMode::Global),
        "nuca" => Ok(TransferMode::Nuca),
        _ => Err(format!("invalid transfer mode `{input}`")),
    }
}

fn parse_u64(input: &str, key: &str) -> Result<u64, String> {
    input
        .parse()
        .map_err(|_| format!("invalid value for `{key}`: `{input}`"))
}

fn parse_u32(input: &str, key: &str) -> Result<u32, String> {
    input
        .parse()
        .map_err(|_| format!("invalid value for `{key}`: `{input}`"))
}

fn parse_usize(input: &str, key: &str) -> Result<usize, String> {
    input
        .parse()
        .map_err(|_| format!("invalid value for `{key}`: `{input}`"))
}

fn parse_size_bytes(input: &str) -> Result<u64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let numeric_end = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (num_part, suffix_part) = trimmed.split_at(numeric_end);
    if num_part.is_empty() {
        return Err(format!("missing numeric value in `{input}`"));
    }

    let value: f64 = num_part
        .parse()
        .map_err(|_| format!("invalid numeric value in `{input}`"))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("size must be positive: `{input}`"));
    }

    let suffix = suffix_part.trim().to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" | "kib" => KIB,
        "m" | "mb" | "mib" => KIB.pow(2),
        "g" | "gb" | "gib" => KIB.pow(3),
        "t" | "tb" | "tib" => KIB.pow(4),
        _ => {
            return Err(format!(
                "unknown size suffix in `{input}`; use B, KB, MB, GB, TB, KiB, MiB, GiB, or TiB"
            ));
        }
    };

    let total = value * multiplier as f64;
    if total > u64::MAX as f64 {
        return Err(format!("size is too large: `{input}`"));
    }

    Ok(total.round() as u64)
}
