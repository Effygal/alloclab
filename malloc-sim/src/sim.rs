// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use crate::config::{Config, TransferMode};

const PER_CPU_LATENCY_NS: f64 = 3.1;
const TRANSFER_LATENCY_NS: f64 = 18.0;
const CENTRAL_LATENCY_NS: f64 = 52.0;
const PAGEHEAP_LATENCY_NS: f64 = 137.0;
const MMAP_LATENCY_NS: f64 = 12_000.0;

pub struct Simulator {
    config: Config,
    classes: SizeClassCatalog,
    per_cpu: Vec<PerCpuCache>,
    transfer: TransferLayer,
    central: CentralFreeList,
    pageheap: PageHeap,
    spans: Vec<Span>,
    live_allocs: HashMap<u64, LiveAlloc>,
    stats: Stats,
    next_resize_ts: u64,
    current_ts: u64,
    max_cpu_seen: u32,
}

impl Simulator {
    pub fn new(config: Config) -> Self {
        let pageheap = PageHeap::new(
            config.page_size,
            config.hugepage_size,
            config.lifetime_aware_pageheap,
            config.lifetime_threshold_capacity,
            config.release_empty_hugepages,
        );

        Self {
            classes: SizeClassCatalog::new(
                config.page_size,
                config.hugepage_size,
                config.large_object_threshold,
            ),
            transfer: TransferLayer::new(
                config.transfer_mode,
                config.cpus_per_domain,
                config.transfer_cache_bytes,
            ),
            central: CentralFreeList::new(
                config.central_span_lists,
                config.central_span_prioritization,
            ),
            pageheap,
            stats: Stats::default(),
            spans: Vec::new(),
            per_cpu: Vec::new(),
            live_allocs: HashMap::new(),
            next_resize_ts: config.resize_interval,
            current_ts: 0,
            max_cpu_seen: 0,
            config,
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        let file = File::open(&self.config.trace_path)
            .map_err(|err| format!("failed to open {:?}: {err}", self.config.trace_path))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut line_no = 0_u64;

        loop {
            line.clear();
            let n = reader
                .read_line(&mut line)
                .map_err(|err| format!("failed to read trace: {err}"))?;
            if n == 0 {
                break;
            }
            line_no += 1;
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() {
                continue;
            }
            let event = parse_trace_line(line_trimmed, line_no)?;
            self.apply_event(event)?;
        }

        self.refresh_peaks();
        Ok(())
    }

    pub fn write_report<W: Write>(&self, mut out: W) -> Result<(), std::io::Error> {
        let snapshot = self.snapshot();
        writeln!(out, "Trace: {}", self.config.trace_path.display())?;
        writeln!(out, "CPUs seen: {}", self.max_cpu_seen + 1)?;
        writeln!(out, "Transfer mode: {}", self.config.transfer_mode_name())?;
        writeln!(
            out,
            "Knobs: per_cpu_cache={} transfer_cache={} central_priority={} lifetime_aware_pageheap={} dynamic_per_cpu={}",
            human_bytes(self.config.per_cpu_cache_bytes),
            human_bytes(self.config.transfer_cache_bytes),
            on_off(self.config.central_span_prioritization),
            on_off(self.config.lifetime_aware_pageheap),
            on_off(self.config.dynamic_per_cpu),
        )?;
        writeln!(out)?;

        writeln!(out, "Workload")?;
        writeln!(out, "  allocs: {}", self.stats.alloc_events)?;
        writeln!(out, "  frees: {}", self.stats.free_events)?;
        writeln!(out, "  live objects at end: {}", self.live_allocs.len())?;
        writeln!(
            out,
            "  requested bytes: total={} peak_live={} end_live={}",
            human_bytes(self.stats.total_requested_bytes),
            human_bytes(self.stats.peak_live_requested_bytes),
            human_bytes(snapshot.live_requested_bytes),
        )?;
        writeln!(out)?;

        writeln!(out, "Allocation Paths")?;
        for (label, count, pct) in [
            (
                "per_cpu",
                self.stats.alloc_path_counts[AllocSource::PerCpu.index()],
                pct_of(
                    self.stats.alloc_path_counts[AllocSource::PerCpu.index()],
                    self.stats.alloc_events,
                ),
            ),
            (
                "transfer",
                self.stats.alloc_path_counts[AllocSource::Transfer.index()],
                pct_of(
                    self.stats.alloc_path_counts[AllocSource::Transfer.index()],
                    self.stats.alloc_events,
                ),
            ),
            (
                "central",
                self.stats.alloc_path_counts[AllocSource::Central.index()],
                pct_of(
                    self.stats.alloc_path_counts[AllocSource::Central.index()],
                    self.stats.alloc_events,
                ),
            ),
            (
                "pageheap",
                self.stats.alloc_path_counts[AllocSource::PageHeap.index()],
                pct_of(
                    self.stats.alloc_path_counts[AllocSource::PageHeap.index()],
                    self.stats.alloc_events,
                ),
            ),
            (
                "mmap",
                self.stats.alloc_path_counts[AllocSource::Mmap.index()],
                pct_of(
                    self.stats.alloc_path_counts[AllocSource::Mmap.index()],
                    self.stats.alloc_events,
                ),
            ),
        ] {
            writeln!(out, "  {label}: {count} ({pct:.2}%)")?;
        }
        writeln!(
            out,
            "  per_cpu miss rate: {:.2}%",
            pct_of(self.stats.per_cpu_misses, self.stats.alloc_events)
        )?;
        writeln!(
            out,
            "  transfer local hits: {}",
            self.stats.transfer_local_hits
        )?;
        writeln!(
            out,
            "  transfer backing/global hits: {}",
            self.stats.transfer_backing_hits
        )?;
        writeln!(
            out,
            "  remote transfer fetches: {} ({:.2}% of transfer fetches)",
            self.stats.remote_transfer_allocs,
            pct_of(
                self.stats.remote_transfer_allocs,
                self.stats.transfer_local_hits + self.stats.transfer_backing_hits,
            )
        )?;
        writeln!(out)?;

        writeln!(out, "Estimated Allocator Cost")?;
        writeln!(
            out,
            "  alloc ns total={:.1} avg={:.2}",
            self.stats.alloc_latency_ns,
            average(self.stats.alloc_latency_ns, self.stats.alloc_events),
        )?;
        writeln!(
            out,
            "  free ns total={:.1} avg={:.2}",
            self.stats.free_latency_ns,
            average(self.stats.free_latency_ns, self.stats.free_events),
        )?;
        writeln!(
            out,
            "  total allocator ns={:.1}",
            self.stats.alloc_latency_ns + self.stats.free_latency_ns,
        )?;
        writeln!(out)?;

        writeln!(out, "Memory")?;
        writeln!(
            out,
            "  footprint: peak={} end={}",
            human_bytes(self.stats.peak_footprint_bytes),
            human_bytes(snapshot.footprint_bytes),
        )?;
        writeln!(
            out,
            "  internal fragmentation: peak={} end={} ({:.2}% of live requested)",
            human_bytes(self.stats.peak_internal_fragmentation_bytes),
            human_bytes(snapshot.internal_fragmentation_bytes),
            pct_bytes(
                snapshot.internal_fragmentation_bytes,
                snapshot.live_requested_bytes,
            )
        )?;
        writeln!(
            out,
            "  external fragmentation: peak={} end={} ({:.2}% of live requested)",
            human_bytes(self.stats.peak_external_fragmentation_bytes),
            human_bytes(snapshot.external_fragmentation_bytes),
            pct_bytes(
                snapshot.external_fragmentation_bytes,
                snapshot.live_requested_bytes,
            )
        )?;
        writeln!(
            out,
            "  external breakdown: per_cpu={} transfer={} central={} pageheap={}",
            human_bytes(snapshot.per_cpu_bytes),
            human_bytes(snapshot.transfer_bytes),
            human_bytes(snapshot.central_bytes),
            human_bytes(snapshot.pageheap_bytes),
        )?;
        writeln!(
            out,
            "  hugepage coverage proxy: {:.2}%",
            pct_bytes(
                snapshot.small_active_span_bytes,
                snapshot.mapped_small_bytes.max(1),
            )
        )?;
        writeln!(out)?;

        writeln!(out, "Backend")?;
        writeln!(out, "  spans created: {}", self.stats.spans_created)?;
        writeln!(out, "  spans released: {}", self.stats.spans_released)?;
        writeln!(
            out,
            "  hugepages mapped={} released={} active={}",
            self.stats.hugepages_mapped,
            self.stats.hugepages_released,
            self.pageheap.active_hugepages(),
        )?;
        writeln!(
            out,
            "  large direct allocs: {}",
            self.stats.direct_large_allocs
        )?;
        writeln!(
            out,
            "  per_cpu resize passes={} bytes_rebalanced={}",
            self.stats.resize_passes,
            human_bytes(self.stats.rebalanced_bytes),
        )?;

        Ok(())
    }

    fn apply_event(&mut self, event: TraceEvent) -> Result<(), String> {
        self.current_ts = event.ts();
        if self.config.dynamic_per_cpu {
            while self.current_ts >= self.next_resize_ts {
                self.rebalance_per_cpu_caches();
                self.next_resize_ts = self
                    .next_resize_ts
                    .saturating_add(self.config.resize_interval);
            }
        }

        match event {
            TraceEvent::Alloc {
                cpu, addr, size, ..
            } => self.alloc(cpu, addr, size)?,
            TraceEvent::Free { cpu, addr, .. } => self.free(cpu, addr)?,
        }

        self.refresh_peaks();
        Ok(())
    }

    fn alloc(&mut self, cpu: u32, addr: u64, size: u64) -> Result<(), String> {
        self.ensure_cpu(cpu);
        if self.live_allocs.contains_key(&addr) {
            return Err(format!("double allocation of live address {addr}"));
        }

        self.stats.alloc_events += 1;
        self.stats.total_requested_bytes += size;
        self.stats.live_requested_bytes += size;

        if let Some(class_id) = self.classes.class_for(size) {
            let class = self.classes.class(class_id).clone();
            let (span_id, source) = self.alloc_small(cpu, class_id)?;
            self.live_allocs.insert(
                addr,
                LiveAlloc::Small {
                    span_id,
                    requested_size: size,
                    allocated_size: class.object_size,
                },
            );
            self.stats.live_allocated_bytes += class.object_size;
            self.stats.alloc_path_counts[source.index()] += 1;
            self.stats.alloc_latency_ns += source.latency_ns();
        } else {
            let source = self.pageheap.alloc_large(size);
            self.live_allocs.insert(
                addr,
                LiveAlloc::Large {
                    requested_size: size,
                    allocated_size: align_up(size, self.config.page_size),
                },
            );
            self.stats.direct_large_allocs += 1;
            self.stats.live_allocated_bytes += align_up(size, self.config.page_size);
            self.stats.alloc_path_counts[source.index()] += 1;
            self.stats.alloc_latency_ns += source.latency_ns();
        }

        Ok(())
    }

    fn free(&mut self, cpu: u32, addr: u64) -> Result<(), String> {
        self.ensure_cpu(cpu);
        let alloc = self
            .live_allocs
            .remove(&addr)
            .ok_or_else(|| format!("free of unknown address {addr}"))?;

        self.stats.free_events += 1;

        match alloc {
            LiveAlloc::Small {
                span_id,
                requested_size,
                allocated_size,
            } => {
                self.stats.live_requested_bytes -= requested_size;
                self.stats.live_allocated_bytes -= allocated_size;
                let source = self.free_small(cpu, span_id)?;
                self.stats.free_path_counts[source.index()] += 1;
                self.stats.free_latency_ns += source.latency_ns();
            }
            LiveAlloc::Large {
                requested_size,
                allocated_size,
            } => {
                self.stats.live_requested_bytes -= requested_size;
                self.stats.live_allocated_bytes -= allocated_size;
                self.pageheap.free_large(allocated_size);
                self.stats.free_path_counts[AllocSource::PageHeap.index()] += 1;
                self.stats.free_latency_ns += PAGEHEAP_LATENCY_NS;
            }
        }

        Ok(())
    }

    fn alloc_small(&mut self, cpu: u32, class_id: usize) -> Result<(usize, AllocSource), String> {
        if let Some(span_id) = self.per_cpu_alloc(cpu, class_id) {
            self.on_object_allocated(span_id);
            return Ok((span_id, AllocSource::PerCpu));
        }

        self.stats.per_cpu_misses += 1;
        self.per_cpu[cpu as usize].misses_in_interval += 1;

        let class_bytes = self.classes.class(class_id).object_size;
        let available_slots = self.per_cpu[cpu as usize].available_slots(class_bytes);
        let batch = available_slots.max(1).min(self.config.refill_batch);
        let (mut entries, source) = self.acquire_from_middle(cpu, class_id, batch)?;

        let span_id = take_one(&mut entries)
            .ok_or_else(|| "middle tier returned no objects for refill".to_string())?;
        self.on_object_allocated(span_id);
        self.store_in_per_cpu(cpu, class_id, entries);
        Ok((span_id, source))
    }

    fn free_small(&mut self, cpu: u32, span_id: usize) -> Result<AllocSource, String> {
        {
            let span = self
                .spans
                .get_mut(span_id)
                .ok_or_else(|| format!("unknown span {span_id}"))?;
            if span.live_count == 0 {
                return Err(format!("double free detected for span {span_id}"));
            }
            span.live_count -= 1;
        }
        self.refresh_central_bucket(span_id);

        let class_id = self.spans[span_id].class_id;
        let class_bytes = self.classes.class(class_id).object_size;

        if self.per_cpu[cpu as usize].can_store(class_bytes) {
            self.store_in_per_cpu(cpu, class_id, vec![TokenRun { span_id, count: 1 }]);
            self.try_release_span(span_id);
            return Ok(AllocSource::PerCpu);
        }

        if self.per_cpu[cpu as usize].pool_len(class_id) >= self.config.drain_batch {
            let drained = self.per_cpu_take(cpu, class_id, self.config.drain_batch);
            self.store_in_transfer(cpu, class_id, drained);
            if self.per_cpu[cpu as usize].can_store(class_bytes) {
                self.store_in_per_cpu(cpu, class_id, vec![TokenRun { span_id, count: 1 }]);
                self.try_release_span(span_id);
                return Ok(AllocSource::Transfer);
            }
        }

        let touched_central =
            self.store_in_transfer(cpu, class_id, vec![TokenRun { span_id, count: 1 }]);
        self.try_release_span(span_id);
        Ok(if touched_central {
            AllocSource::Central
        } else {
            AllocSource::Transfer
        })
    }

    fn per_cpu_alloc(&mut self, cpu: u32, class_id: usize) -> Option<usize> {
        let class_bytes = self.classes.class(class_id).object_size;
        let span_id = self.per_cpu[cpu as usize].pop_one(class_id, class_bytes)?;
        self.stats.per_cpu_bytes -= class_bytes;
        self.spans[span_id].per_cpu_cached -= 1;
        Some(span_id)
    }

    fn per_cpu_take(&mut self, cpu: u32, class_id: usize, count: usize) -> Vec<TokenRun> {
        let class_bytes = self.classes.class(class_id).object_size;
        let entries = self.per_cpu[cpu as usize].pop_many(class_id, count, class_bytes);
        let removed: usize = entries.iter().map(|entry| entry.count).sum();
        self.stats.per_cpu_bytes -= class_bytes * removed as u64;
        for entry in &entries {
            self.spans[entry.span_id].per_cpu_cached -= entry.count;
        }
        entries
    }

    fn store_in_per_cpu(&mut self, cpu: u32, class_id: usize, entries: Vec<TokenRun>) {
        if entries.is_empty() {
            return;
        }
        let class_bytes = self.classes.class(class_id).object_size;
        let added: usize = entries.iter().map(|entry| entry.count).sum();
        self.stats.per_cpu_bytes += class_bytes * added as u64;
        for entry in &entries {
            self.spans[entry.span_id].per_cpu_cached += entry.count;
        }
        self.per_cpu[cpu as usize].push_many(class_id, entries, class_bytes);
    }

    fn acquire_from_middle(
        &mut self,
        cpu: u32,
        class_id: usize,
        count: usize,
    ) -> Result<(Vec<TokenRun>, AllocSource), String> {
        let mut entries = Vec::new();
        let mut remaining = count;
        let mut source = AllocSource::Transfer;

        let (local_entries, local_remote) = self.transfer_take(cpu, class_id, remaining, false);
        remaining -= local_entries.iter().map(|entry| entry.count).sum::<usize>();
        if !local_entries.is_empty() {
            self.stats.transfer_local_hits += 1;
            if local_remote {
                self.stats.remote_transfer_allocs += 1;
            }
            entries.extend(local_entries);
        }

        if remaining > 0 {
            let (backing_entries, backing_remote) =
                self.transfer_take(cpu, class_id, remaining, true);
            remaining -= backing_entries
                .iter()
                .map(|entry| entry.count)
                .sum::<usize>();
            if !backing_entries.is_empty() {
                self.stats.transfer_backing_hits += 1;
                if backing_remote {
                    self.stats.remote_transfer_allocs += 1;
                }
                entries.extend(backing_entries);
            }
        }

        if remaining == 0 {
            return Ok((entries, source));
        }

        let (central_entries, backing) = self.central_take(class_id, remaining)?;
        if !central_entries.is_empty() {
            source = backing;
            entries.extend(central_entries);
        }

        Ok((entries, source))
    }

    fn transfer_take(
        &mut self,
        cpu: u32,
        class_id: usize,
        count: usize,
        backing: bool,
    ) -> (Vec<TokenRun>, bool) {
        let class_bytes = self.classes.class(class_id).object_size;
        let (entries, remote) = self
            .transfer
            .take(cpu, class_id, count, backing, class_bytes);
        let removed: usize = entries.iter().map(|entry| entry.count).sum();
        self.stats.transfer_bytes -= class_bytes * removed as u64;
        for entry in &entries {
            self.spans[entry.span_id].transfer_cached -= entry.count;
        }
        (entries, remote)
    }

    fn store_in_transfer(&mut self, cpu: u32, class_id: usize, entries: Vec<TokenRun>) -> bool {
        if entries.is_empty() {
            return false;
        }

        let class_bytes = self.classes.class(class_id).object_size;
        let overflow = self.transfer.store(cpu, class_id, entries, class_bytes);
        let stored: usize = overflow.stored.iter().map(|entry| entry.count).sum();
        self.stats.transfer_bytes += class_bytes * stored as u64;
        for entry in &overflow.stored {
            self.spans[entry.span_id].transfer_cached += entry.count;
        }

        let touched_central = !overflow.overflow.is_empty();
        if touched_central {
            self.store_in_central(class_id, overflow.overflow);
        }
        touched_central
    }

    fn central_take(
        &mut self,
        class_id: usize,
        count: usize,
    ) -> Result<(Vec<TokenRun>, AllocSource), String> {
        let mut out = Vec::new();
        let mut remaining = count;
        let mut source = AllocSource::Central;
        let class = self.classes.class(class_id).clone();
        let class_bytes = class.object_size;

        while remaining > 0 {
            if let Some(span_id) = self.central.pop_span(class_id, &self.spans) {
                let take = remaining.min(self.spans[span_id].central_cached);
                self.spans[span_id].central_cached -= take;
                self.stats.central_bytes -= class_bytes * take as u64;
                out.push(TokenRun {
                    span_id,
                    count: take,
                });
                if self.spans[span_id].central_cached > 0 {
                    self.central.requeue(class_id, span_id, &self.spans);
                }
                remaining -= take;
                continue;
            }

            let (span_id, backing) = self.make_span(class_id, &class)?;
            if backing == AllocSource::Mmap {
                source = AllocSource::Mmap;
            } else if source != AllocSource::Mmap {
                source = AllocSource::PageHeap;
            }
            self.central.requeue(class_id, span_id, &self.spans);
        }

        Ok((out, source))
    }

    fn make_span(
        &mut self,
        class_id: usize,
        class: &SizeClass,
    ) -> Result<(usize, AllocSource), String> {
        let (hugepage_id, backing) = self.pageheap.allocate_span(class)?;
        let span_id = self.spans.len();
        self.spans.push(Span {
            class_id,
            object_size: class.object_size,
            span_bytes: class.span_bytes,
            capacity: class.span_capacity,
            live_count: 0,
            per_cpu_cached: 0,
            transfer_cached: 0,
            central_cached: class.span_capacity,
            slack_bytes: class.slack_bytes,
            hugepage_id: Some(hugepage_id),
            active: true,
            queue_version: 0,
        });
        self.stats.spans_created += 1;
        if backing == AllocSource::Mmap {
            self.stats.hugepages_mapped += 1;
        }
        self.stats.central_bytes +=
            class.object_size * class.span_capacity as u64 + class.slack_bytes;
        Ok((span_id, backing))
    }

    fn store_in_central(&mut self, class_id: usize, entries: Vec<TokenRun>) {
        if entries.is_empty() {
            return;
        }
        let class_bytes = self.classes.class(class_id).object_size;
        let total: usize = entries.iter().map(|entry| entry.count).sum();
        self.stats.central_bytes += class_bytes * total as u64;

        for entry in entries {
            self.spans[entry.span_id].central_cached += entry.count;
            self.central.requeue(class_id, entry.span_id, &self.spans);
            self.try_release_span(entry.span_id);
        }
    }

    fn on_object_allocated(&mut self, span_id: usize) {
        self.spans[span_id].live_count += 1;
        self.refresh_central_bucket(span_id);
    }

    fn refresh_central_bucket(&mut self, span_id: usize) {
        if !self.spans[span_id].active || self.spans[span_id].central_cached == 0 {
            return;
        }
        let class_id = self.spans[span_id].class_id;
        self.central.requeue(class_id, span_id, &self.spans);
    }

    fn try_release_span(&mut self, span_id: usize) {
        if !self.spans[span_id].releasable() {
            return;
        }

        let class_id = self.spans[span_id].class_id;
        let object_size = self.spans[span_id].object_size;
        let central_objects = self.spans[span_id].central_cached as u64;
        self.stats.central_bytes -= object_size * central_objects + self.spans[span_id].slack_bytes;

        let hugepage_id = self.spans[span_id]
            .hugepage_id
            .expect("active span must have a hugepage");
        let span_bytes = self.spans[span_id].span_bytes;
        self.spans[span_id].central_cached = 0;
        self.spans[span_id].active = false;
        self.spans[span_id].queue_version = self.spans[span_id].queue_version.wrapping_add(1);
        if self.pageheap.release_span(hugepage_id, span_bytes) {
            self.stats.hugepages_released += 1;
        }
        self.stats.spans_released += 1;
        self.central.invalidate(class_id, span_id);
    }

    fn refresh_peaks(&mut self) {
        let snapshot = self.snapshot();
        self.stats.peak_live_requested_bytes = self
            .stats
            .peak_live_requested_bytes
            .max(snapshot.live_requested_bytes);
        self.stats.peak_footprint_bytes = self
            .stats
            .peak_footprint_bytes
            .max(snapshot.footprint_bytes);
        self.stats.peak_internal_fragmentation_bytes = self
            .stats
            .peak_internal_fragmentation_bytes
            .max(snapshot.internal_fragmentation_bytes);
        self.stats.peak_external_fragmentation_bytes = self
            .stats
            .peak_external_fragmentation_bytes
            .max(snapshot.external_fragmentation_bytes);
    }

    fn rebalance_per_cpu_caches(&mut self) {
        if self.per_cpu.len() < 2 {
            return;
        }

        self.stats.resize_passes += 1;
        let mut ranked: Vec<(usize, u64)> = self
            .per_cpu
            .iter()
            .enumerate()
            .map(|(cpu, cache)| (cpu, cache.misses_in_interval))
            .collect();
        ranked.sort_by_key(|&(cpu, misses)| (Reverse(misses), cpu));

        let grow_targets: Vec<usize> = ranked
            .iter()
            .take(self.config.top_cpus_to_grow.min(ranked.len()))
            .filter(|(_, misses)| *misses > 0)
            .map(|(cpu, _)| *cpu)
            .collect();
        if grow_targets.is_empty() {
            for cache in &mut self.per_cpu {
                cache.misses_in_interval = 0;
            }
            return;
        }

        let donors: Vec<usize> = ranked
            .iter()
            .rev()
            .map(|(cpu, _)| *cpu)
            .filter(|cpu| !grow_targets.contains(cpu))
            .collect();
        if donors.is_empty() {
            for cache in &mut self.per_cpu {
                cache.misses_in_interval = 0;
            }
            return;
        }

        let min_capacity = self.config.per_cpu_cache_bytes / 4;
        let mut donor_idx = 0;

        for target in grow_targets {
            let Some(&donor) = donors.get(donor_idx % donors.len()) else {
                break;
            };
            donor_idx += 1;

            if self.per_cpu[donor].capacity_bytes <= min_capacity + self.config.resize_quantum_bytes
            {
                continue;
            }

            self.per_cpu[donor].capacity_bytes -= self.config.resize_quantum_bytes;
            self.per_cpu[target].capacity_bytes += self.config.resize_quantum_bytes;
            self.stats.rebalanced_bytes += self.config.resize_quantum_bytes;

            self.evict_per_cpu_excess(donor as u32);
        }

        for cache in &mut self.per_cpu {
            cache.misses_in_interval = 0;
        }
    }

    fn evict_per_cpu_excess(&mut self, cpu: u32) {
        let cpu_idx = cpu as usize;
        while self.per_cpu[cpu_idx].used_bytes > self.per_cpu[cpu_idx].capacity_bytes {
            let Some(class_id) = self.per_cpu[cpu_idx].largest_nonempty_class() else {
                break;
            };

            let class_bytes = self.classes.class(class_id).object_size;
            let overflow_bytes =
                self.per_cpu[cpu_idx].used_bytes - self.per_cpu[cpu_idx].capacity_bytes;
            let mut count = (overflow_bytes / class_bytes).max(1) as usize;
            count = count.min(self.per_cpu[cpu_idx].pool_len(class_id));

            let entries = self.per_cpu_take(cpu, class_id, count);
            self.store_in_transfer(cpu, class_id, entries);
        }
    }

    fn ensure_cpu(&mut self, cpu: u32) {
        if cpu > self.max_cpu_seen {
            self.max_cpu_seen = cpu;
        }
        while self.per_cpu.len() <= cpu as usize {
            self.per_cpu
                .push(PerCpuCache::new(self.config.per_cpu_cache_bytes));
        }
    }

    fn snapshot(&self) -> Snapshot {
        let pageheap_bytes = self.pageheap.external_bytes();
        let mapped_small_bytes = self.pageheap.mapped_small_bytes;
        let small_active_span_bytes = self.pageheap.active_span_bytes();
        let external_fragmentation_bytes = self.stats.per_cpu_bytes
            + self.stats.transfer_bytes
            + self.stats.central_bytes
            + pageheap_bytes;

        Snapshot {
            live_requested_bytes: self.stats.live_requested_bytes,
            footprint_bytes: self.stats.live_allocated_bytes + external_fragmentation_bytes,
            internal_fragmentation_bytes: self.stats.live_allocated_bytes
                - self.stats.live_requested_bytes,
            external_fragmentation_bytes,
            per_cpu_bytes: self.stats.per_cpu_bytes,
            transfer_bytes: self.stats.transfer_bytes,
            central_bytes: self.stats.central_bytes,
            pageheap_bytes,
            mapped_small_bytes,
            small_active_span_bytes,
        }
    }
}

#[derive(Clone, Copy)]
enum TraceEvent {
    Alloc {
        ts: u64,
        cpu: u32,
        addr: u64,
        size: u64,
    },
    Free {
        ts: u64,
        cpu: u32,
        addr: u64,
    },
}

impl TraceEvent {
    fn ts(self) -> u64 {
        match self {
            Self::Alloc { ts, .. } | Self::Free { ts, .. } => ts,
        }
    }
}

fn parse_trace_line(line: &str, line_no: u64) -> Result<TraceEvent, String> {
    let parts: Vec<&str> = line.split_ascii_whitespace().collect();
    match parts.as_slice() {
        [ts, cpu, "alloc", addr, size] => Ok(TraceEvent::Alloc {
            ts: parse_u64_field(ts, line_no, "ts")?,
            cpu: parse_u32_field(cpu, line_no, "cpu")?,
            addr: parse_u64_field(addr, line_no, "addr")?,
            size: parse_u64_field(size, line_no, "size")?,
        }),
        [ts, cpu, "free", addr] => Ok(TraceEvent::Free {
            ts: parse_u64_field(ts, line_no, "ts")?,
            cpu: parse_u32_field(cpu, line_no, "cpu")?,
            addr: parse_u64_field(addr, line_no, "addr")?,
        }),
        _ => Err(format!("invalid trace line {line_no}: `{line}`")),
    }
}

fn parse_u64_field(input: &str, line_no: u64, field: &str) -> Result<u64, String> {
    input
        .parse()
        .map_err(|_| format!("invalid {field} on line {line_no}: `{input}`"))
}

fn parse_u32_field(input: &str, line_no: u64, field: &str) -> Result<u32, String> {
    input
        .parse()
        .map_err(|_| format!("invalid {field} on line {line_no}: `{input}`"))
}

#[derive(Clone)]
struct SizeClass {
    object_size: u64,
    span_bytes: u64,
    span_capacity: usize,
    slack_bytes: u64,
    life_bucket: LifeBucket,
}

struct SizeClassCatalog {
    classes: Vec<SizeClass>,
    by_size: HashMap<u64, usize>,
    page_size: u64,
    hugepage_size: u64,
    large_object_threshold: u64,
}

impl SizeClassCatalog {
    fn new(page_size: u64, hugepage_size: u64, large_object_threshold: u64) -> Self {
        Self {
            classes: Vec::new(),
            by_size: HashMap::new(),
            page_size,
            hugepage_size,
            large_object_threshold,
        }
    }

    fn class_for(&mut self, requested: u64) -> Option<usize> {
        if requested > self.large_object_threshold {
            return None;
        }

        let object_size = round_class_size(requested);
        if let Some(&id) = self.by_size.get(&object_size) {
            return Some(id);
        }

        let span_bytes = align_up(
            (object_size * 64).clamp(self.page_size, self.hugepage_size),
            self.page_size,
        );
        let span_capacity = (span_bytes / object_size).max(1) as usize;
        let slack_bytes = span_bytes - object_size * span_capacity as u64;
        let life_bucket = if span_capacity <= 16 {
            LifeBucket::Short
        } else {
            LifeBucket::Long
        };

        let id = self.classes.len();
        self.classes.push(SizeClass {
            object_size,
            span_bytes,
            span_capacity,
            slack_bytes,
            life_bucket,
        });
        self.by_size.insert(object_size, id);
        Some(id)
    }

    fn class(&self, class_id: usize) -> &SizeClass {
        &self.classes[class_id]
    }
}

fn round_class_size(requested: u64) -> u64 {
    match requested {
        0..=256 => align_up(requested.max(16), 16),
        257..=1024 => align_up(requested, 64),
        1025..=8192 => align_up(requested, 256),
        8193..=32768 => align_up(requested, 1024),
        _ => align_up(requested, 4096),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LifeBucket {
    Short,
    Long,
}

#[derive(Clone)]
struct Span {
    class_id: usize,
    object_size: u64,
    span_bytes: u64,
    capacity: usize,
    live_count: usize,
    per_cpu_cached: usize,
    transfer_cached: usize,
    central_cached: usize,
    slack_bytes: u64,
    hugepage_id: Option<usize>,
    active: bool,
    queue_version: u64,
}

impl Span {
    fn releasable(&self) -> bool {
        self.active
            && self.live_count == 0
            && self.per_cpu_cached == 0
            && self.transfer_cached == 0
            && self.central_cached == self.capacity
    }
}

#[derive(Clone, Copy)]
enum LiveAlloc {
    Small {
        span_id: usize,
        requested_size: u64,
        allocated_size: u64,
    },
    Large {
        requested_size: u64,
        allocated_size: u64,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AllocSource {
    PerCpu,
    Transfer,
    Central,
    PageHeap,
    Mmap,
}

impl AllocSource {
    fn index(self) -> usize {
        match self {
            Self::PerCpu => 0,
            Self::Transfer => 1,
            Self::Central => 2,
            Self::PageHeap => 3,
            Self::Mmap => 4,
        }
    }

    fn latency_ns(self) -> f64 {
        match self {
            Self::PerCpu => PER_CPU_LATENCY_NS,
            Self::Transfer => TRANSFER_LATENCY_NS,
            Self::Central => CENTRAL_LATENCY_NS,
            Self::PageHeap => PAGEHEAP_LATENCY_NS,
            Self::Mmap => MMAP_LATENCY_NS,
        }
    }
}

#[derive(Default)]
struct PerCpuCache {
    capacity_bytes: u64,
    used_bytes: u64,
    misses_in_interval: u64,
    pools: BTreeMap<usize, TokenPool>,
}

impl PerCpuCache {
    fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            used_bytes: 0,
            misses_in_interval: 0,
            pools: BTreeMap::new(),
        }
    }

    fn can_store(&self, object_size: u64) -> bool {
        self.used_bytes + object_size <= self.capacity_bytes
    }

    fn available_slots(&self, object_size: u64) -> usize {
        if self.used_bytes >= self.capacity_bytes {
            0
        } else {
            ((self.capacity_bytes - self.used_bytes) / object_size) as usize
        }
    }

    fn push_many(&mut self, class_id: usize, entries: Vec<TokenRun>, object_size: u64) {
        let added: usize = entries.iter().map(|entry| entry.count).sum();
        self.used_bytes += object_size * added as u64;
        self.pools.entry(class_id).or_default().push_many(entries);
    }

    fn pop_one(&mut self, class_id: usize, object_size: u64) -> Option<usize> {
        let pool = self.pools.get_mut(&class_id)?;
        let span_id = pool.pop_one()?;
        self.used_bytes -= object_size;
        if pool.total == 0 {
            self.pools.remove(&class_id);
        }
        Some(span_id)
    }

    fn pop_many(&mut self, class_id: usize, count: usize, object_size: u64) -> Vec<TokenRun> {
        let Some(pool) = self.pools.get_mut(&class_id) else {
            return Vec::new();
        };
        let entries = pool.pop_many(count);
        let removed: usize = entries.iter().map(|entry| entry.count).sum();
        self.used_bytes -= removed as u64 * object_size;
        if pool.total == 0 {
            self.pools.remove(&class_id);
        }
        entries
    }

    fn pool_len(&self, class_id: usize) -> usize {
        self.pools.get(&class_id).map_or(0, |pool| pool.total)
    }

    fn largest_nonempty_class(&self) -> Option<usize> {
        self.pools.iter().rev().find_map(|(class_id, pool)| {
            if pool.total > 0 {
                Some(*class_id)
            } else {
                None
            }
        })
    }
}

#[derive(Default)]
struct TokenPool {
    entries: VecDeque<TokenRun>,
    total: usize,
}

impl TokenPool {
    fn push_many(&mut self, entries: Vec<TokenRun>) {
        for entry in entries {
            self.push(entry.span_id, entry.count);
        }
    }

    fn push(&mut self, span_id: usize, count: usize) {
        if count == 0 {
            return;
        }
        self.total += count;
        if let Some(back) = self.entries.back_mut() {
            if back.span_id == span_id {
                back.count += count;
                return;
            }
        }
        self.entries.push_back(TokenRun { span_id, count });
    }

    fn pop_one(&mut self) -> Option<usize> {
        let entry = self.entries.front_mut()?;
        entry.count -= 1;
        self.total -= 1;
        let span_id = entry.span_id;
        if entry.count == 0 {
            self.entries.pop_front();
        }
        Some(span_id)
    }

    fn pop_many(&mut self, mut count: usize) -> Vec<TokenRun> {
        let mut out = Vec::new();
        while count > 0 {
            let Some(mut entry) = self.entries.pop_front() else {
                break;
            };
            let take = entry.count.min(count);
            count -= take;
            self.total -= take;
            out.push(TokenRun {
                span_id: entry.span_id,
                count: take,
            });
            entry.count -= take;
            if entry.count > 0 {
                self.entries.push_front(entry);
            }
        }
        out
    }
}

#[derive(Clone)]
struct TokenRun {
    span_id: usize,
    count: usize,
}

struct TransferStoreResult {
    stored: Vec<TokenRun>,
    overflow: Vec<TokenRun>,
}

struct TransferLayer {
    mode: TransferMode,
    cpus_per_domain: u32,
    per_cache_capacity_bytes: u64,
    local: Vec<TransferCache>,
    backing: TransferCache,
}

impl TransferLayer {
    fn new(mode: TransferMode, cpus_per_domain: u32, per_cache_capacity_bytes: u64) -> Self {
        Self {
            mode,
            cpus_per_domain,
            per_cache_capacity_bytes,
            local: Vec::new(),
            backing: TransferCache::new(per_cache_capacity_bytes),
        }
    }

    fn domain_for(&self, cpu: u32) -> usize {
        (cpu / self.cpus_per_domain) as usize
    }

    fn ensure_domain(&mut self, cpu: u32) {
        if self.mode != TransferMode::Nuca {
            return;
        }
        let domain = self.domain_for(cpu);
        while self.local.len() <= domain {
            self.local
                .push(TransferCache::new(self.per_cache_capacity_bytes));
        }
    }

    fn take(
        &mut self,
        cpu: u32,
        class_id: usize,
        count: usize,
        backing: bool,
        object_size: u64,
    ) -> (Vec<TokenRun>, bool) {
        match self.mode {
            TransferMode::Global => {
                if backing {
                    return (Vec::new(), false);
                }
                let entries = self.backing.take(class_id, count, object_size);
                let remote = !entries.is_empty();
                (entries, remote)
            }
            TransferMode::Nuca => {
                self.ensure_domain(cpu);
                if backing {
                    let entries = self.backing.take(class_id, count, object_size);
                    let remote = !entries.is_empty();
                    (entries, remote)
                } else {
                    let domain = self.domain_for(cpu);
                    let entries = self.local[domain].take(class_id, count, object_size);
                    (entries, false)
                }
            }
        }
    }

    fn store(
        &mut self,
        cpu: u32,
        class_id: usize,
        entries: Vec<TokenRun>,
        object_size: u64,
    ) -> TransferStoreResult {
        match self.mode {
            TransferMode::Global => {
                let (stored, overflow) = self.backing.store(class_id, entries, object_size);
                TransferStoreResult { stored, overflow }
            }
            TransferMode::Nuca => {
                self.ensure_domain(cpu);
                let domain = self.domain_for(cpu);
                let (local_stored, local_overflow) =
                    self.local[domain].store(class_id, entries, object_size);
                let (backing_stored, overflow) =
                    self.backing.store(class_id, local_overflow, object_size);
                let mut stored = local_stored;
                stored.extend(backing_stored);
                TransferStoreResult { stored, overflow }
            }
        }
    }
}

struct TransferCache {
    capacity_bytes: u64,
    used_bytes: u64,
    pools: BTreeMap<usize, TokenPool>,
}

impl TransferCache {
    fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            used_bytes: 0,
            pools: BTreeMap::new(),
        }
    }

    fn take(&mut self, class_id: usize, count: usize, object_size: u64) -> Vec<TokenRun> {
        let Some(pool) = self.pools.get_mut(&class_id) else {
            return Vec::new();
        };
        let entries = pool.pop_many(count);
        if pool.total == 0 {
            self.pools.remove(&class_id);
        }
        let removed: usize = entries.iter().map(|entry| entry.count).sum();
        self.used_bytes -= removed as u64 * object_size;
        entries
    }

    fn store(
        &mut self,
        class_id: usize,
        entries: Vec<TokenRun>,
        object_size: u64,
    ) -> (Vec<TokenRun>, Vec<TokenRun>) {
        let free_slots = if self.used_bytes >= self.capacity_bytes {
            0
        } else {
            ((self.capacity_bytes - self.used_bytes) / object_size) as usize
        };

        let mut stored = Vec::new();
        let mut overflow = Vec::new();
        let mut remaining_slots = free_slots;

        for entry in entries {
            if remaining_slots == 0 {
                overflow.push(entry);
                continue;
            }

            let take = entry.count.min(remaining_slots);
            if take > 0 {
                stored.push(TokenRun {
                    span_id: entry.span_id,
                    count: take,
                });
                remaining_slots -= take;
                self.used_bytes += take as u64 * object_size;
            }
            if entry.count > take {
                overflow.push(TokenRun {
                    span_id: entry.span_id,
                    count: entry.count - take,
                });
            }
        }

        if !stored.is_empty() {
            self.pools
                .entry(class_id)
                .or_default()
                .push_many(stored.clone());
        }

        (stored, overflow)
    }
}

struct CentralFreeList {
    lists: usize,
    prioritize: bool,
    classes: HashMap<usize, CentralClass>,
}

impl CentralFreeList {
    fn new(lists: usize, prioritize: bool) -> Self {
        Self {
            lists,
            prioritize,
            classes: HashMap::new(),
        }
    }

    fn class_mut(&mut self, class_id: usize) -> &mut CentralClass {
        self.classes
            .entry(class_id)
            .or_insert_with(|| CentralClass::new(self.lists))
    }

    fn requeue(&mut self, class_id: usize, span_id: usize, spans: &[Span]) {
        let bucket = span_bucket(&spans[span_id], self.lists);
        let prioritize = self.prioritize;
        let class = self.class_mut(class_id);
        class.push(span_id, bucket, prioritize, spans);
    }

    fn pop_span(&mut self, class_id: usize, spans: &[Span]) -> Option<usize> {
        let prioritize = self.prioritize;
        self.class_mut(class_id).pop(prioritize, spans)
    }

    fn invalidate(&mut self, class_id: usize, span_id: usize) {
        let class = self.class_mut(class_id);
        class.invalidated.push(span_id);
    }
}

struct CentralClass {
    fifo: VecDeque<QueueEntry>,
    buckets: Vec<VecDeque<QueueEntry>>,
    invalidated: Vec<usize>,
}

impl CentralClass {
    fn new(lists: usize) -> Self {
        Self {
            fifo: VecDeque::new(),
            buckets: (0..lists).map(|_| VecDeque::new()).collect(),
            invalidated: Vec::new(),
        }
    }

    fn push(&mut self, span_id: usize, bucket: usize, prioritize: bool, spans: &[Span]) {
        let version = spans[span_id].queue_version.wrapping_add(1);
        if prioritize {
            self.buckets[bucket].push_back(QueueEntry { span_id, version });
        } else {
            self.fifo.push_back(QueueEntry { span_id, version });
        }
    }

    fn pop(&mut self, prioritize: bool, spans: &[Span]) -> Option<usize> {
        if prioritize {
            for bucket in &mut self.buckets {
                while let Some(entry) = bucket.pop_front() {
                    if queue_entry_valid(entry, spans) {
                        return Some(entry.span_id);
                    }
                }
            }
            None
        } else {
            while let Some(entry) = self.fifo.pop_front() {
                if queue_entry_valid(entry, spans) {
                    return Some(entry.span_id);
                }
            }
            None
        }
    }
}

#[derive(Clone, Copy)]
struct QueueEntry {
    span_id: usize,
    version: u64,
}

fn queue_entry_valid(entry: QueueEntry, spans: &[Span]) -> bool {
    let span = &spans[entry.span_id];
    span.active && span.central_cached > 0 && span.queue_version.wrapping_add(1) == entry.version
}

fn span_bucket(span: &Span, lists: usize) -> usize {
    if span.live_count == 0 {
        return lists - 1;
    }
    let level = usize::BITS as usize - 1 - span.live_count.leading_zeros() as usize;
    lists.saturating_sub(level + 1)
}

struct PageHeap {
    page_size: u64,
    hugepage_size: u64,
    lifetime_aware: bool,
    release_empty_hugepages: bool,
    hugepages: Vec<Hugepage>,
    large_free_runs: BTreeMap<u64, usize>,
    mapped_small_bytes: u64,
    mapped_large_bytes: u64,
    free_large_bytes: u64,
}

impl PageHeap {
    fn new(
        page_size: u64,
        hugepage_size: u64,
        lifetime_aware: bool,
        _lifetime_threshold_capacity: usize,
        release_empty_hugepages: bool,
    ) -> Self {
        Self {
            page_size,
            hugepage_size,
            lifetime_aware,
            release_empty_hugepages,
            hugepages: Vec::new(),
            large_free_runs: BTreeMap::new(),
            mapped_small_bytes: 0,
            mapped_large_bytes: 0,
            free_large_bytes: 0,
        }
    }

    fn allocate_span(&mut self, class: &SizeClass) -> Result<(usize, AllocSource), String> {
        let target_bucket = if self.lifetime_aware {
            Some(class.life_bucket)
        } else {
            None
        };

        if let Some((id, _)) = self
            .hugepages
            .iter()
            .enumerate()
            .filter(|(_, hp)| hp.active && hp.free_bytes >= class.span_bytes)
            .filter(|(_, hp)| target_bucket.is_none() || hp.life_bucket == target_bucket)
            .max_by_key(|(_, hp)| hp.used_bytes)
        {
            let hp = &mut self.hugepages[id];
            hp.free_bytes -= class.span_bytes;
            hp.used_bytes += class.span_bytes;
            hp.live_spans += 1;
            return Ok((id, AllocSource::PageHeap));
        }

        let id = self.hugepages.len();
        self.hugepages.push(Hugepage {
            active: true,
            free_bytes: self.hugepage_size - class.span_bytes,
            used_bytes: class.span_bytes,
            live_spans: 1,
            life_bucket: target_bucket,
        });
        self.mapped_small_bytes += self.hugepage_size;
        Ok((id, AllocSource::Mmap))
    }

    fn release_span(&mut self, hugepage_id: usize, span_bytes: u64) -> bool {
        let hp = &mut self.hugepages[hugepage_id];
        hp.free_bytes += span_bytes;
        hp.used_bytes -= span_bytes;
        hp.live_spans -= 1;
        if self.release_empty_hugepages && hp.live_spans == 0 {
            hp.active = false;
            self.mapped_small_bytes -= self.hugepage_size;
            return true;
        }
        false
    }

    fn alloc_large(&mut self, requested: u64) -> AllocSource {
        let alloc_bytes = align_up(requested, self.page_size);
        if let Some(count) = self.large_free_runs.get_mut(&alloc_bytes) {
            if *count > 0 {
                *count -= 1;
                self.free_large_bytes -= alloc_bytes;
                if *count == 0 {
                    self.large_free_runs.remove(&alloc_bytes);
                }
                return AllocSource::PageHeap;
            }
        }

        self.mapped_large_bytes += alloc_bytes;
        AllocSource::Mmap
    }

    fn free_large(&mut self, alloc_bytes: u64) {
        if self.release_empty_hugepages {
            self.mapped_large_bytes -= alloc_bytes;
            return;
        }
        *self.large_free_runs.entry(alloc_bytes).or_insert(0) += 1;
        self.free_large_bytes += alloc_bytes;
    }

    fn active_hugepages(&self) -> usize {
        self.hugepages.iter().filter(|hp| hp.active).count()
    }

    fn active_span_bytes(&self) -> u64 {
        self.hugepages
            .iter()
            .filter(|hp| hp.active)
            .map(|hp| hp.used_bytes)
            .sum()
    }

    fn external_bytes(&self) -> u64 {
        let small_free: u64 = self
            .hugepages
            .iter()
            .filter(|hp| hp.active)
            .map(|hp| hp.free_bytes)
            .sum();
        small_free + self.free_large_bytes
    }
}

struct Hugepage {
    active: bool,
    free_bytes: u64,
    used_bytes: u64,
    live_spans: usize,
    life_bucket: Option<LifeBucket>,
}

#[derive(Default)]
struct Stats {
    alloc_events: u64,
    free_events: u64,
    total_requested_bytes: u64,
    live_requested_bytes: u64,
    live_allocated_bytes: u64,
    per_cpu_bytes: u64,
    transfer_bytes: u64,
    central_bytes: u64,
    alloc_path_counts: [u64; 5],
    free_path_counts: [u64; 5],
    alloc_latency_ns: f64,
    free_latency_ns: f64,
    per_cpu_misses: u64,
    transfer_local_hits: u64,
    transfer_backing_hits: u64,
    remote_transfer_allocs: u64,
    spans_created: u64,
    spans_released: u64,
    hugepages_mapped: u64,
    hugepages_released: u64,
    direct_large_allocs: u64,
    resize_passes: u64,
    rebalanced_bytes: u64,
    peak_live_requested_bytes: u64,
    peak_footprint_bytes: u64,
    peak_internal_fragmentation_bytes: u64,
    peak_external_fragmentation_bytes: u64,
}

struct Snapshot {
    live_requested_bytes: u64,
    footprint_bytes: u64,
    internal_fragmentation_bytes: u64,
    external_fragmentation_bytes: u64,
    per_cpu_bytes: u64,
    transfer_bytes: u64,
    central_bytes: u64,
    pageheap_bytes: u64,
    mapped_small_bytes: u64,
    small_active_span_bytes: u64,
}

fn take_one(entries: &mut Vec<TokenRun>) -> Option<usize> {
    while let Some(first) = entries.first_mut() {
        if first.count == 0 {
            entries.remove(0);
            continue;
        }
        first.count -= 1;
        let span_id = first.span_id;
        if first.count == 0 {
            entries.remove(0);
        }
        return Some(span_id);
    }
    None
}

fn align_up(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (value + (align - 1)) & !(align - 1)
}

fn average(total: f64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

fn pct_of(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * value as f64 / total as f64
    }
}

fn pct_bytes(value: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * value as f64 / total as f64
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{bytes}{}", UNITS[idx])
    } else {
        format!("{value:.2}{}", UNITS[idx])
    }
}

fn on_off(value: bool) -> &'static str {
    if value {
        "on"
    } else {
        "off"
    }
}

trait ConfigExt {
    fn transfer_mode_name(&self) -> &'static str;
}

impl ConfigExt for Config {
    fn transfer_mode_name(&self) -> &'static str {
        match self.transfer_mode {
            TransferMode::Global => "global",
            TransferMode::Nuca => "nuca",
        }
    }
}
