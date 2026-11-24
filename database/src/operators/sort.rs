#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unreachable_code
)]

use super::Operator;
use super::io_utils::{ScratchRun, ScratchRunReader, ScratchRunWriter};
use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{Read, Write};
use std::rc::Rc;

const SCRATCH_WRITER_TARGET_BYTES: usize = 1 * 1024 * 1024;
const MIN_MERGE_FAN_IN: usize = 2;

#[inline(always)]
fn scratch_writer_blocks(block_size: usize) -> usize {
    ((SCRATCH_WRITER_TARGET_BYTES + block_size - 1) / block_size).max(1)
}

struct HeapEntry {
    row: Row,
    run_idx: usize,
    sort_indices: Rc<[(usize, bool)]>,
}

impl HeapEntry {
    #[inline(always)]
    fn new(row: Row, run_idx: usize, sort_indices: Rc<[(usize, bool)]>) -> Self {
        Self {
            row,
            run_idx,
            sort_indices,
        }
    }

    #[inline(always)]
    fn compare_rows(sort_indices: &[(usize, bool)], a: &Row, b: &Row) -> Ordering {
        for &(idx, ascending) in sort_indices {
            let cmp = a.values[idx]
                .partial_cmp(&b.values[idx])
                .unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return if ascending { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    }
}

impl PartialEq for HeapEntry {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx
            && Self::compare_rows(&self.sort_indices, &self.row, &other.row) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        match Self::compare_rows(&self.sort_indices, &self.row, &other.row) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => other.run_idx.cmp(&self.run_idx),
        }
    }
}

struct RunGenEntry {
    row: Row,
    sort_indices: Rc<[(usize, bool)]>,
    frozen: bool,
}

impl RunGenEntry {
    #[inline(always)]
    fn new(row: Row, sort_indices: Rc<[(usize, bool)]>, frozen: bool) -> Self {
        Self {
            row,
            sort_indices,
            frozen,
        }
    }
}

impl PartialEq for RunGenEntry {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        HeapEntry::compare_rows(&self.sort_indices, &self.row, &other.row) == Ordering::Equal
    }
}

impl Eq for RunGenEntry {}

impl PartialOrd for RunGenEntry {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RunGenEntry {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.frozen, other.frozen) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            _ => match HeapEntry::compare_rows(&self.sort_indices, &self.row, &other.row) {
                Ordering::Less => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
                Ordering::Equal => Ordering::Equal,
            },
        }
    }
}

pub struct SortOperator<'a, R: Read, W: Write> {
    child: Box<dyn Operator + 'a>,
    sort_indices: Rc<[(usize, bool)]>,
    memory_limit_bytes: usize,
    initialized: bool,
    in_memory_rows: std::vec::IntoIter<Row>,
    is_external: bool,
    scratch_pool_ptr: *mut BufferPoolManager<R, W>,
    scratch_block_size: usize,
    runs: Vec<ScratchRun>,
    run_readers: Vec<ScratchRunReader<'a, R, W>>,
    merge_heap: BinaryHeap<HeapEntry>,
}

// 🚀 CRITICAL DISK LEAK FIX: Free intermediate runs if dropped early
impl<'a, R: Read, W: Write> Drop for SortOperator<'a, R, W> {
    fn drop(&mut self) {
        let pool = unsafe { &mut *self.scratch_pool_ptr };
        for run in &self.runs {
            for span in &run.spans {
                for i in 0..span.num_blocks {
                    pool.disk_manager.free_anon_block(span.start_block + (i as u64));
                }
            }
        }
    }
}

impl<'a, R: Read, W: Write> SortOperator<'a, R, W> {
    pub fn new(
        child: Box<dyn Operator + 'a>,
        sort_indices: Vec<(usize, bool)>,
        memory_limit_bytes: usize,
        scratch_pool_ptr: *mut BufferPoolManager<R, W>,
        scratch_block_size: usize,
    ) -> Self {
        Self {
            child,
            sort_indices: sort_indices.into(),
            memory_limit_bytes,
            initialized: false,
            in_memory_rows: Vec::new().into_iter(),
            is_external: false,
            scratch_pool_ptr,
            scratch_block_size,
            runs: Vec::new(),
            run_readers: Vec::new(),
            merge_heap: BinaryHeap::new(),
        }
    }

    #[inline(always)]
    fn estimate_row_size(&self, row: &Row) -> usize {
        row.encoded_len() + 64
    }

    #[inline(always)]
    fn merge_fan_in(&self) -> usize {
        let per_reader = crate::operators::io_utils::SCRATCH_READER_TARGET_BYTES
            .max(self.scratch_block_size)
            .max(1);
        let by_budget = (self.memory_limit_bytes / per_reader).max(MIN_MERGE_FAN_IN);
        by_budget.min(64)
    }

    fn merge_runs_batch(&self, runs: Vec<ScratchRun>) -> ScratchRun {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::with_capacity(runs.len());
        for (idx, run) in runs.into_iter().enumerate() {
            let mut reader = ScratchRunReader::new(
                self.scratch_pool_ptr,
                run.spans,
                run.total_bytes,
                self.scratch_block_size,
                true,
            );
            if let Some(row) = reader.read_row() {
                heap.push(HeapEntry::new(row, idx, Rc::clone(&self.sort_indices)));
            }
            readers.push(reader);
        }
        let mut writer = ScratchRunWriter::new(
            self.scratch_pool_ptr,
            self.scratch_block_size,
            scratch_writer_blocks(self.scratch_block_size),
        );
        while let Some(mut top) = heap.peek_mut() {
            writer.write_row(&top.row);
            if let Some(next) = readers[top.run_idx].read_row() {
                top.row = next;
            } else {
                std::collections::binary_heap::PeekMut::pop(top);
            }
        }
        writer.finish()
    }

    fn collapse_runs(&mut self) {
        let fan_in = self.merge_fan_in();
        while self.runs.len() > fan_in {
            let pending = std::mem::take(&mut self.runs);
            let mut iter = pending.into_iter();
            loop {
                let chunk: Vec<_> = iter.by_ref().take(fan_in).collect();
                if chunk.is_empty() {
                    break;
                }
                if chunk.len() == 1 {
                    self.runs.push(chunk.into_iter().next().unwrap());
                } else {
                    self.runs.push(self.merge_runs_batch(chunk));
                }
            }
        }
    }

    #[inline(always)]
    fn non_decreasing(&self, last: &Row, candidate: &Row) -> bool {
        HeapEntry::compare_rows(&self.sort_indices, last, candidate) != Ordering::Greater
    }
}

impl<'a, R: Read, W: Write> Operator for SortOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut buf = Vec::new();
            let mut mem = 0usize;
            let safe_threshold = (self.memory_limit_bytes / 3).max(256 * 1024);

            while mem < safe_threshold {
                match self.child.next() {
                    Some(row) => {
                        mem += self.estimate_row_size(&row);
                        buf.push(row);
                    }
                    None => break,
                }
            }

            let mut pending_row = self.child.next();

            if pending_row.is_none() {
                if !buf.is_empty() {
                    buf.sort_unstable_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                    self.in_memory_rows = buf.into_iter();
                }
                self.initialized = true;
                return self.in_memory_rows.next();
            }

            self.is_external = true;

            let mut heap: BinaryHeap<RunGenEntry> = BinaryHeap::with_capacity(buf.len() + 1);
            for row in buf.drain(..) {
                heap.push(RunGenEntry::new(row, Rc::clone(&self.sort_indices), false));
            }
            heap.push(RunGenEntry::new(
                pending_row.take().unwrap(),
                Rc::clone(&self.sort_indices),
                false,
            ));

            let mut writer = ScratchRunWriter::new(
                self.scratch_pool_ptr,
                self.scratch_block_size,
                scratch_writer_blocks(self.scratch_block_size),
            );

            loop {
                if heap.peek().map(|e| e.frozen).unwrap_or(true) {
                    let run = writer.finish();
                    if run.total_bytes > 0 {
                        self.runs.push(run);
                    }
                    if heap.is_empty() {
                        break;
                    }

                    let mut v = heap.into_vec();
                    for e in &mut v {
                        e.frozen = false;
                    }
                    heap = BinaryHeap::from(v);
                    writer = ScratchRunWriter::new(
                        self.scratch_pool_ptr,
                        self.scratch_block_size,
                        scratch_writer_blocks(self.scratch_block_size),
                    );
                    continue;
                }

                let entry = heap.pop().unwrap();
                let row = entry.row;
                writer.write_row(&row);

                if let Some(next_row) = self.child.next() {
                    let frozen_flag = !self.non_decreasing(&row, &next_row);
                    heap.push(RunGenEntry::new(
                        next_row,
                        Rc::clone(&self.sort_indices),
                        frozen_flag,
                    ));
                }
            }

            if self.is_external {
                self.collapse_runs();
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        run.spans,
                        run.total_bytes,
                        self.scratch_block_size,
                        true,
                    );
                    if let Some(r) = reader.read_row() {
                        self.merge_heap.push(HeapEntry::new(
                            r,
                            self.run_readers.len(),
                            Rc::clone(&self.sort_indices),
                        ));
                    }
                    self.run_readers.push(reader);
                }
            }
            self.initialized = true;
        }

        if !self.is_external {
            return self.in_memory_rows.next();
        }

        if let Some(mut top) = self.merge_heap.peek_mut() {
            if let Some(next) = self.run_readers[top.run_idx].read_row() {
                return Some(std::mem::replace(&mut top.row, next));
            } else {
                return Some(std::collections::binary_heap::PeekMut::pop(top).row);
            }
        }
        None
    }
}