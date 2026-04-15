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

struct HeapEntry {
    row: Row,
    run_idx: usize,
    sort_indices: Vec<(usize, bool)>,
}
impl HeapEntry {
    fn new(row: Row, run_idx: usize, sort_indices: &[(usize, bool)]) -> Self {
        Self {
            row,
            run_idx,
            sort_indices: sort_indices.to_vec(),
        }
    }
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
    fn eq(&self, other: &Self) -> bool {
        self.run_idx == other.run_idx
            && Self::compare_rows(&self.sort_indices, &self.row, &other.row) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match Self::compare_rows(&self.sort_indices, &self.row, &other.row) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => other.run_idx.cmp(&self.run_idx),
        }
    }
}

pub struct SortOperator<'a, R: Read, W: Write> {
    child: Box<dyn Operator + 'a>,
    sort_indices: Vec<(usize, bool)>,
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
            sort_indices,
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
    fn estimate_row_size(row: &Row) -> usize {
        std::mem::size_of::<Row>()
            + row
                .values
                .iter()
                .map(|v| match v {
                    Value::String(s) => std::mem::size_of::<Value>() + s.len(),
                    _ => std::mem::size_of::<Value>(),
                })
                .sum::<usize>()
    }
    fn merge_fan_in(&self) -> usize {
        128
    }
    fn merge_runs_batch(&self, runs: Vec<ScratchRun>) -> ScratchRun {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        for (idx, run) in runs.into_iter().enumerate() {
            let mut reader = ScratchRunReader::new(
                self.scratch_pool_ptr,
                run.block_ids,
                run.total_bytes,
                self.scratch_block_size,
                true,
            );
            if let Some(row) = reader.read_row() {
                heap.push(HeapEntry::new(row, idx, &self.sort_indices));
            }
            readers.push(reader);
        }
        let mut writer = ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 1024);
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
}
impl<'a, R: Read, W: Write> Operator for SortOperator<'a, R, W> {
    fn next(&mut self) -> Option<Row> {
        if !self.initialized {
            let mut run = Vec::new();
            let mut mem = 0;
            while let Some(row) = self.child.next() {
                mem += Self::estimate_row_size(&row);
                run.push(row);
                if mem >= self.memory_limit_bytes {
                    self.is_external = true;
                    run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                    let mut writer =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 1024);
                    for r in &run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());
                    run = Vec::new();
                    mem = 0;
                }
            }
            if !run.is_empty() {
                run.sort_by(|a, b| HeapEntry::compare_rows(&self.sort_indices, a, b));
                if self.is_external {
                    let mut writer =
                        ScratchRunWriter::new(self.scratch_pool_ptr, self.scratch_block_size, 1024);
                    for r in &run {
                        writer.write_row(r);
                    }
                    self.runs.push(writer.finish());
                } else {
                    self.in_memory_rows = run.into_iter();
                }
            }
            if self.is_external {
                self.collapse_runs();
                for run in self.runs.drain(..) {
                    let mut reader = ScratchRunReader::new(
                        self.scratch_pool_ptr,
                        run.block_ids,
                        run.total_bytes,
                        self.scratch_block_size,
                        true,
                    );
                    if let Some(r) = reader.read_row() {
                        self.merge_heap.push(HeapEntry::new(
                            r,
                            self.run_readers.len(),
                            &self.sort_indices,
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
