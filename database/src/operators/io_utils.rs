#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unreachable_code
)]

use crate::buffer_pool::BufferPoolManager;
use crate::data::{Row, Value};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Write};

pub(crate) struct Fnv1aHasher(pub u64);
impl Default for Fnv1aHasher {
    #[inline(always)]
    fn default() -> Self {
        Self(0xcbf29ce484222325)
    }
}
impl Hasher for Fnv1aHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

pub(crate) struct AlternateHasher(pub u64);
impl Default for AlternateHasher {
    #[inline(always)]
    fn default() -> Self {
        Self(0x100000001b3)
    }
}
impl Hasher for AlternateHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(0xcbf29ce484222325);
        }
    }
}

pub(crate) type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<Fnv1aHasher>>;

pub(crate) struct BloomFilter {
    pub bits: Vec<u8>,
    pub num_bits: u64,
}
impl BloomFilter {
    pub(crate) fn new(size_bytes: usize) -> Self {
        Self {
            bits: vec![0; size_bytes],
            num_bits: (size_bytes as u64) * 8,
        }
    }
    #[inline(always)]
    pub(crate) fn insert(&mut self, h1: u64, h2: u64) {
        let idx1 = (h1 % self.num_bits) as usize;
        let idx2 = (h2 % self.num_bits) as usize;
        self.bits[idx1 / 8] |= 1 << (idx1 % 8);
        self.bits[idx2 / 8] |= 1 << (idx2 % 8);
    }
    #[inline(always)]
    pub(crate) fn contains(&self, h1: u64, h2: u64) -> bool {
        let idx1 = (h1 % self.num_bits) as usize;
        let idx2 = (h2 % self.num_bits) as usize;
        (self.bits[idx1 / 8] & (1 << (idx1 % 8))) != 0
            && (self.bits[idx2 / 8] & (1 << (idx2 % 8))) != 0
    }
}

pub(crate) struct ScratchRun {
    pub block_ids: Vec<u64>,
    pub total_bytes: usize,
}

pub(crate) struct SharedBufferManager<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_size: usize,
    partition_buffers: Vec<Vec<u8>>,
    partition_block_ids: Vec<Vec<u64>>,
    partition_byte_counts: Vec<usize>,
    total_buffered_bytes: usize,
    flush_threshold: usize,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}

impl<'a, R: Read, W: Write> SharedBufferManager<'a, R, W> {
    pub(crate) fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        num_partitions: usize,
        block_size: usize,
    ) -> Self {
        Self {
            pool_ptr,
            block_size,
            partition_buffers: vec![Vec::new(); num_partitions],
            partition_block_ids: vec![Vec::new(); num_partitions],
            partition_byte_counts: vec![0; num_partitions],
            total_buffered_bytes: 0,
            flush_threshold: 8 * 1024 * 1024,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn write_row(&mut self, p_idx: usize, row: &Row) {
        let encoded = row.encode();
        let row_len = (encoded.len() as u32).to_le_bytes();
        self.partition_buffers[p_idx].extend_from_slice(&row_len);
        self.partition_buffers[p_idx].extend_from_slice(&encoded);
        let bytes_added = 4 + encoded.len();
        self.partition_byte_counts[p_idx] += bytes_added;
        self.total_buffered_bytes += bytes_added;

        if self.total_buffered_bytes >= self.flush_threshold {
            self.flush_largest();
        }
    }

    fn flush_largest(&mut self) {
        let (p_idx, _) = self
            .partition_buffers
            .iter()
            .enumerate()
            .max_by_key(|(_, b)| b.len())
            .unwrap();
        let buf = &mut self.partition_buffers[p_idx];
        let num_blocks = buf.len() / self.block_size;
        if num_blocks == 0 {
            return;
        }
        let pool = unsafe { &mut *self.pool_ptr };
        let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
        let flush_bytes = num_blocks * self.block_size;
        pool.write_pages_direct(start_block, num_blocks, &buf[0..flush_bytes]);
        for i in 0..num_blocks {
            self.partition_block_ids[p_idx].push(start_block + i as u64);
        }
        self.total_buffered_bytes -= flush_bytes;
        buf.drain(0..flush_bytes);
    }

    pub(crate) fn finish(mut self) -> Vec<ScratchRun> {
        for i in 0..self.partition_buffers.len() {
            if !self.partition_buffers[i].is_empty() {
                let pool = unsafe { &mut *self.pool_ptr };
                let buf = &mut self.partition_buffers[i];
                let num_blocks = (buf.len() + self.block_size - 1) / self.block_size;
                let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
                buf.resize(num_blocks * self.block_size, 0);
                pool.write_pages_direct(start_block, num_blocks, buf);
                for j in 0..num_blocks {
                    self.partition_block_ids[i].push(start_block + j as u64);
                }
            }
        }
        self.partition_block_ids
            .into_iter()
            .zip(self.partition_byte_counts.into_iter())
            .map(|(ids, bytes)| ScratchRun {
                block_ids: ids,
                total_bytes: bytes,
            })
            .collect()
    }
}

pub(crate) struct ScratchRunWriter<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_size: usize,
    current_blocks: Vec<u8>,
    offset: usize,
    block_ids: Vec<u64>,
    total_bytes: usize,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}
impl<'a, R: Read, W: Write> ScratchRunWriter<'a, R, W> {
    pub(crate) fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_size: usize,
        capacity_blocks: usize,
    ) -> Self {
        Self {
            pool_ptr,
            block_size,
            current_blocks: vec![0; block_size * capacity_blocks],
            offset: 0,
            block_ids: Vec::new(),
            total_bytes: 0,
            _marker: std::marker::PhantomData,
        }
    }
    pub(crate) fn write_row(&mut self, row: &Row) {
        let encoded = row.encode();
        let row_len = u32::try_from(encoded.len()).unwrap();
        self.write_bytes(&row_len.to_le_bytes());
        self.write_bytes(&encoded);
    }
    fn write_bytes(&mut self, mut bytes: &[u8]) {
        self.total_bytes += bytes.len();
        while !bytes.is_empty() {
            if self.offset == self.current_blocks.len() {
                self.flush_buffer();
            }
            let writable = (self.current_blocks.len() - self.offset).min(bytes.len());
            self.current_blocks[self.offset..self.offset + writable]
                .copy_from_slice(&bytes[..writable]);
            self.offset += writable;
            bytes = &bytes[writable..];
        }
    }
    fn flush_buffer(&mut self) {
        if self.offset == 0 {
            return;
        }
        let pool = unsafe { &mut *self.pool_ptr };
        let num_blocks = (self.offset + self.block_size - 1) / self.block_size;
        let start_block = pool.disk_manager.allocate_anon_blocks(num_blocks as u64);
        let valid_len = self.offset;
        let target_len = num_blocks * self.block_size;
        self.current_blocks[valid_len..target_len].fill(0);
        pool.write_pages_direct(start_block, num_blocks, &self.current_blocks[0..target_len]);
        for i in 0..num_blocks {
            self.block_ids.push(start_block + (i as u64));
        }
        self.offset = 0;
    }
    pub(crate) fn finish(mut self) -> ScratchRun {
        self.flush_buffer();
        ScratchRun {
            block_ids: self.block_ids,
            total_bytes: self.total_bytes,
        }
    }
}

pub(crate) struct ScratchRunReader<'a, R: Read, W: Write> {
    pool_ptr: *mut BufferPoolManager<R, W>,
    block_ids: Vec<u64>,
    total_bytes: usize,
    bytes_read: usize,
    block_idx: usize,
    current_block: Vec<u8>,
    offset: usize,
    loaded: bool,
    free_on_drop: bool,
    _marker: std::marker::PhantomData<&'a mut BufferPoolManager<R, W>>,
}
impl<'a, R: Read, W: Write> ScratchRunReader<'a, R, W> {
    pub(crate) fn new(
        pool_ptr: *mut BufferPoolManager<R, W>,
        block_ids: Vec<u64>,
        total_bytes: usize,
        block_size: usize,
        free_on_drop: bool,
    ) -> Self {
        Self {
            pool_ptr,
            block_ids,
            total_bytes,
            bytes_read: 0,
            block_idx: 0,
            current_block: vec![0; block_size],
            offset: 0,
            loaded: false,
            free_on_drop,
            _marker: std::marker::PhantomData,
        }
    }
    pub fn has_more(&self) -> bool {
        self.bytes_read < self.total_bytes
    }

    pub(crate) fn read_row(&mut self) -> Option<Row> {
        if self.bytes_read >= self.total_bytes {
            return None;
        }
        let len_bytes = self.read_exact(4)?;
        let row_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        let row_bytes = self.read_exact(row_len)?;
        Row::decode(&row_bytes)
    }

    fn read_exact(&mut self, len: usize) -> Option<Vec<u8>> {
        if self.bytes_read + len > self.total_bytes {
            return None;
        }
        let mut out = vec![0; len];
        let mut written = 0;
        while written < len {
            self.ensure_block_loaded()?;
            let available = self.current_block.len() - self.offset;
            let to_copy = (len - written).min(available);
            out[written..written + to_copy]
                .copy_from_slice(&self.current_block[self.offset..self.offset + to_copy]);
            self.offset += to_copy;
            self.bytes_read += to_copy;
            written += to_copy;
        }
        Some(out)
    }

    fn ensure_block_loaded(&mut self) -> Option<()> {
        if !self.loaded || self.offset == self.current_block.len() {
            let start_block_id = *self.block_ids.get(self.block_idx)?;
            let mut num_blocks = 1;

            while self.block_idx + num_blocks < self.block_ids.len() && num_blocks < 8 {
                if self.block_ids[self.block_idx + num_blocks]
                    == start_block_id + (num_blocks as u64)
                {
                    num_blocks += 1;
                } else {
                    break;
                }
            }

            let pool = unsafe { &mut *self.pool_ptr };
            self.current_block = pool.read_pages_direct(start_block_id, num_blocks);
            self.block_idx += num_blocks;
            self.offset = 0;
            self.loaded = true;
        }
        Some(())
    }
}
impl<'a, R: Read, W: Write> Drop for ScratchRunReader<'a, R, W> {
    fn drop(&mut self) {
        if self.free_on_drop {
            let pool = unsafe { &mut *self.pool_ptr };
            for &id in &self.block_ids {
                pool.disk_manager.free_anon_block(id);
            }
        }
    }
}
