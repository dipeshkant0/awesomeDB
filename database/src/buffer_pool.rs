#![allow(dead_code)]
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};

pub struct DiskManager<R: Read, W: Write> {
    disk_in: BufReader<R>,
    disk_out: W,
    pub block_size: usize,
    pub anon_start_block: u64,
    next_free_anon_block: u64,
    free_blocks: Vec<u64>,
}

impl<R: Read, W: Write> DiskManager<R, W> {
    pub fn new(mut disk_in: BufReader<R>, mut disk_out: W) -> Self {
        disk_out.write_all(b"get block-size\n").unwrap();
        disk_out.flush().unwrap();
        let mut line = String::new();
        disk_in.read_line(&mut line).unwrap();
        let block_size: usize = line.trim().parse().unwrap();

        disk_out.write_all(b"get anon-start-block\n").unwrap();
        disk_out.flush().unwrap();
        line.clear();
        disk_in.read_line(&mut line).unwrap();
        let anon_start_block: u64 = line.trim().parse().unwrap();

        Self {
            disk_in,
            disk_out,
            block_size,
            anon_start_block,
            next_free_anon_block: anon_start_block,
            free_blocks: Vec::new(),
        }
    }

    pub fn read_page(&mut self, block_id: u64, buffer: &mut [u8]) {
        write!(self.disk_out, "get block {} 1\n", block_id).unwrap();
        self.disk_out.flush().unwrap();
        self.disk_in.read_exact(buffer).unwrap();
    }

    pub fn write_page(&mut self, block_id: u64, buffer: &[u8]) {
        assert!(
            block_id >= self.anon_start_block,
            "Cannot write to Read-Only region!"
        );
        write!(self.disk_out, "put block {} 1\n", block_id).unwrap();
        self.disk_out.write_all(buffer).unwrap();
        self.disk_out.flush().unwrap();
    }

    pub fn read_pages_direct(&mut self, start_block: u64, num_blocks: usize) -> Vec<u8> {
        let mut buffer = vec![0; num_blocks * self.block_size];
        self.read_pages_direct_into(start_block, num_blocks, &mut buffer);
        buffer
    }

    pub fn read_pages_direct_into(
        &mut self,
        start_block: u64,
        num_blocks: usize,
        buffer: &mut Vec<u8>,
    ) {
        let target_len = num_blocks * self.block_size;
        buffer.resize(target_len, 0);
        write!(self.disk_out, "get block {} {}\n", start_block, num_blocks).unwrap();
        self.disk_out.flush().unwrap();
        self.disk_in.read_exact(buffer).unwrap();
    }

    pub fn write_pages_direct(&mut self, start_block: u64, num_blocks: usize, buffer: &[u8]) {
        assert!(
            start_block >= self.anon_start_block,
            "Cannot write to Read-Only region!"
        );
        write!(self.disk_out, "put block {} {}\n", start_block, num_blocks).unwrap();
        self.disk_out.write_all(buffer).unwrap();
        self.disk_out.flush().unwrap();
    }

    pub fn get_file_start_block(&mut self, file_id: &str) -> u64 {
        write!(self.disk_out, "get file start-block {}\n", file_id).unwrap();
        self.disk_out.flush().unwrap();
        let mut line = String::new();
        self.disk_in.read_line(&mut line).unwrap();
        line.trim().parse().unwrap()
    }

    pub fn get_file_num_blocks(&mut self, file_id: &str) -> u64 {
        write!(self.disk_out, "get file num-blocks {}\n", file_id).unwrap();
        self.disk_out.flush().unwrap();
        let mut line = String::new();
        self.disk_in.read_line(&mut line).unwrap();
        line.trim().parse().unwrap()
    }

    pub fn allocate_anon_blocks(&mut self, num_blocks: u64) -> u64 {
        if num_blocks == 1 {
            self.free_blocks.pop().unwrap_or_else(|| {
                let start = self.next_free_anon_block;
                self.next_free_anon_block += 1;
                start
            })
        } else {
            if !self.free_blocks.is_empty() {
                self.free_blocks.sort_unstable();
                let mut sequence_start_idx = 0;
                let mut current_len = 1;

                for i in 1..self.free_blocks.len() {
                    if self.free_blocks[i] == self.free_blocks[i - 1] + 1 {
                        current_len += 1;
                        if current_len == num_blocks {
                            let start_block = self.free_blocks[sequence_start_idx];
                            self.free_blocks.drain(sequence_start_idx..=i);
                            return start_block;
                        }
                    } else {
                        sequence_start_idx = i;
                        current_len = 1;
                    }
                }
            }

            let start = self.next_free_anon_block;
            self.next_free_anon_block += num_blocks;
            start
        }
    }

    pub fn free_anon_block(&mut self, block_id: u64) {
        self.free_blocks.push(block_id);
    }
}

#[derive(PartialEq, Clone, Copy)]
pub enum QueueType {
    Am,   // Main Cache (Hot)
    A1in, // Probation Cache (Cold)
}

pub struct Frame {
    pub data: Vec<u8>,
    pub is_dirty: bool,
    pub pin_count: u32,
    pub block_id: Option<u64>,
    pub queue_type: Option<QueueType>,
}

pub struct BufferPoolManager<R: Read, W: Write> {
    pub disk_manager: DiskManager<R, W>,
    pub frames: Vec<Frame>,
    page_table: HashMap<u64, usize>,
    free_list: VecDeque<usize>,

    am: VecDeque<usize>,
    a1in: VecDeque<usize>,
    a1out: VecDeque<u64>,
    a1out_set: HashSet<u64>,
    kin: usize,
    kout: usize,
}

impl<R: Read, W: Write> BufferPoolManager<R, W> {
    pub fn new(disk_manager: DiskManager<R, W>, num_frames: usize) -> Self {
        let block_size = disk_manager.block_size;
        let mut frames = Vec::with_capacity(num_frames);
        let mut free_list = VecDeque::with_capacity(num_frames);

        for i in 0..num_frames {
            frames.push(Frame {
                data: vec![0; block_size],
                is_dirty: false,
                pin_count: 0,
                block_id: None,
                queue_type: None,
            });
            free_list.push_back(i);
        }

        Self {
            disk_manager,
            frames,
            page_table: HashMap::with_capacity(num_frames),
            free_list,
            am: VecDeque::with_capacity(num_frames),
            a1in: VecDeque::with_capacity(num_frames),
            a1out: VecDeque::with_capacity(num_frames),
            a1out_set: HashSet::with_capacity(num_frames),
            kin: std::cmp::max(1, num_frames / 4),
            kout: std::cmp::max(1, num_frames / 2),
        }
    }

    pub fn fetch_page(&mut self, block_id: u64) -> Result<usize, String> {
        if let Some(&frame_id) = self.page_table.get(&block_id) {
            let frame = &mut self.frames[frame_id];
            frame.pin_count += 1;

            if frame.queue_type == Some(QueueType::Am) {
                if let Some(idx) = self.am.iter().position(|&x| x == frame_id) {
                    self.am.remove(idx);
                    self.am.push_front(frame_id);
                }
            }

            return Ok(frame_id);
        }

        if self.a1out_set.contains(&block_id) {
            let frame_id = self.find_victim_frame()?;

            self.a1out_set.remove(&block_id);
            if let Some(idx) = self.a1out.iter().position(|&x| x == block_id) {
                self.a1out.remove(idx);
            }

            self.disk_manager
                .read_page(block_id, &mut self.frames[frame_id].data);

            let frame = &mut self.frames[frame_id];
            frame.block_id = Some(block_id);
            frame.pin_count = 1;
            frame.is_dirty = false;
            frame.queue_type = Some(QueueType::Am);

            self.am.push_front(frame_id);
            self.page_table.insert(block_id, frame_id);

            return Ok(frame_id);
        }

        let frame_id = self.find_victim_frame()?;

        self.disk_manager
            .read_page(block_id, &mut self.frames[frame_id].data);

        let frame = &mut self.frames[frame_id];
        frame.block_id = Some(block_id);
        frame.pin_count = 1;
        frame.is_dirty = false;
        frame.queue_type = Some(QueueType::A1in);

        self.a1in.push_front(frame_id);
        self.page_table.insert(block_id, frame_id);

        Ok(frame_id)
    }

    fn find_victim_frame(&mut self) -> Result<usize, String> {
        if let Some(frame_id) = self.free_list.pop_front() {
            return Ok(frame_id);
        }

        if !self.a1in.is_empty() {
            for i in 0..self.a1in.len() {
                let idx = self.a1in.len() - 1 - i;
                if self.frames[self.a1in[idx]].pin_count == 0 {
                    return Ok(self.evict_from_a1in(idx));
                }
            }
        }

        if !self.am.is_empty() {
            for i in 0..self.am.len() {
                let idx = self.am.len() - 1 - i;
                if self.frames[self.am[idx]].pin_count == 0 {
                    return Ok(self.evict_from_am(idx));
                }
            }
        }

        // All frames are pinned — cannot evict. Under RLIMIT_AS a panic backtrace
        // can itself OOM, so exit cleanly.
        eprintln!("FATAL: Buffer pool exhausted — all frames pinned");
        std::process::exit(1);
    }

    fn evict_from_a1in(&mut self, list_index: usize) -> usize {
        let frame_id = self.a1in.remove(list_index).unwrap();
        let old_block_id = self.frames[frame_id].block_id.unwrap();

        self.write_back_if_dirty(frame_id);
        self.page_table.remove(&old_block_id);

        self.a1out.push_front(old_block_id);
        self.a1out_set.insert(old_block_id);

        if self.a1out.len() > self.kout {
            if let Some(forgotten_block) = self.a1out.pop_back() {
                self.a1out_set.remove(&forgotten_block);
            }
        }

        self.clear_frame(frame_id);
        frame_id
    }

    fn evict_from_am(&mut self, list_index: usize) -> usize {
        let frame_id = self.am.remove(list_index).unwrap();
        let old_block_id = self.frames[frame_id].block_id.unwrap();

        self.write_back_if_dirty(frame_id);
        self.page_table.remove(&old_block_id);

        self.clear_frame(frame_id);
        frame_id
    }

    fn write_back_if_dirty(&mut self, frame_id: usize) {
        let frame = &mut self.frames[frame_id];
        if frame.is_dirty {
            if let Some(block_id) = frame.block_id {
                self.disk_manager.write_page(block_id, &frame.data);
            }
        }
    }

    fn clear_frame(&mut self, frame_id: usize) {
        let frame = &mut self.frames[frame_id];
        frame.block_id = None;
        frame.is_dirty = false;
        frame.pin_count = 0;
        frame.queue_type = None;
    }

    pub fn get_frame_data(&self, frame_id: usize) -> &[u8] {
        &self.frames[frame_id].data
    }

    pub fn unpin_page(&mut self, block_id: u64, is_dirty: bool) {
        if let Some(&frame_id) = self.page_table.get(&block_id) {
            let frame = &mut self.frames[frame_id];
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
            if is_dirty {
                frame.is_dirty = true;
            }
        }
    }

    pub fn read_pages_direct(&mut self, start_block: u64, num_blocks: usize) -> Vec<u8> {
        self.disk_manager.read_pages_direct(start_block, num_blocks)
    }

    pub fn read_pages_direct_into(
        &mut self,
        start_block: u64,
        num_blocks: usize,
        buffer: &mut Vec<u8>,
    ) {
        self.disk_manager
            .read_pages_direct_into(start_block, num_blocks, buffer)
    }

    pub fn write_pages_direct(&mut self, start_block: u64, num_blocks: usize, buffer: &[u8]) {
        self.disk_manager
            .write_pages_direct(start_block, num_blocks, buffer)
    }
}
