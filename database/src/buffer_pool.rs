use std::collections::{HashMap, VecDeque};
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
               disk_in, disk_out, block_size, anon_start_block,
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
          assert!(block_id >= self.anon_start_block, "Cannot write to Read-Only region!");
          write!(self.disk_out, "put block {} 1\n", block_id).unwrap();
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
              let start = self.next_free_anon_block;
              self.next_free_anon_block += num_blocks;
              start
          }
     }

     pub fn free_anon_block(&mut self, block_id: u64) {
          self.free_blocks.push(block_id);
     }
}

pub struct Frame {
    pub data: Vec<u8>,         
    pub is_dirty: bool,       
    pub pin_count: u32,       
    pub block_id: Option<u64>,
    pub referenced: bool,
}

pub struct BufferPoolManager<R: Read, W: Write> {
    pub disk_manager: DiskManager<R, W>,
    pub frames: Vec<Frame>,             
    page_table: HashMap<u64, usize>,
    free_list: VecDeque<usize>,
    clock_hand: usize,
}

impl<R: Read, W: Write> BufferPoolManager<R, W> {
     pub fn new(disk_manager: DiskManager<R, W>, num_frames: usize) -> Self {
          let block_size = disk_manager.block_size;
          let mut frames = Vec::with_capacity(num_frames);
          let mut free_list = VecDeque::with_capacity(num_frames);

          for i in 0..num_frames {
               frames.push(Frame {
                    data: vec![0; block_size], is_dirty: false, pin_count: 0,
                    block_id: None, referenced: false,
               });
               free_list.push_back(i);
          }

          Self { disk_manager, frames, page_table: HashMap::new(), free_list, clock_hand: 0 }
     }

     pub fn fetch_page(&mut self, block_id: u64) -> Result<usize, String> {
          if let Some(&frame_id) = self.page_table.get(&block_id) {
               self.frames[frame_id].pin_count += 1;
               self.frames[frame_id].referenced = true; // Mark as recently used
               return Ok(frame_id);
          }

          let frame_id = self.find_victim_frame()?;

          if let Some(old_block_id) = self.frames[frame_id].block_id {
               if self.frames[frame_id].is_dirty {
                    self.disk_manager.write_page(old_block_id, &self.frames[frame_id].data);
               }
               self.page_table.remove(&old_block_id);
          }

          self.disk_manager.read_page(block_id, &mut self.frames[frame_id].data);
          
          self.frames[frame_id].block_id = Some(block_id);
          self.frames[frame_id].pin_count = 1;
          self.frames[frame_id].is_dirty = false;
          self.frames[frame_id].referenced = true;
          
          self.page_table.insert(block_id, frame_id);
          Ok(frame_id)
     }

     pub fn get_frame_data(&self, frame_id: usize) -> &[u8] {
          &self.frames[frame_id].data
     }

     pub fn unpin_page(&mut self, block_id: u64, is_dirty: bool) {
          if let Some(&frame_id) = self.page_table.get(&block_id) {
               let frame = &mut self.frames[frame_id];
               if frame.pin_count > 0 { frame.pin_count -= 1; }
               if is_dirty { frame.is_dirty = true; }
          }
     }

     fn find_victim_frame(&mut self) -> Result<usize, String> {
          if let Some(frame_id) = self.free_list.pop_front() {
               return Ok(frame_id);
          }

          let start = self.clock_hand;
          let mut looped_once = false;

          loop {
               let frame = &mut self.frames[self.clock_hand];
               
               if frame.pin_count == 0 {
                    if frame.referenced {
                         frame.referenced = false; // Give a second chance
                    } else {
                         let victim = self.clock_hand;
                         self.clock_hand = (self.clock_hand + 1) % self.frames.len();
                         return Ok(victim);
                    }
               }
               
               self.clock_hand = (self.clock_hand + 1) % self.frames.len();
               
               if self.clock_hand == start {
                    if looped_once {
                         let all_pinned = self.frames.iter().all(|f| f.pin_count > 0);
                         if all_pinned { return Err("OOM: All frames pinned!".to_string()); }
                    }
                    looped_once = true;
               }
          }
     }
}