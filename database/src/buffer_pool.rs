use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};

// This acts as your driver to talk to the Disk Simulator over File Descriptors.
pub struct DiskManager<R: Read, W: Write> {
    disk_in: BufReader<R>,
    disk_out: W,
    pub block_size: usize,
    pub anon_start_block: u64,
}

impl<R: Read, W: Write> DiskManager<R, W> {

     pub fn new(mut disk_in: BufReader<R>, mut disk_out: W) -> Self {

          // Get block size from disk simulator
          disk_out.write_all(b"get block-size\n").unwrap();
          disk_out.flush().unwrap();
          let mut line = String::new();
          disk_in.read_line(&mut line).unwrap();
          let block_size: usize = line.trim().parse().unwrap();

          // Get anon start block
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
          line.trim().parse().expect("Failed to parse num-blocks")
     }

}

pub struct Frame {
    pub data: Vec<u8>,         
    pub is_dirty: bool,       
    pub pin_count: u32,       
    pub block_id: Option<u64>,
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
                    data: vec![0; block_size],
                    is_dirty: false,
                    pin_count: 0,
                    block_id: None,
               });
               free_list.push_back(i);
          }

          Self {
               disk_manager,
               frames,
               page_table: HashMap::new(),
               free_list,
               clock_hand: 0,
          }
     }

     /// Asks the buffer pool for a page. Returns the integer index of the frame.

     pub fn fetch_page(&mut self, block_id: u64) -> Result<usize, String> {

          // Case 1: The block is already cached in memory!
          if let Some(&frame_id) = self.page_table.get(&block_id) {
               self.frames[frame_id].pin_count += 1;
               return Ok(frame_id);
          }

          // Case 2: We need to load it from disk. Find a frame to put it in.
          let frame_id = self.find_victim_frame()?;

          // If the frame we selected previously held a dirty block, save it to disk first!
          if let Some(old_block_id) = self.frames[frame_id].block_id {
               if self.frames[frame_id].is_dirty {
                    self.disk_manager.write_page(old_block_id, &self.frames[frame_id].data);
               }
               self.page_table.remove(&old_block_id); // Remove the old block from the page table
          }

          // Load the new block from disk into the frame
          self.disk_manager.read_page(block_id, &mut self.frames[frame_id].data);
          
          // Update the frame's metadata
          self.frames[frame_id].block_id = Some(block_id);
          self.frames[frame_id].pin_count = 1;
          self.frames[frame_id].is_dirty = false;
          
          // Record it in the page table so we can find it fast next time
          self.page_table.insert(block_id, frame_id);

          Ok(frame_id)
     }

     pub fn get_frame_data(&self, frame_id: usize) -> &[u8] {
          &self.frames[frame_id].data
     }

     /// Always call this when you are done with a page, or the buffer pool will fill up and crash!
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

     fn find_victim_frame(&mut self) -> Result<usize, String> {

          //Do we have an empty frame in the free list?
          if let Some(frame_id) = self.free_list.pop_front() {
               return Ok(frame_id);
          }

          //All frames are full. We must kick one out using the CLOCK algorithm.
          let start = self.clock_hand;
          loop {
               let frame = &self.frames[self.clock_hand];
               
               // If the pin count is 0, nobody is currently using this frame. Evict it!
               if frame.pin_count == 0 {
                    let victim = self.clock_hand;
                    self.clock_hand = (self.clock_hand + 1) % self.frames.len();
                    return Ok(victim);
               }
               
               // Move the clock hand to the next frame
               self.clock_hand = (self.clock_hand + 1) % self.frames.len();
               
               // If we checked every single frame and they are ALL pinned, we are out of memory!
               if self.clock_hand == start {
                    return Err("OOM: All frames in the buffer pool are pinned!".to_string());
               }
          }
     }
}
