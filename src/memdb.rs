use byteorder::{ByteOrder, LittleEndian};

const NULL_ADDR: MemDBArenaAddr = MemDBArenaAddr {
    idx: u32::MAX,
    off: u32::MAX,
};
const ALIGN_MASK: usize = ((1 << 32) - 8); // 29 bit 1 and 3 bit 0.
const NULL_BLOCK_OFFSET: usize = i32::MAX as usize;
const MAX_BLOCK_SIZE: usize = 128 << 20;
const INIT_BLOCK_SIZE: usize = 4 * 1024;

type TheEndian = LittleEndian;

/// MemDBArenaAddr is a memory address in a MemDBArena.
#[derive(PartialOrd, PartialEq)]
struct MemDBArenaAddr {
    idx: u32,
    off: u32,
}

impl MemDBArenaAddr {
    fn new() -> Self {
        MemDBArenaAddr {
            idx: u32::MAX,
            off: u32::MAX,
        }
    }

    fn is_null(&self) -> bool {
        *self == NULL_ADDR
    }

    fn store(&self, dst: &mut [u8]) {
        TheEndian::write_u32(dst, self.idx);
        TheEndian::write_u32(&mut dst[4..], self.off);
    }

    fn load(&mut self, src: &[u8]) {
        self.idx = TheEndian::read_u32(src);
        self.off = TheEndian::read_u32(&src[4..]);
    }
}

/// MemDBArenaBlock is a memory block, the owned memory will be deallocated doing drop.
/// The memory management strategy is, all the memory is allocated from a specific arena
/// block and the user need not to deallocate the memory themselves, all the cleanup work
/// will be done by the arena block.
struct MemDBArenaBlock {
    membuf: MemBuf,
    length: usize,
}

/// MemBuf is a mark for a memory like vector.
struct MemBuf {
    buf: *mut u8,
    capacity: usize,
}

impl MemDBArenaBlock {
    /// Create a new MemDBArenaBlock with fixed capacity.
    fn new(block_size: usize) -> Self {
        MemDBArenaBlock {
            membuf: MemBuf {
                buf: unsafe {
                    let mut v = Vec::<u8>::with_capacity(block_size);
                    let ptr = v.as_mut_ptr();
                    std::mem::forget(v);
                    ptr
                },
                capacity: block_size,
            },
            length: 0,
        }
    }

    /// Try to allocate a buf for the given size in current block, if the space is not
    /// enough, None will be returned. The offset and allocated result are returned.
    fn alloc(&mut self, size: usize, align: bool) -> (usize, Option<MemBuf>) {
        let mut offset = self.length;
        if align {
            offset = ((self.length + 7) & ALIGN_MASK) as usize;
        }
        let new_len = offset + size;
        if new_len > self.membuf.capacity {
            return (NULL_BLOCK_OFFSET as usize, None);
        }
        self.length = new_len;
        let buf = unsafe {
            MemBuf {
                buf: self.membuf.buf.add(offset as usize),
                capacity: size,
            }
        };
        (offset, Some(buf))
    }
}

impl Drop for MemDBArenaBlock {
    /// Free the owned memory buffer.
    fn drop(&mut self) {
        unsafe {
            // Create a slice from the buffer to make
            // a fat pointer. Then, use Box::from_raw
            // to deallocate it.
            let ptr = core::slice::from_raw_parts_mut(self.membuf.buf, self.membuf.capacity)
                as *mut [u8];
            Box::from_raw(ptr);
        }
    }
}

/// A checkpoint recording the memory status.
struct MemDBCheckpoint {
    block_size: usize,
    blocks: usize,
    offset_in_block: usize,
}

impl MemDBCheckpoint {
    /// Checks if two checkpoints are in the same arena block.
    fn is_same_position(&self, other: &MemDBCheckpoint) -> bool {
        self.blocks == other.blocks && self.offset_in_block == other.offset_in_block
    }
}

/// MemDBArena is a collection of MemDBArenaBlock.
struct MemDBArena {
    block_size: usize,
    blocks: Vec<MemDBArenaBlock>,
}

impl MemDBArena {
    /// Do the checkpoint for current memory usage status.
    fn checkpoint(&self) -> MemDBCheckpoint {
        let mut snap = MemDBCheckpoint {
            block_size: self.block_size,
            blocks: self.blocks.len(),
            offset_in_block: 0,
        };
        if self.blocks.len() > 0 {
            snap.offset_in_block = self.blocks.last().unwrap().length;
        }
        snap
    }

    /// Revert to the snapshot memory usage status.
    fn truncate(&mut self, snap: &MemDBCheckpoint) {
        assert!(snap.blocks <= self.blocks.len());
        self.blocks.truncate(snap.blocks);
        if self.blocks.len() > 0 {
            self.blocks.last_mut().unwrap().length = snap.offset_in_block;
        }
        self.block_size = snap.block_size;
    }

    fn enlarge(&mut self, allocate_size: usize, block_size: usize) {
        self.block_size = block_size;
        loop {
            if self.block_size > allocate_size {
                break;
            }
            self.block_size = self.block_size << 1;
        }
        let new_block = MemDBArenaBlock::new(self.block_size);
        self.blocks.push(new_block);
    }
}

// bit 1 => red, bit 0 => black
const NODE_COLOR_BIT: u8 = 0x80;
const NODE_FLAGS_MASK: u8 = (!NODE_COLOR_BIT);

/// MemDBNode is the tree node of the memory buffer.
struct MemDBNode {
    up: MemDBArenaAddr,
    left: MemDBArenaAddr,
    right: MemDBArenaAddr,
    vptr: MemDBArenaAddr,
    klen: u16,
    flags: u8,
    kptr: MemBuf,
}

impl MemDBNode {
    /// Check if the node is marked red.
    fn is_red(&self) -> bool {
        (self.flags & NODE_COLOR_BIT) != 0
    }

    /// Check if the node is marked black.
    fn is_black(&self) -> bool {
        !self.is_red()
    }

    /// Mark the node red.
    fn set_red(&mut self) {
        self.flags |= NODE_COLOR_BIT;
    }

    /// Mark the node black.
    fn set_black(&mut self) {
        self.flags &= (!NODE_COLOR_BIT);
    }

    /// Return the key reference to the key.The underlying memory of the key node
    /// is owned by the MemDBArenaBlock, so the MemDBNode is not responsible for
    /// deallocating it.
    fn get_key(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.kptr.buf, self.kptr.capacity) }
    }
}

pub struct MemDB {
    count: i32,
    size: i32,
    vlog_invalid: bool,
    dirty: bool,
}

impl MemDB {
    /// Create a new MemDB object.
    pub fn new() -> Self {
        MemDB {
            count: 0,
            size: 0,
            vlog_invalid: false,
            dirty: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Borrow;

    #[test]
    fn test_mem_addr() {
        let mut mem_addr = MemDBArenaAddr::new();
        assert!(mem_addr.is_null(), true);
        mem_addr.idx = 1;
        mem_addr.off = 2;
        let mut buf = vec![0; 8];
        mem_addr.store(buf.as_mut_slice());
        let mut new_mem_addr = MemDBArenaAddr::new();
        new_mem_addr.load(buf.as_slice());
        assert_eq!(new_mem_addr.idx, 1);
        assert_eq!(new_mem_addr.off, 2);
    }

    #[test]
    fn test_mem_arena_block() {
        let block_size = 128;
        let mut mem_arena_block = MemDBArenaBlock::new(block_size);
        assert_eq!(mem_arena_block.membuf.capacity, block_size);

        // Test alloc.
        let (offset, alloc_res) = mem_arena_block.alloc(15, true);
        assert_eq!(offset, 0);
        assert_eq!(mem_arena_block.length, 15);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.capacity, 15);
        assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf);
        let (offset, alloc_res) = mem_arena_block.alloc(32, true);
        assert_eq!(offset, 16);
        assert_eq!(mem_arena_block.length, 48);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.capacity, 32);
        unsafe {
            assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf.add(16));
        }
        let (offset, alloc_res) = mem_arena_block.alloc(29, true);
        assert_eq!(offset, 48);
        assert_eq!(mem_arena_block.length, 77);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.capacity, 29);
        unsafe {
            assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf.add(48));
        }

        // No more space.
        let (offset, alloc_res) = mem_arena_block.alloc(64, true);
        assert_eq!(offset, NULL_BLOCK_OFFSET);
        assert!(alloc_res.is_none());

        // Alloc again.
        let (offset, alloc_res) = mem_arena_block.alloc(5, true);
        assert_eq!(offset, 80);
        assert_eq!(mem_arena_block.length, 85);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.capacity, 5);
        unsafe {
            assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf.add(80));
        }
    }
}
