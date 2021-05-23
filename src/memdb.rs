use byteorder::{ByteOrder, LittleEndian};

const NULL_ADDR: MemDBArenaAddr = MemDBArenaAddr {
    idx: u32::MAX,
    off: u32::MAX,
};
const ALIGN_MASK: i32 = 1 << 32 - 8; // 29 bit 1 and 3 bit 0.
const NULL_BLOCK_OFFSET: i32 = i32::MAX;
const MAX_BLOCK_SIZE: i32 = 128 << 20;
const INIT_BLOCK_SIZE: i32 = 4 * 1024;

type TheEndian = LittleEndian;

/// MemDBArenaAddr is a memory address in a MemDBArena.
#[derive(PartialOrd, PartialEq)]
struct MemDBArenaAddr {
    idx: u32,
    off: u32,
}

impl MemDBArenaAddr {
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

    /// Revert to snapshot memory usage status.
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
        let new_block = MemDBArenaBlock {
            membuf: MemBuf {
                buf: unsafe {
                    let mut v = Vec::<u8>::with_capacity(self.block_size);
                    let ptr = v.as_mut_ptr();
                    std::mem::forget(v);
                    ptr
                },
                capacity: self.block_size,
            },
            length: 0,
        };
        self.blocks.push(new_block);
    }
}

/// MemDBArenaBlock is a memory block, the owned memory will be deallocated doing drop.
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
    /// Try to allocate a buf for the given size in current block, if the space is not
    /// enough, None will be returned.
    fn alloc(&mut self, size: usize, align: bool) -> (usize, Option<MemBuf>) {
        let mut offset = self.length;
        if align {
            offset = ((self.length + 7) as i32 & ALIGN_MASK) as usize;
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
mod tests {}
