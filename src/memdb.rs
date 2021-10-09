use byteorder::{ByteOrder, LittleEndian};
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;

const NULL_ADDR: MemDBArenaAddr = MemDBArenaAddr {
    idx: u32::MAX,
    off: u32::MAX,
};
const ALIGN_MASK: usize = (1 << 32) - 8; // 29 bit 1 and 3 bit 0.
const NULL_BLOCK_OFFSET: usize = i32::MAX as usize;
const MAX_BLOCK_SIZE: usize = 128 << 20;
const INIT_BLOCK_SIZE: usize = 4 * 1024;
const MEMDB_VLOG_HDR_SIZE: usize = 8 + 8 + 4;

// bit 1 => red, bit 0 => black
const NODE_COLOR_BIT: u8 = 0x80;
const NODE_FLAGS_MASK: u8 = !NODE_COLOR_BIT;

type TheEndian = LittleEndian;

/// MemDBArenaAddr is a memory address in a MemDBArena.
/// idx is the index for arena blocks.
/// off is the offset within a specific block.
#[derive(PartialOrd, PartialEq, Copy, Clone)]
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

    /// store the information within the MemDBArenaAddr object into the dst.
    fn store(&self, dst: &mut [u8]) {
        TheEndian::write_u32(dst, self.idx);
        TheEndian::write_u32(&mut dst[4..], self.off);
    }

    /// load the information from the src into the MemDBArenaAddr object.
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
    len: usize,
}

impl MemBuf {
    /// Do copy the value into the memory address.
    fn copy_value(&mut self, value: &[u8]) {
        unsafe {
            self.buf.copy_from(value.as_ptr(), value.len());
        }
    }
}

impl MemDBArenaBlock {
    /// Create a new MemDBArenaBlock with fixed capacity.
    fn new(block_size: usize) -> Self {
        MemDBArenaBlock {
            membuf: MemBuf {
                buf: {
                    let mut v = Vec::<u8>::with_capacity(block_size);
                    let ptr = v.as_mut_ptr();
                    std::mem::forget(v);
                    ptr
                },
                len: block_size,
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
        if new_len > self.membuf.len {
            return (NULL_BLOCK_OFFSET as usize, None);
        }
        self.length = new_len;
        let buf = unsafe {
            MemBuf {
                buf: self.membuf.buf.add(offset as usize),
                len: size,
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
            let ptr =
                core::slice::from_raw_parts_mut(self.membuf.buf, self.membuf.len) as *mut [u8];
            Box::from_raw(ptr);
        }
    }
}

/// A checkpoint recording the memory status.
#[derive(Copy, Clone)]
struct MemDBCheckpoint {
    block_size: usize,
    blocks: usize,
    offset_in_block: usize,
}

impl MemDBCheckpoint {
    fn new(block_size: usize, blocks: usize) -> Self {
        MemDBCheckpoint {
            block_size,
            blocks,
            offset_in_block: 0,
        }
    }
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
    fn new() -> Self {
        MemDBArena {
            block_size: 0,
            blocks: vec![],
        }
    }
    fn alloc_in_last_block(
        &mut self,
        size: usize,
        align: bool,
    ) -> (MemDBArenaAddr, Option<MemBuf>) {
        let idx = self.blocks.len() - 1;
        let (offset, data) = self.blocks[idx].alloc(size, align);
        if offset == NULL_BLOCK_OFFSET {
            return (NULL_ADDR, None);
        }
        (
            MemDBArenaAddr {
                idx: idx as u32,
                off: offset as u32,
            },
            data,
        )
    }

    fn alloc(&mut self, size: usize, align: bool) -> (MemDBArenaAddr, Option<MemBuf>) {
        if size > MAX_BLOCK_SIZE {
            panic!("allocate size is greater than the maximum size");
        }
        if self.blocks.len() == 0 {
            self.enlarge(size, INIT_BLOCK_SIZE);
        }
        let (addr, data) = self.alloc_in_last_block(size, align);
        if !addr.is_null() {
            return (addr, data);
        }
        self.enlarge(size, self.block_size << 1);
        self.alloc_in_last_block(size, align)
    }

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
        // Size will never be larger than maxBlockSize.
        if self.block_size > MAX_BLOCK_SIZE {
            self.block_size = MAX_BLOCK_SIZE
        }
        let new_block = MemDBArenaBlock::new(self.block_size);
        self.blocks.push(new_block);
    }
}

/// MemDBNode is the tree node in the memdb.
#[derive(Clone, Copy)]
struct MemDBNode {
    up: MemDBArenaAddr,
    left: MemDBArenaAddr,
    right: MemDBArenaAddr,
    vptr: MemDBArenaAddr,
    klen: u16,
    flags: u8,
}

impl MemDBNode {
    fn new() -> Self {
        MemDBNode {
            up: NULL_ADDR,
            left: NULL_ADDR,
            right: NULL_ADDR,
            vptr: NULL_ADDR,
            klen: 0,
            flags: 0,
        }
    }
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
        self.flags &= !NODE_COLOR_BIT;
    }

    /// Return the key reference to the key.The underlying memory of the key node
    /// is owned by the MemDBArenaBlock, so the MemDBNode is not responsible for
    /// deallocating it.
    fn get_key(&mut self) -> MemBuf {
        let flag_ptr: *mut u8 = &mut self.flags;
        MemBuf {
            buf: unsafe { flag_ptr.add(1) },
            len: self.klen as usize,
        }
    }
}

struct MemDBNodeAddr {
    node: MemDBNode,
    addr: MemDBArenaAddr,
}

impl MemDBNodeAddr {
    fn new(node: MemDBNode, addr: MemDBArenaAddr) -> Self {
        MemDBNodeAddr { node, addr }
    }

    fn is_null(&self) -> bool {
        self.addr.is_null()
    }

    fn get_up(&self, db: &mut MemDB) -> MemDBNodeAddr {
        db.get_node(self.node.up)
    }

    fn get_left(&self, db: &mut MemDB) -> MemDBNodeAddr {
        db.get_node(self.node.left)
    }

    fn get_right(&self, db: &mut MemDB) -> MemDBNodeAddr {
        db.get_node(self.node.right)
    }
}

struct MemDBVlog {
    mem_arena: MemDBArena,
}

impl MemDBVlog {
    fn new() -> Self {
        MemDBVlog {
            mem_arena: MemDBArena::new(),
        }
    }

    /// Append a value into the memory db value log, the new value will have pointer to the node
    /// itself and the old or previous value.
    fn append_value(
        &mut self,
        node_addr: MemDBArenaAddr,
        old_value: MemDBArenaAddr,
        value: &[u8],
    ) -> MemDBArenaAddr {
        let size = MEMDB_VLOG_HDR_SIZE + value.len();
        let (mut addr, mem) = self.mem_arena.alloc(size, false);
        let mut membuf = mem.unwrap();
        membuf.copy_value(value);
        let mem_vlog_header = MemDBVlogHdr {
            node_addr,
            old_value,
            value_len: value.len() as u32,
        };
        unsafe {
            let slice_buf = std::slice::from_raw_parts_mut(
                membuf.buf.add(value.len()),
                membuf.len - value.len(),
            );
            mem_vlog_header.store(slice_buf);
        }
        addr.off += size as u32;
        addr
    }

    fn get_value(&self, addr: MemDBArenaAddr) -> Option<&[u8]> {
        let len_off = addr.off as usize - MEMDB_VLOG_HDR_SIZE;
        let block: &MemDBArenaBlock = self.mem_arena.blocks.get(addr.idx as usize).unwrap();
        let dst_slice = unsafe {
            std::slice::from_raw_parts(block.membuf.buf.add(len_off as usize), len_off as usize)
        };
        let value_len = TheEndian::read_u32(dst_slice) as usize;
        if value_len == 0 {
            return None;
        }
        let value_off = len_off - value_len;
        unsafe {
            Some(std::slice::from_raw_parts(
                block.membuf.buf.add(value_off as usize),
                value_off as usize,
            ))
        }
    }

    fn can_modify(cp: &Option<MemDBCheckpoint>, addr: MemDBArenaAddr) -> bool {
        if cp.is_none() {
            return true;
        }
        let check_point = cp.as_ref().unwrap();
        if addr.idx > (check_point.blocks - 1) as u32 {
            return true;
        }
        if addr.idx == (check_point.blocks - 1) as u32
            && addr.off > check_point.offset_in_block as u32
        {
            return true;
        }
        false
    }

    fn get_snapshot_value(
        &self,
        addr: MemDBArenaAddr,
        snap: &Option<MemDBCheckpoint>,
    ) -> (Option<&[u8]>, bool) {
        let addr_res =
            self.select_value_history(addr, |the_addr| MemDBVlog::can_modify(snap, addr));
        if addr_res.is_none() {
            return (None, false);
        }
        (self.get_value(addr_res.unwrap()), true)
    }

    fn select_value_history<F: Fn(&MemDBArenaAddr) -> bool>(
        &self,
        addr: MemDBArenaAddr,
        predicate: F,
    ) -> Option<MemDBArenaAddr> {
        let mut addr_res = addr;
        while !addr_res.is_null() {
            if predicate(&addr_res) {
                return Some(addr_res);
            }
            let mut hdr = MemDBVlogHdr::new();
            let src_addr = unsafe {
                let block: &MemDBArenaBlock =
                    self.mem_arena.blocks.get(addr_res.idx as usize).unwrap();
                std::slice::from_raw_parts(
                    block
                        .membuf
                        .buf
                        .add((addr_res.off - MEMDB_VLOG_HDR_SIZE as u32) as usize),
                    MEMDB_VLOG_HDR_SIZE,
                )
            };
            hdr.load(src_addr);
            addr_res = hdr.old_value;
        }
        None
    }

    fn checkpoint(&mut self) -> MemDBCheckpoint {
        self.mem_arena.checkpoint()
    }

    /// TODO.
    fn inspect_kv_in_log<F: Fn(&[u8], &[u8])>(
        &self,
        db: &mut MemDB,
        head: &MemDBCheckpoint,
        tail: &MemDBCheckpoint,
        f: F,
    ) {
    }

    fn move_back_cursor(&mut self, cursor: &mut MemDBCheckpoint, hdr: &MemDBVlogHdr) {
        cursor.offset_in_block -= MEMDB_VLOG_HDR_SIZE + hdr.value_len as usize;
        if cursor.offset_in_block == 0 {
            cursor.blocks -= 1;
            if cursor.blocks > 0 {
                cursor.offset_in_block = self.mem_arena.blocks[cursor.blocks - 1].length;
            }
        }
    }
}

struct MemDBVlogHdr {
    node_addr: MemDBArenaAddr,
    old_value: MemDBArenaAddr,
    value_len: u32,
}
impl MemDBVlogHdr {
    fn new() -> MemDBVlogHdr {
        MemDBVlogHdr {
            node_addr: MemDBArenaAddr { idx: 0, off: 0 },
            old_value: MemDBArenaAddr { idx: 0, off: 0 },
            value_len: 0,
        }
    }

    fn store(&self, dst: &mut [u8]) {
        let mut cursor = 0;
        TheEndian::write_u32(&mut dst[cursor..], self.value_len);
        cursor += 4;
        self.old_value.store(&mut dst[cursor..]);
        cursor += 4;
        self.node_addr.store(&mut dst[cursor..]);
    }

    fn load(&mut self, src: &[u8]) {
        let mut cursor = 0;
        self.value_len = TheEndian::read_u32(&src[cursor..]);
        cursor += 4;
        self.old_value.load(&src[cursor..]);
        cursor += 8;
        self.node_addr.load(&src[cursor..]);
    }
}

struct NodeAllocator {
    mem_arena: MemDBArena,

    // Dummy node, so that we can make X.left.up = X.
    // We then use this instead of NULL to mean the top or bottom
    // end of the rb tree. It is a black node.
    null_node: MemDBNode,
}

impl NodeAllocator {
    fn new() -> Self {
        NodeAllocator {
            mem_arena: MemDBArena::new(),
            null_node: MemDBNode::new(),
        }
    }

    fn get_node(&mut self, addr: MemDBArenaAddr) -> MemDBNode {
        if addr.is_null() {
            return MemDBNode::new();
        }
        let block: &MemDBArenaBlock = self.mem_arena.blocks.get(addr.idx as usize).unwrap();
        unsafe {
            let node_ptr = block.membuf.buf.add(addr.off as usize) as *const MemDBNode;
            (*node_ptr)
        }
    }
}

/// MemDB is rollbackable Red-Black Tree optimized for TiDB's transaction states buffer use scenario.
/// You can think MemDB is a combination of two separate tree map, one for key => value and another for key => keyFlags.
///
/// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
///
/// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
/// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
/// If there are persistent flags associated with key, we will keep this key in node without value.
pub struct MemDB {
    lock: std::sync::Mutex<i8>,
    root: MemDBArenaAddr,
    allocator: NodeAllocator,
    vlog: MemDBVlog,

    count: i32,
    size: i32,
    vlog_invalid: bool,
    dirty: bool,
    stages: Vec<MemDBCheckpoint>,
}

impl MemDB {
    /// Create a new MemDB object.
    pub fn new() -> Self {
        MemDB {
            lock: std::sync::Mutex::new(1),
            root: MemDBArenaAddr::new(),
            allocator: NodeAllocator::new(),
            vlog: MemDBVlog::new(),
            count: 0,
            size: 0,
            vlog_invalid: false,
            dirty: false,
            stages: vec![],
        }
    }

    pub fn staging(&mut self) -> usize {
        let _ = self.lock.lock();
        self.stages.push(self.vlog.checkpoint());
        self.stages.len()
    }

    pub fn release(&mut self, h: usize) {
        if h != self.stages.len() {
            // This should never happens in production environment.
            // Use panic to make debug easier.
            panic!("cannot release staging buffer");
        }

        self.lock.lock();
        if h == 1 {
            let tail = self.vlog.checkpoint();
            if !self.stages[0].is_same_position(&tail) {
                self.dirty = true;
            }
        }
        self.stages.pop();
    }
    /// Cleanup cleanup the resources referenced by the StagingHandle.
    /// If the changes are not published by `Release`, they will be discarded.
    pub fn cleanup(&mut self, h: usize) {
        if h > self.stages.len() {
            return;
        }
        if h < self.stages.len() {
            // This should never happens in production environment.
            // Use panic to make debug easier.
            panic!("cannot cleanup staging buffer");
        }

        self.lock.lock();
        let cp = self.stages.get(h - 1).unwrap().clone();
        if !self.vlog_invalid {
            let curr = self.vlog.checkpoint();
            if !curr.is_same_position(&cp) {
                self.revert_to_checkpoint(&cp);
                self.truncate(&cp);
            }
        }
        self.stages.pop();
    }

    fn get_node(&mut self, x: MemDBArenaAddr) -> MemDBNodeAddr {
        MemDBNodeAddr::new(self.allocator.get_node(x), x)
    }

    /// TODO.
    fn revert_to_checkpoint(&mut self, cp: &MemDBCheckpoint) {
        let mut cursor = self.vlog.checkpoint();
        while !cp.is_same_position(&cursor) {
            let hdr_off = cursor.offset_in_block - MEMDB_VLOG_HDR_SIZE;
            let block = self.vlog.mem_arena.blocks.get(cursor.blocks - 1).unwrap();
            let mut hdr = MemDBVlogHdr::new();
            unsafe {
                hdr.load(core::slice::from_raw_parts(
                    block.membuf.buf.add(hdr_off),
                    MEMDB_VLOG_HDR_SIZE,
                ));
            }
            let mut node = self.get_node(hdr.node_addr);
            node.node.vptr = hdr.old_value;
            self.size -= hdr.value_len as i32;
            if hdr.old_value.is_null() {
                // TODO, process key flags.
            } else {
                self.size += self.vlog.get_value(hdr.old_value).unwrap().len() as i32;
            }

            self.vlog.move_back_cursor(&mut cursor, &hdr);
        }
    }

    /// Revert to the snapshot memory usage status.
    fn truncate(&mut self, snap: &MemDBCheckpoint) {
        assert!(snap.blocks <= self.vlog.mem_arena.blocks.len());
        self.vlog.mem_arena.blocks.truncate(snap.blocks);
        if self.vlog.mem_arena.blocks.len() > 0 {
            self.vlog.mem_arena.blocks.last_mut().unwrap().length = snap.offset_in_block;
        }
        self.vlog.mem_arena.block_size = snap.block_size;
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
        assert_eq!(mem_arena_block.membuf.len, block_size);

        // Test alloc.
        let (offset, alloc_res) = mem_arena_block.alloc(15, true);
        assert_eq!(offset, 0);
        assert_eq!(mem_arena_block.length, 15);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.len, 15);
        assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf);
        let (offset, alloc_res) = mem_arena_block.alloc(32, true);
        assert_eq!(offset, 16);
        assert_eq!(mem_arena_block.length, 48);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.len, 32);
        unsafe {
            assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf.add(16));
        }
        let (offset, alloc_res) = mem_arena_block.alloc(29, true);
        assert_eq!(offset, 48);
        assert_eq!(mem_arena_block.length, 77);
        let mem_addr = alloc_res.unwrap();
        assert_eq!(mem_addr.len, 29);
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
        assert_eq!(mem_addr.len, 5);
        unsafe {
            assert_eq!(mem_addr.buf, mem_arena_block.membuf.buf.add(80));
        }
    }

    #[test]
    fn test_vlog_basic() {
        let v = b"value";
        let vlog = MemDBVlog::new();
    }
}
