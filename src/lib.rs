use std::{
    borrow::{Borrow, BorrowMut},
    hash::{Hash, Hasher},
    mem::MaybeUninit,
    ptr::addr_of,
    sync::{
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use ebr::{Ebr, Guard};

mod dll;

use crate::dll::{DoublyLinkedList, Node};

#[cfg(any(test, feature = "lock_free_delays"))]
fn debug_delay() {
    std::thread::yield_now();
}

#[cfg(not(any(test, feature = "lock_free_delays")))]
fn debug_delay() {}

fn random_bool() -> bool {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);
        ((x.0 as u64).wrapping_mul(2 as u64) >> 32) as u32
    }) == 1
}

fn probabilistic_size(input: usize) -> u8 {
    let po2 = if random_bool() {
        input.next_power_of_two() >> 1
    } else {
        input.next_power_of_two()
    };

    let ret = if po2 == 0 {
        0
    } else {
        po2.trailing_zeros() as u8
    };

    assert!(
        ret < 64,
        "input {} should not return a number over 64, {}",
        input,
        ret
    );

    ret
}

fn probabilistic_unsize(input: u8) -> usize {
    1 << input
}

pub struct Fnv(u64);

impl Default for Fnv {
    #[inline]
    fn default() -> Fnv {
        Fnv(0xcbf29ce484222325)
    }
}

impl std::hash::Hasher for Fnv {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let Fnv(mut hash) = *self;

        for byte in bytes.iter() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }

        *self = Fnv(hash);
    }
}

pub(crate) type FastSet8<V> = std::collections::HashSet<V, std::hash::BuildHasherDefault<Fnv>>;

type PageId = u64;

#[cfg(any(test, feature = "lock_free_delays"))]
const MAX_QUEUE_ITEMS: usize = 4;

#[cfg(not(any(test, feature = "lock_free_delays")))]
const MAX_QUEUE_ITEMS: usize = 64;

#[cfg(any(test, feature = "lock_free_delays"))]
const N_SHARDS: usize = 2;

#[cfg(not(any(test, feature = "lock_free_delays")))]
const N_SHARDS: usize = 256;

const SHARD_BITS: usize = N_SHARDS.trailing_zeros() as usize;

struct AccessBlock {
    len: AtomicUsize,
    block: [AtomicU64; MAX_QUEUE_ITEMS],
    next: AtomicPtr<AccessBlock>,
}

impl Default for AccessBlock {
    fn default() -> AccessBlock {
        AccessBlock {
            len: AtomicUsize::new(0),
            block: unsafe { MaybeUninit::zeroed().assume_init() },
            next: AtomicPtr::default(),
        }
    }
}

struct AccessQueue {
    writing: AtomicPtr<AccessBlock>,
    full_list: AtomicPtr<AccessBlock>,
}

impl AccessBlock {
    fn new(item: CacheAccess) -> AccessBlock {
        let mut ret = AccessBlock {
            len: AtomicUsize::new(1),
            block: unsafe { MaybeUninit::zeroed().assume_init() },
            next: AtomicPtr::default(),
        };
        ret.block[0] = AtomicU64::from(u64::from(item));
        ret
    }
}

impl Default for AccessQueue {
    fn default() -> AccessQueue {
        AccessQueue {
            writing: AtomicPtr::new(Box::into_raw(Box::default())),
            full_list: AtomicPtr::default(),
        }
    }
}

impl AccessQueue {
    fn push(&self, item: CacheAccess) -> bool {
        loop {
            debug_delay();
            let head = self.writing.load(Ordering::Acquire);
            let block = unsafe { &*head };

            debug_delay();
            let offset = block.len.fetch_add(1, Ordering::Acquire);

            if offset < MAX_QUEUE_ITEMS {
                let item_u64: u64 = item.into();
                assert_ne!(item_u64, 0);
                debug_delay();
                unsafe {
                    block
                        .block
                        .get_unchecked(offset)
                        .store(item_u64, Ordering::Release);
                }
                return false;
            } else {
                // install new writer
                let new = Box::into_raw(Box::new(AccessBlock::new(item)));
                debug_delay();
                let res = self.writing.compare_exchange_weak(
                    head,
                    new,
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                if res.is_err() {
                    // we lost the CAS, free the new item that was
                    // never published to other threads
                    unsafe {
                        drop(Box::from_raw(new));
                    }
                    continue;
                }

                // push the now-full item to the full list for future
                // consumption
                let mut ret;
                let mut full_list_ptr = self.full_list.load(Ordering::Acquire);
                while {
                    // we loop because maybe other threads are pushing stuff too
                    block.next.store(full_list_ptr, Ordering::Release);
                    debug_delay();
                    ret = self.full_list.compare_exchange_weak(
                        full_list_ptr,
                        head,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                    ret.is_err()
                } {
                    full_list_ptr = ret.unwrap_err();
                }
                return true;
            }
        }
    }

    fn take<'a>(&self, guard: Guard<'a, Box<AccessBlock>>) -> CacheAccessIter<'a> {
        debug_delay();
        let ptr = self.full_list.swap(std::ptr::null_mut(), Ordering::AcqRel);

        CacheAccessIter {
            guard,
            current_offset: 0,
            current_block: ptr,
        }
    }
}

impl Drop for AccessQueue {
    fn drop(&mut self) {
        debug_delay();
        let writing = self.writing.load(Ordering::Acquire);
        unsafe {
            Box::from_raw(writing);
        }
        debug_delay();
        let mut head = self.full_list.load(Ordering::Acquire);
        while !head.is_null() {
            unsafe {
                debug_delay();
                let next = (*head).next.swap(std::ptr::null_mut(), Ordering::Release);
                Box::from_raw(head);
                head = next;
            }
        }
    }
}

struct CacheAccessIter<'a> {
    guard: Guard<'a, Box<AccessBlock>>,
    current_offset: usize,
    current_block: *mut AccessBlock,
}

impl<'a> Iterator for CacheAccessIter<'a> {
    type Item = CacheAccess;

    fn next(&mut self) -> Option<CacheAccess> {
        while !self.current_block.is_null() {
            let current_block = unsafe { &*self.current_block };

            debug_delay();
            if self.current_offset >= MAX_QUEUE_ITEMS {
                let to_drop_ptr = self.current_block;
                debug_delay();
                self.current_block = current_block.next.load(Ordering::Acquire);
                self.current_offset = 0;
                debug_delay();
                let to_drop = unsafe { Box::from_raw(to_drop_ptr) };
                self.guard.defer_drop(to_drop);
                continue;
            }

            let mut next = 0;

            while next == 0 {
                // we spin here because there's a race between bumping
                // the offset and setting the value to something other
                // than 0 (and 0 is an invalid value)
                debug_delay();
                next = current_block.block[self.current_offset].load(Ordering::Acquire);
            }

            self.current_offset += 1;

            return Some(CacheAccess::from(next));
        }

        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheAccess(u64);

impl From<CacheAccess> for u64 {
    fn from(ca: CacheAccess) -> u64 {
        ca.0
    }
}

impl From<u64> for CacheAccess {
    fn from(u: u64) -> CacheAccess {
        CacheAccess(u)
    }
}

impl CacheAccess {
    #[inline]
    fn pid_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(addr_of!(self.0) as _, 7) }
    }
}

const PARTIAL_PID_MASK: u64 = 0x00FFFFFF_FFFFFFFF;

impl CacheAccess {
    fn size(&self) -> usize {
        probabilistic_unsize((self.0 >> 56) as u8)
    }

    fn pid(&self, shard: u8) -> PageId {
        ((shard as u64) << 56) | (self.0 & PARTIAL_PID_MASK)
    }

    fn new(pid: PageId, sz: usize) -> CacheAccess {
        let size_byte: u64 = (probabilistic_size(sz) as u64) << 56;

        CacheAccess(size_byte | (pid & PARTIAL_PID_MASK))
    }
}

/// A simple eviction manager.
#[derive(Clone)]
pub struct CacheAdvisor {
    shards: Arc<[(AccessQueue, Mutex<Shard>)]>,
    ebr: Ebr<Box<AccessBlock>>,
}

impl CacheAdvisor {
    /// Instantiates a new `CacheAdvisor` eviction manager.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity >= N_SHARDS,
            "Please configure the cache \
             capacity to be at least 256"
        );
        let shard_capacity = capacity / N_SHARDS;

        let mut shards = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            shards.push((
                AccessQueue::default(),
                Mutex::new(Shard::new(shard_capacity)),
            ))
        }

        Self {
            shards: shards.into(),
            ebr: Ebr::default(),
        }
    }

    /// Called when an item is accessed. Returns a Vec of items to be
    /// evicted. Uses flat-combining to avoid blocking on what can
    /// be an asynchronous operation.
    pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> {
        let guard = self.ebr.pin();

        let shards = N_SHARDS as u64;
        let (shard_idx, shifted_pid) = (id % shards, id >> SHARD_BITS);
        let idx: usize = shard_idx.try_into().unwrap();
        let (access_queue, shard_mu): &(AccessQueue, Mutex<Shard>) = &self.shards[idx];

        let cache_access = CacheAccess::new(shifted_pid, cost);
        let filled = access_queue.push(cache_access);

        let mut ret = vec![];
        if filled {
            // only try to acquire this if the access queue has filled
            // an entire segment
            if let Ok(mut shard) = shard_mu.try_lock() {
                let accesses = access_queue.take(guard);
                for item in accesses {
                    let to_evict = shard.accessed(item);
                    // map shard internal offsets to global items ids
                    for cache_access in to_evict {
                        let item = cache_access.pid(u8::try_from(shard_idx).unwrap());
                        let size = cache_access.size();
                        ret.push((item, size));
                    }
                }
            }
        }
        ret
    }
}

#[derive(Eq)]
struct Entry(*mut Node);

unsafe impl Send for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Entry) -> std::cmp::Ordering {
        let left_pid: &[u8] = self.borrow();
        let right_pid: &[u8] = other.borrow();
        left_pid.cmp(&right_pid)
    }
}

impl PartialOrd<Entry> for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        unsafe { (*self.0).pid_bytes() == (*other.0).pid_bytes() }
    }
}

impl BorrowMut<CacheAccess> for Entry {
    fn borrow_mut(&mut self) -> &mut CacheAccess {
        unsafe { &mut *self.0 }
    }
}

impl Borrow<CacheAccess> for Entry {
    fn borrow(&self) -> &CacheAccess {
        unsafe { &*self.0 }
    }
}

impl Borrow<[u8]> for Entry {
    fn borrow(&self) -> &[u8] {
        unsafe { &(*self.0).pid_bytes() }
    }
}

// we only hash on pid, since we will change
// sz sometimes and we access the item by pid
impl Hash for Entry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        unsafe { (*self.0).pid_bytes().hash(hasher) }
    }
}

struct Shard {
    dll: DoublyLinkedList,
    entries: FastSet8<Entry>,
    capacity: usize,
    size: usize,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Self {
            dll: DoublyLinkedList::default(),
            entries: FastSet8::default(),
            capacity,
            size: 0,
        }
    }

    /// `PageId`s in the shard list are indexes of the entries.
    fn accessed(&mut self, cache_access: CacheAccess) -> Vec<CacheAccess> {
        let new_size = cache_access.size();

        if let Some(entry) = self.entries.get(cache_access.pid_bytes()) {
            let old_size = unsafe { (*entry.0).size() };

            // This is a bit hacky but it's done
            // this way because we don't have a way
            // of mutating a key that is in a HashSet.
            //
            // This is safe to do because the hash
            // happens based on the PageId of the
            // CacheAccess, rather than the size
            // that we modify here.
            unsafe { *(*entry.0).inner.get_mut() = cache_access };

            self.size -= old_size;
            self.dll.promote(entry.0);
        } else {
            let ptr = self.dll.push_head(cache_access);
            self.entries.insert(Entry(ptr));
        };

        self.size += new_size;

        let mut to_evict = vec![];

        while self.size > self.capacity {
            if self.dll.len() == 1 {
                // don't evict what we just added
                break;
            }

            let node: Box<Node> = self.dll.pop_tail().unwrap();
            let pid_bytes = node.pid_bytes();
            let node_size = node.size();
            let cache_access: CacheAccess = unsafe { *node.inner.get() };

            assert!(self.entries.remove(pid_bytes));

            to_evict.push(cache_access);

            self.size -= node_size;

            // NB: node is stored in our entries map
            // via a raw pointer, which points to
            // the same allocation used in the DLL.
            // We have to be careful to free node
            // only after removing it from both
            // the DLL and our entries map.
            drop(node);
        }

        to_evict
    }
}

#[test]
fn lru_smoke_test() {
    let mut lru = CacheAdvisor::new(2);
    for i in 0..1000 {
        lru.accessed(i, 16);
    }
}

#[test]
fn lru_access_test() {
    let size = 20667;
    let ci = CacheAccess::new(6, size);
    assert_eq!(ci.size(), 32 * 1024);

    let mut lru = CacheAdvisor::new(4096);

    assert_eq!(lru.accessed(0, size,), vec![]);
    assert_eq!(lru.accessed(2, size,), vec![]);
    assert_eq!(lru.accessed(4, size,), vec![]);
    assert_eq!(lru.accessed(6, size,), vec![]);
    assert_eq!(
        lru.accessed(8, size,),
        vec![(0, size), (2, size), (4, size)]
    );
    assert_eq!(lru.accessed(10, size,), vec![]);
    assert_eq!(lru.accessed(12, size,), vec![]);
    assert_eq!(lru.accessed(14, size,), vec![]);
    assert_eq!(
        lru.accessed(16, size,),
        vec![(6, size), (8, size), (10, size), (12, size)]
    );
    assert_eq!(lru.accessed(18, size,), vec![]);
    assert_eq!(lru.accessed(20, size,), vec![]);
    assert_eq!(lru.accessed(22, size,), vec![]);
    assert_eq!(
        lru.accessed(24, size,),
        vec![(14, size), (16, size), (18, size), (20, size)]
    );
}

#[test]
fn print() {
    let n = 1000;
    let mut trues = 0;
    for _ in 0..n {
        if random_bool() {
            trues += 1;
        }
    }

    assert!(trues > 490 && trues < 510);

    let mut compressed_sizes = vec![];
    let mut true_sum = 0;
    for i in 0..n {
        compressed_sizes.push(probabilistic_size(i));
        true_sum += i;
    }
    let sum: usize = compressed_sizes.into_iter().map(probabilistic_unsize).sum();

    let abs_delta = ((sum as f64 / true_sum as f64) - 1.).abs();

    assert!(abs_delta < 0.05, "delta is actually {}", abs_delta);
}
