use std::{
    borrow::{Borrow, BorrowMut},
    hash::{Hash, Hasher},
    num::Wrapping,
    ptr::addr_of,
    sync::{Arc, Mutex},
};

use crossbeam_queue::SegQueue;

mod dll;

use crate::dll::{DoublyLinkedList, Node};

#[derive(Clone)]
struct Rng(Wrapping<u32>);

impl Rng {
    #[inline]
    fn bool_with_probability(&mut self, pct: u8) -> bool {
        assert!(pct <= 100);

        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;

        self.0 = x;

        let rand = ((x.0 as u64).wrapping_mul(100) >> 32) as u32;

        rand < pct as u32
    }

    /// Returns a compressed size which
    /// has been probabilistically chosen.
    fn probabilistic_size(&mut self, input: usize) -> u8 {
        // TODO shift po2 by expected value proportional
        // to error
        let po2 = (input + 1).next_power_of_two();
        let err = po2 - (input + 1);
        let probability_to_downshift = (err * 200) / po2;

        assert!(probability_to_downshift < 100);

        let maybe_downshifted = if self.bool_with_probability(probability_to_downshift as u8) {
            assert_ne!(probability_to_downshift, 0);
            po2 >> 1
        } else {
            po2
        };

        maybe_downshifted.trailing_zeros() as u8
    }
}

fn probabilistic_unsize(input: u8) -> usize {
    (1 << input) - 1
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

pub(crate) type FnvSet8<V> = std::collections::HashSet<V, std::hash::BuildHasherDefault<Fnv>>;

type PageId = u64;

//#[cfg(any(test, feature = "lock_free_delays"))]
// const MAX_QUEUE_ITEMS: usize = 4;

//#[cfg(any(test, feature = "lock_free_delays"))]
// const N_SHARDS: usize = 2;

// #[cfg(not(any(test, feature = "lock_free_delays")))]
const MAX_QUEUE_ITEMS: usize = 16;

//#[cfg(not(any(test, feature = "lock_free_delays")))]
const N_SHARDS: usize = 256;

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

    fn new(pid: PageId, sz: usize, rng: &mut Rng) -> CacheAccess {
        let size_byte: u64 = (rng.probabilistic_size(sz) as u64) << 56;

        CacheAccess(size_byte | (pid & PARTIAL_PID_MASK))
    }
}

/// A simple eviction manager.
#[derive(Clone)]
pub struct CacheAdvisor {
    shards: Arc<[Mutex<Shard>]>,
    access_queues: Arc<[SegQueue<CacheAccess>]>,
    local_queue: Vec<(u64, usize)>,
    rng: Rng,
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
            shards.push(Mutex::new(Shard::new(shard_capacity)))
        }

        let mut access_queues = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            access_queues.push(SegQueue::default());
        }

        Self {
            shards: shards.into(),
            access_queues: access_queues.into(),
            local_queue: Vec::with_capacity(MAX_QUEUE_ITEMS),
            rng: Rng(Wrapping(1406868647)),
        }
    }

    /// Called when an item is accessed. Returns a Vec of items to be
    /// evicted. Uses flat-combining to avoid blocking on what can
    /// be an asynchronous operation.
    pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> {
        self.local_queue.push((id, cost));

        let mut ret = vec![];

        if self.local_queue.len() < MAX_QUEUE_ITEMS {
            return ret;
        }

        let local_queue =
            std::mem::replace(&mut self.local_queue, Vec::with_capacity(MAX_QUEUE_ITEMS));

        for (id, cost) in local_queue {
            let shard_idx = (id % N_SHARDS as u64) as usize;
            let shard_mu = &self.shards[shard_idx];
            let access_queue = &self.access_queues[shard_idx];
            let cache_access = CacheAccess::new(id, cost, &mut self.rng);

            // use flat-combining to avoid lock contention
            if let Ok(mut shard) = shard_mu.try_lock() {
                shard.accessed(cache_access, shard_idx, &mut ret);

                // we take len here and bound pops to this number
                // because we don't want to keep going forever
                // if new items are flowing in - we need to get
                // back to our own work eventually.
                for _ in 0..access_queue.len() {
                    if let Some(queued_cache_access) = access_queue.pop() {
                        shard.accessed(queued_cache_access, shard_idx, &mut ret);
                    }
                }
            } else {
                access_queue.push(cache_access);
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
    entries: FnvSet8<Entry>,
    capacity: usize,
    size: usize,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "shard capacity must be non-zero");

        Self {
            dll: DoublyLinkedList::default(),
            entries: FnvSet8::default(),
            capacity,
            size: 0,
        }
    }

    /// `PageId`s in the shard list are indexes of the entries.
    fn accessed(
        &mut self,
        cache_access: CacheAccess,
        shard_idx: usize,
        ret: &mut Vec<(u64, usize)>,
    ) {
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

        while self.size > self.capacity {
            if self.dll.len() == 1 {
                // don't evict what we just added
                break;
            }

            let node: Box<Node> = self.dll.pop_tail().unwrap();
            let pid_bytes = node.pid_bytes();
            let node_size = node.size();
            let eviction_cache_access: CacheAccess = unsafe { *node.inner.get() };

            assert!(self.entries.remove(pid_bytes));

            let item = eviction_cache_access.pid(u8::try_from(shard_idx).unwrap());
            let size = eviction_cache_access.size();
            ret.push((item, size));

            self.size -= node_size;

            // NB: node is stored in our entries map
            // via a raw pointer, which points to
            // the same allocation used in the DLL.
            // We have to be careful to free node
            // only after removing it from both
            // the DLL and our entries map.
            drop(node);
        }
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
fn probabilistic_ev() {
    let mut rng = Rng(Wrapping(1406868647));
    let mut resized = 0;
    let mut actual = 0;
    for i in 0..1000 {
        let compressed = rng.probabilistic_size(i);
        let decompressed = probabilistic_unsize(compressed);
        resized += decompressed;
        actual += i;
    }

    let abs_delta = ((resized as f64 / actual as f64) - 1.).abs();

    assert!(abs_delta < 0.0001, "delta is actually {}", abs_delta);
}
