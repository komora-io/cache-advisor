use std::{
    borrow::Borrow,
    cell::UnsafeCell,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crossbeam_queue::SegQueue;

mod dll;
//mod dll2;

use crate::dll::{DoublyLinkedList, Node};

const MAX_QUEUE_ITEMS: usize = 8;
const RESIZE_CUTOFF: usize = 8;
const RESIZE_CUTOFF_U8: u8 = RESIZE_CUTOFF as u8;
const N_SHARDS: usize = 256;

// very very simple mutex that reduces instruction cache pollution
struct TryMutex<T> {
    inner: UnsafeCell<T>,
    mu: AtomicBool,
}

impl<T> TryMutex<T> {
    fn new(inner: T) -> TryMutex<T> {
        TryMutex {
            inner: inner.into(),
            mu: false.into(),
        }
    }

    #[inline]
    fn try_lock(&self) -> Option<TryMutexGuard<'_, T>> {
        if self.mu.swap(true, Ordering::Acquire) {
            // already locked
            None
        } else {
            Some(TryMutexGuard { tm: self })
        }
    }
}

struct TryMutexGuard<'a, T> {
    tm: &'a TryMutex<T>,
}

unsafe impl<T: Send> Send for TryMutex<T> {}

unsafe impl<T: Send> Sync for TryMutex<T> {}

impl<'a, T> Drop for TryMutexGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        assert!(self.tm.mu.swap(false, Ordering::Release));
    }
}

impl<'a, T> Deref for TryMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.tm.inner.get() }
    }
}

impl<'a, T> DerefMut for TryMutexGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.tm.inner.get() }
    }
}

#[derive(Clone, Default)]
struct Resizer {
    actual: u128,
    decompressed: u128,
}

impl Resizer {
    /// Returns a compressed size which
    /// has been probabilistically chosen.
    fn compress(&mut self, raw_input: usize) -> u8 {
        if raw_input < RESIZE_CUTOFF {
            return u8::try_from(raw_input).unwrap();
        }

        let upgraded_input = u128::try_from(raw_input).unwrap();
        let po2 = upgraded_input.next_power_of_two();
        let compressed = po2.trailing_zeros() as u8;
        let decompressed = decompress(compressed) as u128;
        self.actual += raw_input as u128;

        let ret = if self.decompressed + decompressed > self.actual {
            compressed - 1
        } else {
            compressed
        };

        self.decompressed += decompress(ret) as u128;

        ret + RESIZE_CUTOFF_U8
    }
}

#[inline]
const fn decompress(input: u8) -> usize {
    match input {
        0..=RESIZE_CUTOFF_U8 => input as usize,
        _ => {
            if let Some(o) = 1_usize.checked_shl((input - RESIZE_CUTOFF_U8) as u32) {
                o
            } else {
                usize::MAX
            }
        }
    }
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

fn _sz_test() {
    let _: [u8; 8] = [0; std::mem::size_of::<CacheAccess>()];
    let _: [u8; 1] = [0; std::mem::align_of::<CacheAccess>()];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheAccess {
    size: u8,
    pid_bytes: [u8; 7],
}

impl CacheAccess {
    fn size(&self) -> usize {
        decompress((self.size) as u8)
    }

    fn pid(&self, shard: u8) -> PageId {
        let mut pid_bytes = [0; 8];
        pid_bytes[1..8].copy_from_slice(&self.pid_bytes);
        pid_bytes[0] = shard;
        PageId::from_le_bytes(pid_bytes)
    }

    fn new(pid: PageId, sz: usize, rng: &mut Resizer) -> CacheAccess {
        let size = rng.compress(sz);

        let mut pid_bytes = [0; 7];
        pid_bytes.copy_from_slice(&pid.to_le_bytes()[1..8]);

        CacheAccess { size, pid_bytes }
    }
}

/// A simple eviction manager with 256 shards.
#[derive(Clone)]
pub struct CacheAdvisor {
    shards: Arc<[TryMutex<Shard>]>,
    access_queues: Arc<[SegQueue<CacheAccess>]>,
    local_queue: Vec<(u64, usize)>,
    rng: Resizer,
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
            shards.push(TryMutex::new(Shard::new(shard_capacity)))
        }

        let mut access_queues = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            access_queues.push(SegQueue::default());
        }

        Self {
            shards: shards.into(),
            access_queues: access_queues.into(),
            local_queue: Vec::with_capacity(MAX_QUEUE_ITEMS),
            rng: Resizer::default(),
        }
    }

    /// Called when an item is accessed. Returns a Vec of items to be
    /// evicted. Avoids blocking under contention by using flat-combining
    /// on 256 LRU shards.
    pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> {
        self.local_queue.push((id, cost));

        let mut ret = vec![];

        if self.local_queue.len() < MAX_QUEUE_ITEMS {
            return ret;
        }

        while let Some((id, cost)) = self.local_queue.pop() {
            let shard_idx = (id.to_le_bytes()[0] as u64 % N_SHARDS as u64) as usize;
            let shard_mu = &self.shards[shard_idx];
            let access_queue = &self.access_queues[shard_idx];
            let cache_access = CacheAccess::new(id, cost, &mut self.rng);

            // use flat-combining to avoid lock contention
            if let Some(mut shard) = shard_mu.try_lock() {
                // we take len here and bound pops to this number
                // because we don't want to keep going forever
                // if new items are flowing in - we need to get
                // back to our own work eventually.
                for _ in 0..access_queue.len() {
                    if let Some(queued_cache_access) = access_queue.pop() {
                        shard.accessed(queued_cache_access, shard_idx, &mut ret);
                    }
                }

                shard.accessed(cache_access, shard_idx, &mut ret);
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
        let left_pid: &[u8; 7] = self.borrow();
        let right_pid: &[u8; 7] = other.borrow();
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
        unsafe { (*self.0).pid_bytes == (*other.0).pid_bytes }
    }
}

impl Borrow<[u8; 7]> for Entry {
    fn borrow(&self) -> &[u8; 7] {
        unsafe { &(*self.0).pid_bytes }
    }
}

// we only hash on pid, since we will change
// sz sometimes and we access the item by pid
impl Hash for Entry {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        unsafe { (*self.0).pid_bytes.hash(hasher) }
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

    fn accessed(
        &mut self,
        cache_access: CacheAccess,
        shard_idx: usize,
        ret: &mut Vec<(u64, usize)>,
    ) {
        if let Some(entry) = self.entries.get(&cache_access.pid_bytes) {
            let old_size = unsafe { (*entry.0).size() };
            self.size -= old_size;

            // This is a bit hacky but it's done
            // this way because HashSet doesn't have
            // a get_mut method.
            //
            // This is safe to do because the hash
            // happens based on the PageId of the
            // CacheAccess, rather than the size
            // that we modify here.
            unsafe { (*entry.0).inner.get_mut().size = cache_access.size };

            self.dll.promote(entry.0);
        } else {
            let ptr = self.dll.push_head(cache_access);
            self.entries.insert(Entry(ptr));
        };

        let new_size = cache_access.size();
        self.size += new_size;

        while self.size > self.capacity {
            if self.dll.len() == 1 {
                // don't evict what we just added
                break;
            }

            let node: Box<Node> = self.dll.pop_tail().unwrap();
            let pid_bytes = node.pid_bytes;
            let node_size = node.size();
            let eviction_cache_access: CacheAccess = unsafe { *node.inner.get() };

            let item = eviction_cache_access.pid(u8::try_from(shard_idx).unwrap());
            let size = eviction_cache_access.size();
            ret.push((item, size));

            assert!(self.entries.remove(&pid_bytes));

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
    let mut lru = CacheAdvisor::new(256);
    let mut evicted = 0;
    for i in 0..10_000 {
        evicted += lru.accessed(i, 16).len();
    }
    assert!(evicted > 9700, "only evicted {} items", evicted);
}

#[test]
fn probabilistic_sum() {
    let mut rng = Resizer::default();
    let mut resized = 0;
    let mut actual = 0;
    for i in 0..1000 {
        let compressed = rng.compress(i);
        let decompressed = decompress(compressed);
        resized += decompressed;
        actual += i;
    }

    let abs_delta = ((resized as f64 / actual as f64) - 1.).abs();

    assert!(abs_delta < 0.005, "delta is actually {}", abs_delta);
}

#[test]
fn probabilistic_ev() {
    let mut rng = Resizer::default();

    for i in 0..1024 {
        let mut resized = 0;
        let mut actual = 0;
        for _ in 1..10000 {
            let compressed = rng.compress(i);
            let decompressed = decompress(compressed);
            resized += decompressed;
            actual += i;
        }

        if i == 0 {
            assert_eq!(actual, 0);
            assert_eq!(resized, 0);
        } else {
            let abs_delta = ((resized as f64 / actual as f64) - 1.).abs();
            assert!(
                abs_delta < 0.005,
                "delta is actually {} for inputs of size {}. actual: {} round-trip: {}",
                abs_delta,
                i,
                actual,
                resized
            );
        }
    }
}

#[test]
fn probabilistic_n() {
    const N: usize = 15;

    let mut rng = Resizer::default();
    let mut resized = 0;
    let mut actual = 0;

    for _ in 0..1000 {
        let compressed = rng.compress(N);
        let decompressed = decompress(compressed);
        resized += decompressed;
        actual += N;
    }

    let abs_delta = ((resized as f64 / actual as f64) - 1.).abs();

    assert!(abs_delta < 0.005, "delta is actually {}", abs_delta);
}
