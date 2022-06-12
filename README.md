# cache-advisor

[docs](https://docs.rs/cache-advisor)

Tells you when to evict items from a cache. Should be able to sustain
dozens of millions of accesses per second on modern server hardware
without any blocking.

# features

* two-segment LRU, protects against cache pollution from single-hit items
* 256 shards accessed via non-blocking flatcombining
* local access buffer that must fill up before accessing shared state
* compresses the costs associated with each item to a `u8` using a compression
  technique that will converge to the overall true sum of costs over time, but
  allows for much less memory to be used for accounting.

# api

```rust
impl CacheAdvisor {
  /// Instantiates a new two-segment `CacheAdvisor` eviction manager.
  ///
  /// Choose an overall size and the percentage 0..=100 that should
  /// be devoted to the entry cache. 20% is a safe default.
  pub fn new(capacity: usize, entry_percent: u8) -> CacheAdvisor { .. }

  /// Mark items that are accessed with a certain cost.
  /// Returns the items that should be evicted and their associated costs.
  /// The returned costs are always a compressed power of two and may not
  /// be the exact cost that you set for an item. Over time it converges
  /// to a correct value, however.
  pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> { .. }

  /// Similar to `accessed` except this will reuse an internal vector for storing
  /// items to be evicted, which will be passed by reference to callers. If the
  /// returned slice is huge and you would like to reclaim underlying memory, call
  /// the `reset_internal_access_buffer` method. This can improve throughput by around
  /// 10% in some cases compared to the simpler `accessed` method above (which may
  /// need to copy items several times as the returned vector is expanded).
  pub fn accessed_reuse_buffer(&mut self, id: u64, cost: usize) -> &[(u64, usize)] { .. }

  /// Resets the internal access buffer, freeing any memory it may have been holding
  /// onto. This should only be called in combination with `accessed_reuse_buffer` if
  /// you want to release the memory that the internal buffer may be consuming. You
  /// probably don't need to call this unless the previous slice returned by
  /// `accessed_reuse_buffer` is over a few thousand items long, if not an order of magnitude
  /// or two larger than that, which should ideally be rare events in workloads where
  /// most items being inserted are somewhat clustered in size.
  pub fn reset_internal_access_buffer(&mut self) { .. }
}
```
