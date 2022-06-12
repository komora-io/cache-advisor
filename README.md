# cache-advisor

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
  pub fn new(capacity: usize, entry_percent: u8) -> Self { .. }

  /// Mark items that are accessed with a certain cost.
  /// Returns the items that should be evicted and their associated costs.
  /// The returned costs are always a compressed power of two and may not
  /// be the exact cost that you set for an item. Over time it converges
  /// to a correct value, however.
  pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> { .. }
}
```
