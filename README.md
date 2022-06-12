# cache-advisor

API:

```rust
impl CacheAdvisor {
  /// Create a new advisor.
  pub fn new(capacity: usize) -> CacheAdvisor { .. }

  /// Mark items that are accessed with a certain cost.
  /// Returns the items that should be evicted and their associated costs
  pub fn accessed(&mut self, id: u64, cost: usize) -> Vec<(u64, usize)> { .. }
}
```

Implementation details:
* pushes accesses to a local queue
* when the local queue reaches 8 items, pushes each access into one of 256 shards
* each shard is protected by a mutex, but it never blocks to lock it, by using `try_lock`
* if the mutex can't be acquired, the accesses are pushed into an access queue for the shard
* if the mutex can be acquired, the acquiring thread applies accesses from the queue as well
