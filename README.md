# berghain

an efficient scan-resistant cache manager for hot objects.

this is not a cache. it is just the thing that tells you what can stay and what needs to go.

API:

```rust
/// create a bouncer that will lazily evict
/// items after crossing the 4096 cost limit
let mut bouncer = berghain::Bouncer::new(4096);

impl Bouncer {
  /// returns the items that should be evicted
  pub fn accessed(
    &mut self,
    item: u64,
    cost: usize
  ) -> Vec<u64>
}
```
