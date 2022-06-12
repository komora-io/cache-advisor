use cache_advisor::CacheAdvisor;
use std::sync::atomic;

const OPS: usize = 100_000_000;
const SZ: usize = 9;
const CAP: usize = 1024 * 1024;

static EVICTED_BYTES: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

fn main() {
    let n_threads: usize = std::thread::available_parallelism()
        .unwrap_or(8.try_into().unwrap())
        .get();

    let ops_per_thread: usize = OPS / n_threads;
    let cache_advisor = CacheAdvisor::new(CAP, 80);

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for tn in 0..n_threads {
        let mut cache_advisor = cache_advisor.clone();
        let base = tn * ops_per_thread;
        let thread = std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                let id = base + i;
                let evicted = cache_advisor.accessed_reuse_buffer(id as u64, SZ);
                let cost = evicted.iter().map(|(_id, cost)| cost).sum();
                EVICTED_BYTES.fetch_add(cost, atomic::Ordering::Relaxed);
            }
        });
        threads.push(thread);
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    let evicted = EVICTED_BYTES.load(atomic::Ordering::Acquire);
    let added = OPS * SZ;
    let present = added.saturating_sub(evicted);

    println!(
        "added: {}mb, evicted: {}mb, present: {}kb ({} % above cap). {:.2} million accesses/s",
        added / 1_000_000,
        evicted / 1_000_000,
        present / 1_000,
        (100 * present.saturating_sub(CAP)) / CAP,
        (OPS * 1000) as f64 / before.elapsed().as_millis() as f64 / 1_000_000.,
    );
}
