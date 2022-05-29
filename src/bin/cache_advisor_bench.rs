use cache_advisor::CacheAdvisor;
use std::sync::atomic;

const N_THREADS: usize = 8;
const OPS: usize = 100_000_000;
const OPS_PER_THREAD: usize = OPS / N_THREADS;
const SZ: usize = 1;
const CAP: usize = 1024 * 1024;

static EVICTED_BYTES: atomic::AtomicUsize = atomic::AtomicUsize::new(0);

fn main() {
    let cache_advisor = CacheAdvisor::new(CAP);

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for tn in 0..N_THREADS {
        let mut cache_advisor = cache_advisor.clone();
        let base = tn * OPS_PER_THREAD;
        let thread = std::thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
                let id = base + i;
                let evicted = cache_advisor.accessed(id as u64, SZ);
                let cost = evicted.iter().map(|(_id, cost)| cost).sum();
                EVICTED_BYTES.fetch_add(cost, atomic::Ordering::Release);
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
        "added: {}mb, evicted: {}mb, present: {}mb ({} % above cap). {:.2} million accesses/s",
        added / 1_000_000,
        evicted / 1_000_000,
        present / 1_000_000,
        (100 * present.saturating_sub(CAP)) / CAP,
        OPS as f64 / before.elapsed().as_secs() as f64 / 1_000_000.,
    );
}
