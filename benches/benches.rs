#![feature(test)]

extern crate test;

use test::Bencher;

use cache_evictor::CacheEvictor;

#[bench]
fn test(b: &mut Bencher) {
    let mut cache_evictor = CacheEvictor::new(1024);

    let mut id = 0;
    b.iter(|| {
        id += 1;
        cache_evictor.accessed(id, 3);
    });
}
