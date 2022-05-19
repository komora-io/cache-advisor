#![feature(test)]

extern crate test;

use test::Bencher;

use cache_advisor::CacheAdvisor;

#[bench]
fn test(b: &mut Bencher) {
    let mut cache_advisor = CacheAdvisor::new(1024);

    let mut id = 0;
    b.iter(|| {
        id += 1;
        cache_advisor.accessed(id, 3);
    });
}
