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
        let evicted = cache_advisor.accessed(id * 256, 1);
        if evicted.len() > 0 && evicted.len() != 16 {
            print!("{} ", evicted.len());
        }
    });
}
