use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use tempfile::TempDir;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("kvs");

    group.bench_function("sled", |b| {
        b.iter(|| {
            let temp_dir =
                TempDir::new().expect("Failed created temp directory in sled set benchmark");

            {
                let mut sled = SledKvsEngine::open(temp_dir.path())
                    .expect("Failed creating kv store for kvs set benchmark");
                for i in 0..2_000 {
                    sled.set(format!("key{}", i), format!("value{}", i))
                        .expect("Failed setting a value to sled db");
                }
            }

            let mut sled = SledKvsEngine::open(temp_dir.path())
                .expect("Failed creating kv store after setting values");

            for i in 0..2_000 {
                let value = sled
                    .get(format!("key{}", i))
                    .expect("Failed getting value from KVStore")
                    .expect("Key not found in Kvstore after setting it");

                assert_eq!(value, format!("value{}", i));
            }
        })
    });

    group.bench_function("kvstore", |b| {
        b.iter(|| {
            let temp_dir =
                TempDir::new().expect("Failed created temp directory in kvstore set benchmark");

            {
                let mut kvstore = KvStore::open(temp_dir.path())
                    .expect("Failed creating kv store for kvs set benchmark");
                for i in 0..2_000 {
                    kvstore
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("Failed setting value to kvstore");
                }
            }

            let mut kvstore = KvStore::open(temp_dir.path())
                .expect("Failed creating kv store after setting values");

            for i in 0..2_000 {
                let value = kvstore
                    .get(format!("key{}", i))
                    .expect("Failed getting value from KVStore")
                    .expect("Key not found in Kvstore after setting it");

                assert_eq!(value, format!("value{}", i));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
