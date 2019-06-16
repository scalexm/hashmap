mod virtual_bucket;

use self::virtual_bucket::{ResizeNeeded, VirtualBucket};
use crate::atomic_arc::{Arc, AtomicArc, NullableAtomicArc};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

pub use fxhash::FxBuildHasher as DefaultBuildHasher;

pub struct HashMap<K, V, S = DefaultBuildHasher> {
    table: AtomicArc<Buckets<K, V>>,
    items: AtomicU64,
    hash_builder: S,
}

const MIN_LOAD_FACTOR_FOR_RESIZE: f32 = 0.5;
const DEPTH_TRESHOLD: i32 = 1;
const N: usize = 7;

impl<K, V> HashMap<K, V> {
    pub fn new() -> Self {
        Self {
            table: AtomicArc::new(Arc::new(Buckets::new(1))),
            items: AtomicU64::new(0),
            hash_builder: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher> HashMap<K, V, S> {
    fn hash(&self, key: &K) -> u64 {
        let mut state = self.hash_builder.build_hasher();
        key.hash(&mut state);
        match state.finish() {
            0 => 1,
            hash => hash,
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let table = self.table.load();
        let hash = self.hash(&key);

        let value = Arc::new(value);
        let f = (self.items.load(Ordering::Relaxed) as f32) / (table.buckets.len() * N) as f32;
        let new_table =
            match table
                .hash_into(hash)
                .insert(hash, key.clone(), value.clone(), true, f, 1)
            {
                Ok(inserted) => {
                    if inserted {
                        self.items.fetch_add(1, Ordering::Relaxed);
                    }
                    match table.resizer.load() {
                        Some(resizer) => Buckets::resize_with_pending_update(
                            &table,
                            &resizer,
                            hash,
                            PendingUpdate::Reinsert(key, value),
                            &self.items,
                        ),
                        None => return,
                    }
                }
                Err(ResizeNeeded) => {
                    let old_size = table.buckets.len();
                    let new_size = 2 * old_size;
                    let new_helper = Resizer::new(new_size, old_size);
                    table.resizer.try_store(&None, Some(Arc::new(new_helper)));

                    Buckets::resize_with_pending_update(
                        &table,
                        &table.resizer.load().unwrap(),
                        hash,
                        PendingUpdate::Insert(key, value),
                        &self.items,
                    )
                }
            };

        if let Some(new_table) = new_table {
            self.table.try_store(&table, Arc::new(new_table));
        }
    }

    pub fn remove(&self, key: &K) {
        let table = self.table.load();
        let hash = self.hash(&key);

        table.hash_into(hash).remove(hash, key);

        if let Some(resizer) = table.resizer.load() {
            let new_table = Buckets::resize_with_pending_update(
                &table,
                &resizer,
                hash,
                PendingUpdate::Remove(key),
                &self.items,
            );

            if let Some(new_table) = new_table {
                self.table.try_store(&table, Arc::new(new_table));
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let table = self.table.load();
        let hash = self.hash(&key);
        table.hash_into(hash).get(hash, key)
    }
}

struct Resizer<K, V> {
    buckets: std::sync::Arc<[VirtualBucket<K, V>]>,
    markers: Vec<AtomicU8>,
}

const CHUNK_SIZE: usize = 8;

impl<K, V> Resizer<K, V> {
    fn new(size: usize, old_size: usize) -> Self {
        let chunks = (old_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        Self {
            buckets: VirtualBucket::alloc(size),
            markers: (0..chunks).map(|_| AtomicU8::new(0)).collect(),
        }
    }

    fn hash_into(&self, hash: u64) -> &VirtualBucket<K, V> {
        &self.buckets[(hash as usize) & (self.buckets.len() - 1)]
    }
}

struct Buckets<K, V> {
    buckets: std::sync::Arc<[VirtualBucket<K, V>]>,
    resizer: NullableAtomicArc<Resizer<K, V>>,
}

impl<K, V> Buckets<K, V> {
    fn new_with_buckets(buckets: std::sync::Arc<[VirtualBucket<K, V>]>) -> Self {
        Self {
            buckets,
            resizer: AtomicArc::new_nullable(None),
        }
    }

    fn new(size: usize) -> Self {
        Self::new_with_buckets(VirtualBucket::alloc(size))
    }

    fn hash_into(&self, hash: u64) -> &VirtualBucket<K, V> {
        &self.buckets[(hash as usize) & (self.buckets.len() - 1)]
    }
}

enum PendingUpdate<'a, K, V> {
    Reinsert(K, Arc<V>),
    Insert(K, Arc<V>),
    Remove(&'a K),
}

impl<K: Eq + Clone, V> Buckets<K, V> {
    fn copy_chunk_to(&self, chunk: usize, dst: &Resizer<K, V>) -> u64 {
        let mut removed = 0;
        let lower = chunk * CHUNK_SIZE;
        let upper = std::cmp::min(lower + CHUNK_SIZE, self.buckets.len());
        for j in lower..upper {
            removed += self.buckets[j].copy_to(dst);
        }
        removed
    }

    fn resize_with_pending_update(
        old_table: &Arc<Buckets<K, V>>,
        resizer: &Arc<Resizer<K, V>>,
        hash: u64,
        update: PendingUpdate<'_, K, V>,
        items: &AtomicU64,
    ) -> Option<Buckets<K, V>> {
        let virtual_bucket = resizer.hash_into(hash);
        match update {
            PendingUpdate::Insert(key, value) => {
                match virtual_bucket.insert(hash, key, value, true, 0., 1) {
                    Ok(true) => items.fetch_add(1, Ordering::Relaxed),
                    Ok(false) => 0,
                    Err(..) => panic!("load factor = 0."),
                };
            }
            PendingUpdate::Reinsert(key, value) => {
                assert!(virtual_bucket.insert(hash, key, value, true, 0., 1).is_ok());
            }
            PendingUpdate::Remove(key) => {
                virtual_bucket.remove(hash, key);
            }
        }

        for (chunk, marker) in resizer.markers.iter().enumerate() {
            match marker.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(..) => {
                    items.fetch_sub(old_table.copy_chunk_to(chunk, &resizer), Ordering::Relaxed);
                    marker.store(2, Ordering::Release);
                }
                Err(..) => continue,
            }
        }

        for marker in resizer.markers.iter() {
            if marker.load(Ordering::Acquire) != 2 {
                return None;
            }
        }

        Some(Buckets::new_with_buckets(resizer.buckets.clone()))
    }
}
