use super::{Resizer, DEPTH_TRESHOLD, MIN_LOAD_FACTOR_FOR_RESIZE, N};
use crate::atomic_arc::{Arc, AtomicArc, NullableAtomicArc};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

struct Entry<K, V> {
    key: K,
    value: NullableAtomicArc<V>,
}

#[repr(C)]
#[repr(align(64))]
pub(super) struct VirtualBucket<K, V> {
    hashes: [AtomicU64; N],
    next: AtomicPtr<VirtualBucket<K, V>>,
    entries: [AtomicPtr<Entry<K, V>>; N],
}

impl<K, V> Default for VirtualBucket<K, V> {
    fn default() -> Self {
        Self {
            hashes: Default::default(),
            next: Default::default(),
            entries: Default::default(),
        }
    }
}

impl<K, V> VirtualBucket<K, V> {
    pub(super) fn alloc(size: usize) -> std::sync::Arc<[VirtualBucket<K, V>]> {
        (0..size)
            .map(|_| Default::default())
            .collect::<Vec<Self>>()
            .into()
    }

    fn find_hash(&self, hash: u64, start: usize) -> Option<usize> {
        for j in start..N {
            if self.hashes[j].load(Ordering::Relaxed) == hash {
                return Some(j);
            }
        }
        None
    }
}

pub(super) struct ResizeNeeded;

impl<K: Eq, V> VirtualBucket<K, V> {
    pub(super) fn insert(
        &self,
        hash: u64,
        mut key: K,
        mut value: Arc<V>,
        is_new_item: bool,
        load_factor: f32,
        depth: i32,
    ) -> Result<bool, ResizeNeeded> {
        for j in 0..N {
            let mut entry = self.entries[j].load(Ordering::SeqCst);
            if entry.is_null() {
                match self.hashes[j].compare_exchange(0, hash, Ordering::AcqRel, Ordering::Relaxed)
                {
                    Ok(..) => (),
                    Err(actual_hash) if actual_hash == hash => (),
                    Err(..) => continue,
                }

                let new_entry = Box::into_raw(Box::new(Entry {
                    key,
                    value: AtomicArc::new_nullable(Some(value)),
                }));

                match self.entries[j].compare_exchange(
                    std::ptr::null_mut(),
                    new_entry,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(..) => return Ok(true),
                    Err(actual_entry) => {
                        entry = actual_entry;
                        let failed_entry = unsafe { Box::from_raw(new_entry) };
                        key = failed_entry.key;
                        value = failed_entry.value.load().unwrap();
                    }
                }
            }

            assert!(!entry.is_null());
            let entry = unsafe { &*entry };

            if self.hashes[j].load(Ordering::SeqCst) != hash || entry.key != key {
                continue;
            } else if !is_new_item {
                return Ok(false);
            }
            entry.value.store(Some(value));
            return Ok(false);
        }

        if load_factor >= MIN_LOAD_FACTOR_FOR_RESIZE && depth >= DEPTH_TRESHOLD {
            return Err(ResizeNeeded);
        }

        let mut next_ptr = self.next.load(Ordering::SeqCst);
        if next_ptr.is_null() {
            let new_next = Box::into_raw(Box::new(VirtualBucket::default()));
            match self.next.compare_exchange(
                next_ptr,
                new_next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(ptr) => next_ptr = ptr,
                Err(ptr) => next_ptr = ptr,
            };
        }

        assert!(!next_ptr.is_null());
        unsafe { &*next_ptr }.insert(hash, key, value, is_new_item, load_factor, depth + 1)
    }

    pub(super) fn remove(&self, hash: u64, key: &K) {
        let mut start = 0;
        while let Some(pos) = self.find_hash(hash, start) {
            let entry = self.entries[pos].load(Ordering::SeqCst);
            if !entry.is_null() && unsafe { (*entry).key == *key } {
                unsafe { (*entry).value.store(None) };
                return;
            }
            start = pos + 1;
        }

        let next_ptr = self.next.load(Ordering::SeqCst);
        if !next_ptr.is_null() {
            unsafe { &*next_ptr }.remove(hash, key);
        }
    }

    pub(super) fn get(&self, hash: u64, key: &K) -> Option<Arc<V>> {
        let mut start = 0;
        while let Some(pos) = self.find_hash(hash, start) {
            let entry = self.entries[pos].load(Ordering::SeqCst);
            if !entry.is_null() && unsafe { (*entry).key == *key } {
                return unsafe { (*entry).value.load() };
            }
            start = pos + 1;
        }

        let next_ptr = self.next.load(Ordering::SeqCst);
        if !next_ptr.is_null() {
            unsafe { &*next_ptr }.get(hash, key)
        } else {
            None
        }
    }
}

impl<K: Clone + Eq, V> VirtualBucket<K, V> {
    pub(super) fn copy_to(&self, resizer: &Resizer<K, V>) -> u64 {
        let mut removed = 0;
        for (entry, hash) in self.entries.iter().zip(self.hashes.iter()) {
            let entry = entry.load(Ordering::SeqCst);
            if !entry.is_null() {
                let entry = unsafe { &*entry };
                let hash = hash.load(Ordering::SeqCst);
                match entry.value.load() {
                    Some(value) => assert!(resizer
                        .hash_into(hash)
                        .insert(hash, entry.key.clone(), value, false, 0., 1)
                        .is_ok()),
                    None => removed += 1,
                }
            }
        }
        removed
    }
}

impl<K, V> Drop for VirtualBucket<K, V> {
    fn drop(&mut self) {
        let ptr = self.next.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }

        for bucket in &self.entries {
            let ptr = bucket.load(Ordering::SeqCst);
            if !ptr.is_null() {
                unsafe {
                    Box::from_raw(ptr);
                }
            }
        }
    }
}
