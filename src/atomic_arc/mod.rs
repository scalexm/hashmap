//! Differential reference counting adapted from [diff]. We use pointer
//! compression to store both a pointer and the outer count on one word, so
//! that we don't need double word atomics: see [wikipedia] on why we can use
//! the 16 highest bits of a pointer to store our outer count on x86_64,
//! and we can also use the 4 lowest bits of the pointer which will be zero
//! thanks to alignment requirements (glibc malloc already aligns pointers on
//! 16 bytes, but for portability with different allocators we do it manually).
//! We'll also pack the two inner counts on one word, for similar reasons.
//!
//! [diff]: http://www.1024cores.net/home/lock-free-algorithms/object-life-time-management/differential-reference-counting/implementation
//! [wikipedia]: https://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details

mod inner;

pub use self::inner::Arc;
use self::inner::Inner;
use std::sync::atomic::{AtomicUsize, Ordering};

const PTR_MASK: usize = (-1isize as usize) >> 20;
const PTR_SHIFT: usize = 4;
const OUTER_COUNT_SHIFT: usize = 44;
const MAX_OUTER_COUNT: usize = 1 << 20 - 1;

/// A type from which one can atomically store and load values of type
/// [Arc<T>](self::Arc).
pub struct AtomicArc<T, P = NonNull> {
    // We store both the pointer and the outer count on one word:
    // bits: 63-------44|43----------------------0
    // data:    count   |   ptr without low bits
    ptr_and_count: AtomicUsize,
    _phantom: std::marker::PhantomData<T>,
    _policy: std::marker::PhantomData<P>,
}

/// An [AtomicArc<T>](self::AtomicArc) with nullable contents.
pub type NullableAtomicArc<T> = AtomicArc<T, Nullable>;

impl<T> AtomicArc<T, NonNull> {
    /// Return a new `AtomicArc<T>`.
    pub fn new(arc: Arc<T>) -> Self {
        let ptr = NonNull::strong_acquire(&arc);

        Self {
            // Initially, the outer count is zero.
            ptr_and_count: AtomicUsize::new(ptr >> PTR_SHIFT),
            _phantom: std::marker::PhantomData,
            _policy: std::marker::PhantomData,
        }
    }
}

impl<T> AtomicArc<T, Nullable> {
    /// Return a new `AtomicArc<T>` with nullable contents.
    pub fn new_nullable(arc: Option<Arc<T>>) -> Self {
        let ptr = Nullable::strong_acquire(&arc);

        Self {
            // Initially, the outer count is zero.
            ptr_and_count: AtomicUsize::new(ptr >> PTR_SHIFT),
            _phantom: std::marker::PhantomData,
            _policy: std::marker::PhantomData,
        }
    }
}

impl<T, P: NullPolicy<T>> AtomicArc<T, P> {
    /// Atomically load an `Arc<T>` from this `AtomicArc<T>`.
    ///
    /// # Notes
    /// If `load` is called `1_048_575` times within a single `AtomicArc`, the
    /// process will be aborted. In practice, this should never happen if care
    /// is taken not to call `load` too many times, as the resulting `Guard<T>`
    /// can be cloned instead.
    pub fn load(&self) -> P::Arc {
        let ptr_and_count = self
            .ptr_and_count
            .fetch_add(1 << OUTER_COUNT_SHIFT, Ordering::Acquire);

        if ptr_and_count >> OUTER_COUNT_SHIFT == MAX_OUTER_COUNT {
            std::process::abort();
        }

        unsafe { P::from_ptr((ptr_and_count & PTR_MASK) << PTR_SHIFT) }
    }

    /// Atomically store an `Arc<T>` to this `AtomicArc<T>`.
    pub fn store(&self, arc: P::Arc) {
        let new_ptr = P::strong_acquire(&arc);

        let old_ptr_and_count = self
            .ptr_and_count
            .swap(new_ptr >> PTR_SHIFT, Ordering::AcqRel);
        unsafe { Self::release(old_ptr_and_count) }
    }

    /// Atomically swap the contents of this `AtomicArc<T>` with another
    /// `Arc<T>`, and return the previous value.
    pub fn swap(&self, arc: P::Arc) -> P::Arc {
        let new_ptr = P::strong_acquire(&arc);

        let old_ptr_and_count = self
            .ptr_and_count
            .swap(new_ptr >> PTR_SHIFT, Ordering::AcqRel)
            // Increment the previous `outer` count before releasing, it has
            // the same effect as `clone`-ing the returned `Arc`.
            .checked_add(1 << OUTER_COUNT_SHIFT)
            .unwrap();

        unsafe {
            Self::release(old_ptr_and_count);
            P::from_ptr((old_ptr_and_count & PTR_MASK) << PTR_SHIFT)
        }
    }

    /// Compare the contents of this `AtomicArc<T>` with `current`, and store
    /// `new` if they are equal. If they are in fact not equal, return `false`.
    pub fn compare_exchange(&self, current: &P::Arc, new: P::Arc) -> bool {
        let new_ptr = P::strong_acquire(&new);
        let current_ptr = P::inner(current);

        let mut old_ptr_and_count = self.ptr_and_count.load(Ordering::Relaxed);
        loop {
            if (old_ptr_and_count & PTR_MASK) << PTR_SHIFT != current_ptr {
                unsafe { P::strong_release(new_ptr) };
                return false;
            }

            match self.ptr_and_count.compare_exchange_weak(
                old_ptr_and_count,
                new_ptr,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(ptr_and_count) => {
                    unsafe { Self::release(ptr_and_count) };
                    return true;
                }
                Err(ptr_and_count) => old_ptr_and_count = ptr_and_count,
            }
        }
    }
}

impl<T, P> AtomicArc<T, P> {
    /// Always handle the null ptr case, so that we can use it in the `Drop`
    /// impl (we cannot specialize it).
    unsafe fn release(ptr_and_count: usize) {
        let inner = ((ptr_and_count & PTR_MASK) << PTR_SHIFT) as *mut Inner<T>;
        let count = ptr_and_count >> OUTER_COUNT_SHIFT;
        if !inner.is_null() {
            Inner::release(inner, -(count as i32), 1);
        }
    }
}

impl<T, P> Drop for AtomicArc<T, P> {
    fn drop(&mut self) {
        unsafe { Self::release(self.ptr_and_count.load(Ordering::Acquire)) }
    }
}

unsafe impl<T: Send> Send for AtomicArc<T> {}
unsafe impl<T: Sync> Sync for AtomicArc<T> {}

pub struct NonNull;
pub struct Nullable;

mod private {
    pub trait Sealed {}
    impl Sealed for super::NonNull {}
    impl Sealed for super::Nullable {}
}

/// Helper trait to handle both `Option<Arc<T>>` and `Arc<T>`. This trait is
/// sealed to prevent downstream users from implementing it.
pub trait NullPolicy<T>: private::Sealed {
    type Arc;

    fn strong_acquire(arc: &Self::Arc) -> usize;
    unsafe fn strong_release(ptr: usize);
    fn inner(arc: &Self::Arc) -> usize;
    unsafe fn from_ptr(ptr: usize) -> Self::Arc;
}

impl<T> NullPolicy<T> for NonNull {
    type Arc = Arc<T>;

    fn strong_acquire(arc: &Arc<T>) -> usize {
        let inner = arc.inner();
        inner.strong_acquire();
        inner as *const _ as usize
    }

    unsafe fn strong_release(ptr: usize) {
        Inner::release(ptr as *mut Inner<T>, 0, 1);
    }

    fn inner(arc: &Arc<T>) -> usize {
        arc.inner() as *const _ as usize
    }

    unsafe fn from_ptr(ptr: usize) -> Arc<T> {
        Arc::from_inner(ptr as *mut Inner<T>)
    }
}

impl<T> NullPolicy<T> for Nullable {
    type Arc = Option<Arc<T>>;

    fn strong_acquire(arc: &Option<Arc<T>>) -> usize {
        arc.as_ref()
            .map(|arc| NonNull::strong_acquire(arc))
            .unwrap_or(0)
    }

    unsafe fn strong_release(ptr: usize) {
        if ptr != 0 {
            Inner::release(ptr as *mut Inner<T>, 0, 1);
        }
    }

    fn inner(arc: &Option<Arc<T>>) -> usize {
        match arc.as_ref() {
            Some(arc) => arc.inner() as *const _ as usize,
            None => 0,
        }
    }

    unsafe fn from_ptr(ptr: usize) -> Option<Arc<T>> {
        if ptr != 0 {
            Some(Arc::from_inner(ptr as *mut Inner<T>))
        } else {
            None
        }
    }
}
