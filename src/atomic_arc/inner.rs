use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicUsize, Ordering};

const BASIC_COUNT_SHIFT: usize = 32;
const STRONG_COUNT_MASK: usize = (-1isize as usize) >> 32;
const MAX_BASIC_COUNT: i32 = std::i32::MAX as _;
const MIN_BASIC_COUNT: i32 = std::i32::MIN as _;
const MAX_STRONG_COUNT: usize = std::u32::MAX as _;

#[repr(align(16))]
/// We abort the process if we overflow one of the two inner counts, but this
/// can only happen if purposefully abusing `std::mem::forget` or other ways
/// of massively leaking `Arc<T>` (basic count overflow) or `AtomicArc<T>`
/// (strong count overflow). Note that the basic count is signed and can
/// underflow, which we'll check as well.
pub(super) struct Inner<T> {
    // We pack both inner counts on one word:
    // bits: 63------32|31-----0
    // data:   strong  |  basic
    counts: AtomicUsize,
    value: T,
}

impl<T> Inner<T> {
    pub(super) fn strong_acquire(&self) {
        let old_counts = self.counts.fetch_add(1, Ordering::Relaxed);
        if old_counts & STRONG_COUNT_MASK == MAX_STRONG_COUNT {
            std::process::abort();
        }
    }

    /// Safety: `inner` must point to a valid `Inner<T>`, and `strong` must not
    /// cause the strong count to underflow.
    pub(super) unsafe fn release(inner: *mut Inner<T>, basic: i32, strong: usize) {
        // Atomically substract `strong` to the strong count and `basic` to the
        // basic count. We rely on the carry on the highest bit being discarded.
        let old_counts = (*inner).counts.fetch_sub(
            strong | ((basic as usize) << BASIC_COUNT_SHIFT),
            Ordering::Release,
        );
        let old_basic = (old_counts >> BASIC_COUNT_SHIFT) as i32;
        let old_strong = old_counts & STRONG_COUNT_MASK;
        if old_basic > MAX_BASIC_COUNT + std::cmp::min(basic, 0) {
            std::process::abort();
        } else if old_basic < MIN_BASIC_COUNT + std::cmp::max(basic, 0) {
            std::process::abort();
        } else if old_basic == basic && old_strong == strong {
            fence(Ordering::Acquire);
            Box::from_raw(inner);
        }
    }
}

/// A thread-safe reference-counting pointer that can be stored in an
/// [AtomicArc<T>](crate::atomic_arc::AtomicArc). An `Arc<T>` can be cheaply
/// cloned.
pub struct Arc<T> {
    inner: NonNull<Inner<T>>,
    _phantom: std::marker::PhantomData<T>,
}

fn check_ptr(ptr: usize) {
    assert_eq!(ptr >> 48, 0);
    assert_eq!(ptr % (1 << super::PTR_SHIFT), 0);
}

impl<T> Arc<T> {
    /// Return a new `Arc<T>`.
    pub fn new(value: T) -> Self {
        let inner = Box::into_raw(Box::new(Inner {
            counts: AtomicUsize::new(1 << BASIC_COUNT_SHIFT),
            value,
        }));

        check_ptr(inner as usize);

        Self {
            inner: unsafe { NonNull::new_unchecked(inner) },
            _phantom: std::marker::PhantomData,
        }
    }

    /// Safety: `inner` must point to a valid `Inner<T>`.
    pub(super) unsafe fn from_inner(inner: *mut Inner<T>) -> Self {
        Self {
            inner: NonNull::new_unchecked(inner),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn inner(&self) -> &Inner<T> {
        unsafe { self.inner.as_ref() }
    }
}

impl<T> std::ops::Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &self.inner.as_ref().value }
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        let old_counts = self
            .inner()
            .counts
            .fetch_add(1 << BASIC_COUNT_SHIFT, Ordering::Relaxed);
        if (old_counts >> BASIC_COUNT_SHIFT) as i32 == MAX_BASIC_COUNT {
            std::process::abort();
        }

        Self {
            inner: self.inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Drop for Arc<T> {
    fn drop(&mut self) {
        unsafe {
            Inner::release(self.inner.as_ptr(), 1, 0);
        }
    }
}

unsafe impl<T: Send> Send for Arc<T> {}
unsafe impl<T: Sync> Sync for Arc<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", **self)
    }
}
