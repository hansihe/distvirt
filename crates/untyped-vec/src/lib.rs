//! A type-erased growable buffer that can be temporarily viewed as a typed
//! `Vec<T>`-like container for any concrete `T`.
//!
//! # Usage
//!
//! ```
//! use untyped_vec::UntypedVec;
//!
//! let mut buf = UntypedVec::new();
//!
//! // Use as a Vec<u32>:
//! {
//!     let mut view = buf.typed_view::<u32>();
//!     view.push(1);
//!     view.push(2);
//!     assert_eq!(&*view, &[1, 2]);
//!     // Elements are dropped when `view` goes out of scope.
//! }
//!
//! // Reuse the same buffer as a Vec<u64> — the allocation is reused,
//! // alignment is upgraded automatically if needed:
//! {
//!     let mut view = buf.typed_view::<u64>();
//!     view.push(42);
//!     assert_eq!(view[0], 42);
//! }
//! ```
//!
//! # Design
//!
//! The core invariant: **between typed views, the buffer holds no live
//! values** — it's just raw bytes. A [`TypedView`] borrows the buffer
//! mutably, so only one typed view can exist at a time, and it is
//! responsible for dropping all `T` values before releasing the borrow.

use std::alloc::{self, Layout};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

/// A type-erased growable buffer that can be temporarily viewed as a `Vec<T>`
/// for any concrete `T`.
///
/// Call [`typed_view`](Self::typed_view) to obtain a [`TypedView`] that
/// supports push, pop, indexing, and slice operations. The view borrows
/// the buffer mutably, so the borrow checker enforces that only one view
/// exists at a time.
///
/// The buffer's allocation is reused across views. Alignment is
/// monotonically upgraded (never downgraded) to accommodate different types.
///
/// # Internal invariants
///
/// These are upheld by all methods and relied upon by unsafe code:
///
/// - `align` is always a power of two (≥ 1).
/// - If `cap_bytes == 0`, no allocation exists and `ptr` is dangling.
/// - If `cap_bytes > 0`, `ptr` points to a live allocation of exactly
///   `cap_bytes` bytes with alignment `align`, allocated via the global
///   allocator.
/// - Between typed views (i.e. when no `TypedView` exists), the buffer
///   contains **no live values** — only uninitialized bytes. This means
///   `upgrade_alignment` can deallocate without running destructors, and
///   `Drop` for `UntypedVec` only needs to free the allocation.
pub struct UntypedVec {
    /// Pointer to the allocation, or dangling if `cap_bytes == 0`.
    ptr: NonNull<u8>,
    /// Size of the current allocation in bytes. 0 means no allocation.
    cap_bytes: usize,
    /// Alignment of the current allocation. Always a power of two.
    /// Only increases over the lifetime of the buffer.
    align: usize,
}

// SAFETY: `UntypedVec` owns its allocation and contains no live values
// between views. It is conceptually equivalent to a `Vec<u8>` (which is
// Send + Sync). The borrow checker prevents a `TypedView` from existing
// on another thread while the `UntypedVec` is accessed — the `&mut self`
// borrow in `typed_view()` ensures exclusivity.
unsafe impl Send for UntypedVec {}
unsafe impl Sync for UntypedVec {}

/// A typed, mutable view into an [`UntypedVec`]. Behaves like a `Vec<T>`
/// but backed by the untyped buffer.
///
/// Created via [`UntypedVec::typed_view`]. The view starts empty (len = 0)
/// regardless of what previous views stored. Supports push, pop, indexing,
/// and all `[T]` slice operations via `Deref`/`DerefMut`.
///
/// On drop, all remaining elements are properly destructed. The underlying
/// allocation is **not** freed — it remains available for future views.
///
/// # Internal invariants
///
/// - `self.len` elements of type `T` are live (initialized) starting at
///   `self.buf.ptr`, laid out contiguously with no padding between elements
///   (same as a `[T]` slice).
/// - `self.buf.align >= align_of::<T>()` (ensured at view creation).
/// - `self.len * size_of::<T>() <= self.buf.cap_bytes` (ensured by push/reserve).
pub struct TypedView<'a, T> {
    buf: &'a mut UntypedVec,
    len: usize,
    _marker: PhantomData<T>,
}

// ---------------------------------------------------------------------------
// UntypedVec
// ---------------------------------------------------------------------------

impl UntypedVec {
    /// Create a new empty buffer. No allocation is performed until the first push.
    pub fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            cap_bytes: 0,
            align: 1,
        }
    }

    /// Create a new buffer with `cap_bytes` pre-allocated at the given
    /// alignment.
    ///
    /// If `cap_bytes` is 0, no allocation is performed. The `align` must be
    /// a power of two. Note that `cap_bytes` is used as-is (not rounded up) —
    /// if you want capacity for N elements of type T, pass
    /// `N * size_of::<T>()` and `align_of::<T>()`.
    ///
    /// # Panics
    ///
    /// Panics if `align` is not a power of two, or if the resulting layout
    /// is invalid (e.g. size overflows when rounded up for alignment).
    pub fn with_capacity(cap_bytes: usize, align: usize) -> Self {
        assert!(align.is_power_of_two(), "alignment must be a power of two");
        if cap_bytes == 0 {
            return Self {
                ptr: NonNull::dangling(),
                cap_bytes: 0,
                align,
            };
        }
        let layout = Layout::from_size_align(cap_bytes, align).expect("invalid layout");
        // SAFETY: layout has non-zero size (cap_bytes > 0 checked above).
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        // SAFETY: alloc returned non-null (checked above).
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            cap_bytes,
            align,
        }
    }

    /// Obtain a typed view of this buffer as a `Vec<T>`-like container.
    ///
    /// The returned [`TypedView`] borrows `self` mutably, so no other views
    /// can exist simultaneously (enforced by the borrow checker). The view
    /// always starts empty — previous contents are not preserved across views.
    ///
    /// If `T` requires stricter alignment than the current allocation, the
    /// buffer is reallocated (without preserving data, since the view starts
    /// empty). Alignment is never downgraded.
    pub fn typed_view<T>(&mut self) -> TypedView<'_, T> {
        // ZSTs need no allocation or alignment work.
        if std::mem::size_of::<T>() != 0 {
            let required_align = std::mem::align_of::<T>();
            if required_align > self.align {
                self.upgrade_alignment(required_align);
            }
        }
        TypedView {
            buf: self,
            len: 0,
            _marker: PhantomData,
        }
    }

    /// The current capacity of the backing buffer in bytes.
    pub fn capacity_bytes(&self) -> usize {
        self.cap_bytes
    }

    /// The alignment of the current allocation.
    pub fn alignment(&self) -> usize {
        self.align
    }

    /// Reallocate the backing buffer with a stricter alignment, without
    /// preserving contents.
    ///
    /// # Safety contract (internal)
    ///
    /// This must only be called when **no live values** exist in the buffer
    /// (i.e. no `TypedView` is active). Currently this is guaranteed because
    /// the only call site is `typed_view()`, which creates a fresh view
    /// with `len = 0`.
    fn upgrade_alignment(&mut self, new_align: usize) {
        debug_assert!(new_align > self.align);
        debug_assert!(new_align.is_power_of_two());

        if self.cap_bytes > 0 {
            // Deallocate the old block — no need to copy because there are
            // no live values (see safety contract above).
            let old_layout =
                Layout::from_size_align(self.cap_bytes, self.align).expect("invalid old layout");
            // SAFETY: `ptr` was allocated with `old_layout` (struct invariant),
            // and `cap_bytes > 0` so the allocation is live.
            unsafe { alloc::dealloc(self.ptr.as_ptr(), old_layout) };

            // Allocate a new block with the same size but stricter alignment.
            let new_layout =
                Layout::from_size_align(self.cap_bytes, new_align).expect("invalid new layout");
            // SAFETY: `new_layout` has non-zero size (`cap_bytes > 0`).
            let new_ptr = unsafe { alloc::alloc(new_layout) };
            if new_ptr.is_null() {
                alloc::handle_alloc_error(new_layout);
            }
            // SAFETY: `alloc` returned non-null (checked above).
            self.ptr = unsafe { NonNull::new_unchecked(new_ptr) };
        }
        // If cap_bytes == 0, no allocation exists — just update the recorded
        // alignment so the first allocation uses the correct value.
        self.align = new_align;
    }

    /// Grow the backing buffer to hold at least `needed_bytes`.
    ///
    /// Uses amortized doubling (capacity is always a power of two).
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `live_bytes <= self.cap_bytes` — it is the number of initialized
    ///   bytes at the start of the buffer that must be preserved across
    ///   reallocation.
    /// - `align <= self.align` when `self.cap_bytes > 0` — the existing
    ///   allocation's alignment is already sufficient. (For the first
    ///   allocation, `align` is used directly.)
    unsafe fn grow(&mut self, needed_bytes: usize, align: usize, live_bytes: usize) {
        debug_assert!(align <= self.align || self.cap_bytes == 0);
        debug_assert!(live_bytes <= self.cap_bytes);

        // Amortized doubling, but at least `needed_bytes`.
        let new_cap = needed_bytes
            .checked_next_power_of_two()
            .expect("capacity overflow");
        let new_cap = new_cap.max(self.cap_bytes.saturating_mul(2));

        // Rust requires allocations to be at most isize::MAX bytes.
        assert!(new_cap <= isize::MAX as usize, "allocation too large");

        if self.cap_bytes == 0 {
            // First allocation — use the caller-provided alignment.
            let layout = Layout::from_size_align(new_cap, align).expect("invalid layout");
            // SAFETY: layout has non-zero size (needed_bytes > 0, so new_cap > 0).
            let ptr = unsafe { alloc::alloc(layout) };
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            // SAFETY: alloc returned non-null.
            self.ptr = unsafe { NonNull::new_unchecked(ptr) };
            self.cap_bytes = new_cap;
            self.align = align;
        } else {
            // Existing allocation — alignment is already sufficient (ensured
            // at view creation time via `upgrade_alignment`), so `realloc`
            // will preserve it.
            let old_layout =
                Layout::from_size_align(self.cap_bytes, self.align).expect("invalid old layout");
            // SAFETY: `ptr` was allocated with `old_layout` (struct invariant),
            // `new_cap >= needed_bytes > self.cap_bytes` so it's a valid new
            // size, and `live_bytes <= self.cap_bytes` bytes are preserved.
            let new_ptr = unsafe { alloc::realloc(self.ptr.as_ptr(), old_layout, new_cap) };
            if new_ptr.is_null() {
                alloc::handle_alloc_error(Layout::from_size_align(new_cap, self.align).unwrap());
            }
            // SAFETY: realloc returned non-null.
            self.ptr = unsafe { NonNull::new_unchecked(new_ptr) };
            self.cap_bytes = new_cap;
        }
    }
}

impl Default for UntypedVec {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for UntypedVec {
    fn drop(&mut self) {
        if self.cap_bytes > 0 {
            let layout = Layout::from_size_align(self.cap_bytes, self.align)
                .expect("invalid layout in drop");
            // SAFETY: `ptr` was allocated with `layout` (struct invariant),
            // and `cap_bytes > 0` guarantees the allocation is live.
            // No live values exist in the buffer (struct invariant: values
            // are only live while a `TypedView` exists, and `TypedView`
            // holds `&mut self`, preventing `drop` from running while a
            // view is active).
            unsafe { alloc::dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

// ---------------------------------------------------------------------------
// TypedView
// ---------------------------------------------------------------------------

impl<'a, T> TypedView<'a, T> {
    /// Number of live `T` elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the view is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Capacity in number of `T` elements.
    pub fn capacity(&self) -> usize {
        if std::mem::size_of::<T>() == 0 {
            usize::MAX
        } else {
            self.buf.cap_bytes / std::mem::size_of::<T>()
        }
    }

    /// Push a value onto the end.
    ///
    /// # Panics
    ///
    /// Panics on capacity overflow (extremely large allocations).
    pub fn push(&mut self, val: T) {
        let size = std::mem::size_of::<T>();

        if size == 0 {
            // ZST: no memory needed, just count.
            self.len = self.len.checked_add(1).expect("ZST length overflow");
            std::mem::forget(val); // nothing to store, but don't run drop
            return;
        }

        let needed = self
            .len
            .checked_add(1)
            .and_then(|n| n.checked_mul(size))
            .expect("capacity overflow");

        if needed > self.buf.cap_bytes {
            // SAFETY: `live_bytes = self.len * size` is the number of
            // initialized bytes. This is ≤ `cap_bytes` because we could
            // fit `self.len` elements before this call. `align_of::<T>()
            // <= self.buf.align` because alignment was upgraded in
            // `typed_view()`.
            unsafe {
                self.buf
                    .grow(needed, std::mem::align_of::<T>(), self.len * size);
            }
        }

        // SAFETY:
        // - Room: `self.len * size + size <= self.buf.cap_bytes` (ensured
        //   by the grow above or by the capacity check).
        // - Alignment: `self.buf.ptr` is aligned to `align_of::<T>()`
        //   (ensured at view creation). The offset `self.len * size` is a
        //   multiple of `align_of::<T>()` because `size_of::<T>()` is
        //   always a multiple of `align_of::<T>()`.
        // - No aliasing: we have `&mut self`, so no other references to
        //   the buffer exist.
        unsafe {
            let dst = self.buf.ptr.as_ptr().add(self.len * size) as *mut T;
            ptr::write(dst, val);
        }
        self.len += 1;
    }

    /// Remove and return the last element, or `None` if empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;

        if std::mem::size_of::<T>() == 0 {
            // SAFETY: For ZSTs, `ptr::read` reads zero bytes. The pointer
            // must be non-null and aligned, which `NonNull::dangling()`
            // guarantees for any type. This is the same approach `Vec` uses.
            unsafe {
                return Some(ptr::read(NonNull::dangling().as_ptr() as *const T));
            }
        }

        // SAFETY: `self.len` was just decremented, so `self.len * size`
        // points to the start of the last live element. That element is
        // initialized (it was previously within bounds). `ptr::read` moves
        // the value out, and we no longer consider it live (len was
        // decremented), so it won't be double-dropped.
        unsafe {
            let src = self
                .buf
                .ptr
                .as_ptr()
                .add(self.len * std::mem::size_of::<T>()) as *const T;
            Some(ptr::read(src))
        }
    }

    /// Clear all elements, dropping each one. Does not release memory.
    pub fn clear(&mut self) {
        if std::mem::needs_drop::<T>() {
            while self.pop().is_some() {}
        } else {
            self.len = 0;
        }
    }

    /// Reserve capacity for at least `additional` more elements beyond the
    /// current length. Does nothing for ZSTs.
    ///
    /// # Panics
    ///
    /// Panics on capacity overflow.
    pub fn reserve(&mut self, additional: usize) {
        let size = std::mem::size_of::<T>();
        if size == 0 {
            return;
        }

        let needed = self
            .len
            .checked_add(additional)
            .and_then(|n| n.checked_mul(size))
            .expect("capacity overflow");

        if needed > self.buf.cap_bytes {
            // SAFETY: same contract as in `push` — `live_bytes = self.len *
            // size <= self.buf.cap_bytes`, and alignment is already correct.
            unsafe {
                self.buf
                    .grow(needed, std::mem::align_of::<T>(), self.len * size);
            }
        }
    }

    /// Get the contents as a slice.
    pub fn as_slice(&self) -> &[T] {
        if self.len == 0 {
            return &[];
        }
        if std::mem::size_of::<T>() == 0 {
            // SAFETY: For ZSTs, `from_raw_parts` needs a non-null, aligned
            // pointer. `NonNull::dangling()` provides this. No bytes are
            // actually read, `len` just tracks the count.
            unsafe {
                std::slice::from_raw_parts(NonNull::dangling().as_ptr() as *const T, self.len)
            }
        } else {
            // SAFETY: `self.buf.ptr` is aligned for `T` (ensured at view
            // creation), and `self.len` elements are initialized contiguously
            // starting at `ptr`. The returned lifetime is tied to `&self`,
            // which borrows the view (and transitively the buffer) immutably.
            unsafe { std::slice::from_raw_parts(self.buf.ptr.as_ptr() as *const T, self.len) }
        }
    }

    /// Get the contents as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        if self.len == 0 {
            return &mut [];
        }
        if std::mem::size_of::<T>() == 0 {
            // SAFETY: Same as `as_slice` — dangling aligned pointer, ZST.
            unsafe {
                std::slice::from_raw_parts_mut(NonNull::dangling().as_ptr() as *mut T, self.len)
            }
        } else {
            // SAFETY: Same as `as_slice`, plus we have `&mut self` so no
            // aliasing references exist.
            unsafe { std::slice::from_raw_parts_mut(self.buf.ptr.as_ptr() as *mut T, self.len) }
        }
    }
}

impl<'a, T> Deref for TypedView<'a, T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<'a, T> DerefMut for TypedView<'a, T> {
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

impl<'a, T> Drop for TypedView<'a, T> {
    fn drop(&mut self) {
        // Drop all live elements, restoring the "no live values" invariant
        // on the underlying `UntypedVec`.
        if std::mem::needs_drop::<T>() && self.len > 0 {
            let size = std::mem::size_of::<T>();
            if size > 0 {
                // SAFETY: `self.len` elements are initialized starting at
                // `self.buf.ptr`, each at offset `i * size`. The pointer is
                // aligned for `T` (ensured at view creation). After
                // `drop_in_place`, the element is no longer live. We drop
                // all elements, restoring the buffer to an untyped state.
                unsafe {
                    for i in 0..self.len {
                        let elem = self.buf.ptr.as_ptr().add(i * size) as *mut T;
                        ptr::drop_in_place(elem);
                    }
                }
            }
            // ZSTs that impl Drop: `drop_in_place` on a ZST is a no-op
            // (zero bytes, nothing to do), and we have no pointer to call
            // it on anyway. The drop glue runs nothing.
        }
        // `len` is not stored on the `UntypedVec`, so nothing to reset.
        // The `TypedView` is consumed and the buffer is "untyped" again.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -----------------------------------------------------------------------
    // Drop-counting helper
    // -----------------------------------------------------------------------

    /// Per-test drop counter. Use `DropCounter::new()` to get a counter,
    /// then `counter.make(value)` to create tracked instances.
    struct DropCounter(std::sync::Arc<AtomicUsize>);

    impl DropCounter {
        fn new() -> Self {
            Self(std::sync::Arc::new(AtomicUsize::new(0)))
        }

        fn make(&self, val: u32) -> Tracked {
            Tracked(val, self.0.clone())
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    struct Tracked(u32, std::sync::Arc<AtomicUsize>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }

    // -----------------------------------------------------------------------
    // Basic operations
    // -----------------------------------------------------------------------

    #[test]
    fn push_single_element() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(42);
        assert_eq!(&*view, &[42]);
    }

    #[test]
    fn push_many_forces_multiple_grows() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        let n = 1000;
        for i in 0..n {
            view.push(i);
        }
        let expected: Vec<u32> = (0..n).collect();
        assert_eq!(&*view, &expected[..]);
    }

    #[test]
    fn pop_lifo_order() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(10);
        view.push(20);
        view.push(30);
        assert_eq!(view.pop(), Some(30));
        assert_eq!(view.pop(), Some(20));
        assert_eq!(view.pop(), Some(10));
    }

    #[test]
    fn pop_empty_returns_none() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        assert_eq!(view.pop(), None);
    }

    #[test]
    fn clear_resets_len() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(1);
        view.push(2);
        view.push(3);
        view.clear();
        assert_eq!(view.len(), 0);
        assert!(view.is_empty());
        assert_eq!(&*view, &[] as &[u32]);
    }

    #[test]
    fn clear_drops_elements() {
        let dc = DropCounter::new();
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Tracked>();
            view.push(dc.make(1));
            view.push(dc.make(2));
            view.push(dc.make(3));
            assert_eq!(dc.count(), 0);
            view.clear();
            assert_eq!(dc.count(), 3);
        }
        // No additional drops after view is dropped (elements already cleared).
        assert_eq!(dc.count(), 3);
    }

    #[test]
    fn len_and_is_empty() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u64>();
        assert!(view.is_empty());
        assert_eq!(view.len(), 0);

        view.push(1);
        assert!(!view.is_empty());
        assert_eq!(view.len(), 1);

        view.push(2);
        assert_eq!(view.len(), 2);

        view.pop();
        assert_eq!(view.len(), 1);

        view.pop();
        assert!(view.is_empty());
    }

    #[test]
    fn reserve_does_not_change_len() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(1);
        view.reserve(100);
        assert_eq!(view.len(), 1);
        assert!(view.capacity() >= 101);
        assert_eq!(&*view, &[1]);
    }

    #[test]
    fn capacity_reports_element_count() {
        let mut buf = UntypedVec::with_capacity(64, 4);
        let view = buf.typed_view::<u32>();
        // 64 bytes / 4 bytes per u32 = 16 elements
        assert_eq!(view.capacity(), 64 / std::mem::size_of::<u32>());
    }

    // -----------------------------------------------------------------------
    // Type reuse
    // -----------------------------------------------------------------------

    #[test]
    fn reuse_u32_then_u64() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u32>();
            for i in 0..20 {
                view.push(i);
            }
        }
        let cap_after_u32 = buf.capacity_bytes();
        assert!(cap_after_u32 > 0);

        {
            let mut view = buf.typed_view::<u64>();
            view.push(999);
            assert_eq!(&*view, &[999u64]);
        }
        // Capacity should be >= what it was (alignment upgrade may reallocate
        // but shouldn't shrink).
        assert!(buf.capacity_bytes() >= cap_after_u32);
    }

    #[test]
    fn reuse_u8_then_u64_alignment_upgrade() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u8>();
            for i in 0..100 {
                view.push(i);
            }
        }
        assert!(buf.alignment() >= 1);
        let old_cap = buf.capacity_bytes();

        {
            let mut view = buf.typed_view::<u64>();
            view.push(0xDEAD_BEEF);
            assert_eq!(view[0], 0xDEAD_BEEF);
        }
        assert!(buf.alignment() >= std::mem::align_of::<u64>());
        // Capacity preserved or grown.
        assert!(buf.capacity_bytes() >= old_cap);
    }

    #[test]
    fn reuse_u64_then_u8_no_downgrade() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u64>();
            view.push(1);
            view.push(2);
        }
        let align_after_u64 = buf.alignment();
        assert!(align_after_u64 >= std::mem::align_of::<u64>());

        {
            let mut view = buf.typed_view::<u8>();
            view.push(0xFF);
            assert_eq!(&*view, &[0xFF]);
        }
        // Alignment should NOT have been downgraded.
        assert_eq!(buf.alignment(), align_after_u64);
    }

    #[test]
    fn sequential_same_type_reuses_allocation() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u32>();
            for i in 0..50 {
                view.push(i);
            }
        }
        let ptr1 = buf.ptr.as_ptr();
        let cap1 = buf.capacity_bytes();

        {
            let mut view = buf.typed_view::<u32>();
            view.push(999);
            assert_eq!(&*view, &[999]);
        }
        // Same allocation reused (no grow needed).
        assert_eq!(buf.ptr.as_ptr(), ptr1);
        assert_eq!(buf.capacity_bytes(), cap1);
    }

    // -----------------------------------------------------------------------
    // Alignment
    // -----------------------------------------------------------------------

    #[test]
    fn alignment_u8() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u8>();
        view.push(1);
        view.push(2);
        assert_eq!(&*view, &[1u8, 2]);
    }

    #[test]
    fn alignment_u64() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u64>();
            view.push(0x1234_5678_9ABC_DEF0);
            assert_eq!(view[0], 0x1234_5678_9ABC_DEF0);
        }
        assert_eq!(
            buf.ptr.as_ptr() as usize % std::mem::align_of::<u64>(),
            0
        );
    }

    #[test]
    fn alignment_u128() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u128>();
            view.push(42);
            assert_eq!(view[0], 42u128);
        }
        assert_eq!(
            buf.ptr.as_ptr() as usize % std::mem::align_of::<u128>(),
            0
        );
    }

    #[test]
    fn alignment_custom_align32() {
        #[repr(align(32))]
        #[derive(Debug, PartialEq)]
        struct Align32(u64);

        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Align32>();
            view.push(Align32(1));
            view.push(Align32(2));
            assert_eq!(view[0], Align32(1));
            assert_eq!(view[1], Align32(2));
        }
        assert_eq!(buf.ptr.as_ptr() as usize % 32, 0);
    }

    #[test]
    fn alignment_correct_after_grow() {
        #[repr(align(16))]
        #[derive(Debug, PartialEq, Clone)]
        struct Big([u8; 64]);

        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Big>();
            // Push enough to trigger multiple grows.
            for i in 0..100u8 {
                view.push(Big([i; 64]));
            }
            // Verify data integrity.
            for i in 0..100u8 {
                assert_eq!(view[i as usize], Big([i; 64]));
            }
        }
        // Verify alignment still holds after grows.
        assert_eq!(buf.ptr.as_ptr() as usize % 16, 0);
    }

    // -----------------------------------------------------------------------
    // Drop correctness
    // -----------------------------------------------------------------------

    #[test]
    fn drop_on_view_drop() {
        let dc = DropCounter::new();
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Tracked>();
            view.push(dc.make(1));
            view.push(dc.make(2));
            view.push(dc.make(3));
            assert_eq!(dc.count(), 0);
        }
        assert_eq!(dc.count(), 3);
    }

    #[test]
    fn pop_no_double_drop() {
        let dc = DropCounter::new();
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Tracked>();
            view.push(dc.make(1));
            view.push(dc.make(2));
            let popped = view.pop().unwrap();
            assert_eq!(dc.count(), 0); // Not dropped yet — we own it.
            drop(popped);
            assert_eq!(dc.count(), 1); // Now it's dropped.
        }
        // View drop should only drop the 1 remaining element.
        assert_eq!(dc.count(), 2);
    }

    #[test]
    fn partial_pop_then_drop_view() {
        let dc = DropCounter::new();
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<Tracked>();
            for i in 0..5 {
                view.push(dc.make(i));
            }
            // Pop 2, leaving 3.
            let _ = view.pop();
            let _ = view.pop();
            assert_eq!(dc.count(), 2);
        }
        // Remaining 3 should be dropped.
        assert_eq!(dc.count(), 5);
    }

    // -----------------------------------------------------------------------
    // ZSTs
    // -----------------------------------------------------------------------

    #[test]
    fn zst_push_pop() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<()>();
        view.push(());
        view.push(());
        view.push(());
        assert_eq!(view.len(), 3);
        assert_eq!(view.pop(), Some(()));
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn zst_no_allocation() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<()>();
            for _ in 0..1000 {
                view.push(());
            }
            assert_eq!(view.len(), 1000);
        }
        assert_eq!(buf.capacity_bytes(), 0);
    }

    #[test]
    fn zst_capacity_is_usize_max() {
        let mut buf = UntypedVec::new();
        let view = buf.typed_view::<()>();
        assert_eq!(view.capacity(), usize::MAX);
    }

    #[test]
    fn zst_slice_access() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<()>();
        view.push(());
        view.push(());
        let s: &[()] = &*view;
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn zst_then_non_zst() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<()>();
            view.push(());
            view.push(());
        }
        {
            let mut view = buf.typed_view::<u32>();
            view.push(42);
            assert_eq!(&*view, &[42]);
        }
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn with_capacity_zero_does_not_allocate() {
        let buf = UntypedVec::with_capacity(0, 8);
        assert_eq!(buf.capacity_bytes(), 0);
    }

    #[test]
    fn push_after_pop_reuses_slot() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(1);
        view.push(2);
        let cap = view.capacity();
        view.pop();
        view.push(3);
        // Should not have grown.
        assert_eq!(view.capacity(), cap);
        assert_eq!(&*view, &[1, 3]);
    }

    #[test]
    fn stress_100k_u8s() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u8>();
        for i in 0..100_000u32 {
            view.push((i & 0xFF) as u8);
        }
        assert_eq!(view.len(), 100_000);
        for i in 0..100_000u32 {
            assert_eq!(view[i as usize], (i & 0xFF) as u8);
        }
    }

    #[test]
    fn with_capacity_avoids_realloc() {
        // Pre-allocate enough for 100 u64s.
        let mut buf = UntypedVec::with_capacity(800, std::mem::align_of::<u64>());
        let ptr_before = buf.ptr.as_ptr();
        {
            let mut view = buf.typed_view::<u64>();
            for i in 0..100u64 {
                view.push(i);
            }
        }
        // No realloc should have occurred.
        assert_eq!(buf.ptr.as_ptr(), ptr_before);
    }

    // -----------------------------------------------------------------------
    // Deref / slice operations
    // -----------------------------------------------------------------------

    #[test]
    fn indexing() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(10);
        view.push(20);
        view.push(30);
        assert_eq!(view[0], 10);
        assert_eq!(view[1], 20);
        assert_eq!(view[2], 30);
    }

    #[test]
    fn iteration() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(1);
        view.push(2);
        view.push(3);
        let sum: u32 = view.iter().sum();
        assert_eq!(sum, 6);
    }

    #[test]
    fn mutable_slice() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(1);
        view.push(2);
        view[0] = 99;
        assert_eq!(&*view, &[99, 2]);
    }

    #[test]
    fn sort_via_deref_mut() {
        let mut buf = UntypedVec::new();
        let mut view = buf.typed_view::<u32>();
        view.push(3);
        view.push(1);
        view.push(2);
        view.sort();
        assert_eq!(&*view, &[1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // Thread safety (static assertions)
    // -----------------------------------------------------------------------

    #[test]
    fn send_sync_assertions() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<UntypedVec>();
        assert_sync::<UntypedVec>();
        assert_send::<TypedView<'_, u32>>();
    }

    // -----------------------------------------------------------------------
    // View starts empty each time
    // -----------------------------------------------------------------------

    #[test]
    fn view_starts_empty_after_previous_view_had_data() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u32>();
            view.push(1);
            view.push(2);
            view.push(3);
        }
        {
            let view = buf.typed_view::<u32>();
            // A new view must start empty — the previous data is gone.
            assert!(view.is_empty());
            assert_eq!(view.len(), 0);
            assert_eq!(&*view, &[] as &[u32]);
        }
    }

    // -----------------------------------------------------------------------
    // Mixed type integrity: verify data written as one type isn't
    // accidentally "visible" as another type.
    // -----------------------------------------------------------------------

    #[test]
    fn different_type_view_starts_empty() {
        let mut buf = UntypedVec::new();
        {
            let mut view = buf.typed_view::<u64>();
            view.push(0xFFFF_FFFF_FFFF_FFFF);
        }
        {
            // Even though the bytes are still there, the view must start at len=0.
            let view = buf.typed_view::<u32>();
            assert!(view.is_empty());
        }
    }
}
