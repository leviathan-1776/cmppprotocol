//! 仅供压测使用；累计请求字节不是存活堆大小。重分配按新请求大小计一次。
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
struct Meter;
#[global_allocator]
static ALLOCATOR: Meter = Meter;

fn record(ptr: *mut u8, size: usize) {
    if !ptr.is_null() && ENABLED.load(Relaxed) {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size as u64, Relaxed);
    }
}

// SAFETY: 所有操作原样委托给 System，不改变布局、指针或分配生命周期。
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        record(ptr, layout.size());
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        record(ptr, layout.size());
        ptr
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        record(result, size);
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}
pub fn enable(enabled: bool) {
    ENABLED.store(enabled, Relaxed);
}
pub fn snapshot() -> (u64, u64) {
    (CALLS.load(Relaxed), BYTES.load(Relaxed))
}
