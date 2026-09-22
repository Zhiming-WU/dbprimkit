//! A simple spin lock implementation, based on the code from the book "Rust Atomics and Locks"
//! by Mara Bos.
//!
//! Below is an example.
//! ```rust
//! use std::sync::Arc;
//! use std::thread;
//! use dbprimkit::sync::spinlock::SpinLock;
//!
//! fn main() {
//!    const THREADS: usize = 10;
//!    const ITERATIONS: usize = 10_000;
//!    let lock = Arc::new(SpinLock::new(0));
//!    std::thread::scope(|s| {
//!        for _ in 0..THREADS {
//!            let lock_clone = lock.clone();
//!            s.spawn(move || {
//!                for _ in 0..ITERATIONS {
//!                    let mut guard = lock_clone.lock();
//!                    *guard += 1;
//!                }
//!            });
//!        }
//!    });
//!    let final_value = *lock.lock();
//!    assert_eq!(final_value, THREADS * ITERATIONS);
//! }
//! ```
//!

use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

const FALSE: usize = 0;
const TRUE: usize = 1;

/// A simple spin lock implementation.
pub struct SpinLock<T> {
    locked: AtomicUsize,
    value: UnsafeCell<T>,
}

unsafe impl<T> Sync for SpinLock<T> where T: Send {}

impl<T> SpinLock<T> {
    /// Make a new unlocked spin lock, with specified initial value.
    pub const fn new(value: T) -> Self {
        Self {
            locked: AtomicUsize::new(FALSE),
            value: UnsafeCell::new(value),
        }
    }

    /// Acquires the lock, blocking the current thread until it is available.
    pub fn lock(&self) -> Guard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(FALSE, TRUE, Acquire, Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Guard { lock: self }
    }

    /// Acquires the lock, with specified retry count.
    pub fn try_lock(&self, retry_cnt: usize) -> Option<Guard<'_, T>> {
        let mut retry = 0usize;
        while retry <= retry_cnt {
            if self
                .locked
                .compare_exchange_weak(FALSE, TRUE, Acquire, Relaxed)
                .is_ok()
            {
                return Some(Guard { lock: self });
            }
            std::hint::spin_loop();
            retry += 1;
        }
        None
    }
}

/// The RAII guard for the acquired lock.
pub struct Guard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(FALSE, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_lock_and_unlock() {
        let lock = SpinLock::new(42);

        {
            let mut guard = lock.lock();
            assert_eq!(*guard, 42);
            *guard = 100;
        }

        let guard = lock.lock();
        assert_eq!(*guard, 100);
    }

    #[test]
    fn test_try_lock_success_and_failure() {
        let lock = SpinLock::new(10);

        let guard1 = lock.try_lock(5);
        assert!(guard1.is_some());

        let guard2 = lock.try_lock(10);
        assert!(guard2.is_none());

        drop(guard1);

        let guard3 = lock.try_lock(0);
        assert!(guard3.is_some());
    }

    #[test]
    fn test_multithreaded_concurrency() {
        const THREADS: usize = 10;
        const ITERATIONS: usize = 10_000;

        let lock = Arc::new(SpinLock::new(0));
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let lock_clone = Arc::clone(&lock);
            let handle = thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    let mut guard = lock_clone.lock();
                    *guard += 1;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_value = *lock.lock();
        assert_eq!(final_value, THREADS * ITERATIONS);
    }

    #[test]
    fn test_complex_data_structure() {
        let lock = SpinLock::new(vec![1, 2, 3]);

        {
            let mut guard = lock.lock();
            guard.push(4);
        }

        let guard = lock.lock();
        assert_eq!(*guard, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_spinlock_sample() {
        const THREADS: usize = 10;
        const ITERATIONS: usize = 10_000;
        let lock = Arc::new(SpinLock::new(0));
        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let lock_clone = lock.clone();
                s.spawn(move || {
                    for _ in 0..ITERATIONS {
                        let mut guard = lock_clone.lock();
                        *guard += 1;
                    }
                });
            }
        });
        let final_value = *lock.lock();
        assert_eq!(final_value, THREADS * ITERATIONS);
    }
}
