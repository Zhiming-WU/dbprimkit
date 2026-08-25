//! A simple fixed-capacity ring buffer implementation with MPMC (or MPSC/SPMC) support,
//! based on Dmitry Vyukov's algorithm at
//! <https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/bounded-mpmc-queue>.
//!
//! Note: You should use only [RingBuffer::push] (in multiple producers case) or only [RingBuffer::push_for_sp]
//! (in single producer case), without in a mixed mode. Same for [RingBuffer::pop] (used in multiple consumers case)
//! and [RingBuffer::pop_for_sc] (used in single consumers case).
//!
use crate::{CacheAligned, Error, Result};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Cell<T> {
    seq: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

pub struct RingBuffer<T> {
    buf: Box<[Cell<T>]>,
    mask: usize,
    head: CacheAligned<AtomicUsize>,
    tail: CacheAligned<AtomicUsize>,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Create a new ring buffer. The capacity is 2^k, where k is the value of the parameter `size_order`.
    pub fn new(size_order: u8) -> Result<Self> {
        let size = 1usize << size_order;
        let max_isize = isize::MAX as usize;
        if size > max_isize {
            return Err(Error::InvalidArgument("`size_order` is too large".into()));
        }

        let mut buf = Vec::with_capacity(size);
        for i in 0..size {
            buf.push(Cell {
                seq: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Ok(Self {
            buf: buf.into_boxed_slice(),
            mask: size - 1,
            head: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
            tail: CacheAligned::<AtomicUsize>(AtomicUsize::new(0)),
        })
    }

    /// Push a value into the ring buffer.
    pub fn push(&self, value: T) -> Result<(&mut T, usize)> {
        let mut tail = self.tail.0.load(Ordering::Relaxed);

        loop {
            let cell = &self.buf[tail & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let diff = seq as isize - tail as isize;

            if diff == 0 {
                match self.tail.0.compare_exchange_weak(
                    tail,
                    tail + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let v = unsafe { (*cell.data.get()).write(value) };
                        cell.seq.store(tail + 1, Ordering::Release);
                        return Ok((v, tail & self.mask));
                    }
                    Err(actual) => {
                        tail = actual;
                    }
                }
            } else if diff < 0 {
                return Err(Error::RingBufferFull);
            } else {
                tail = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Pop a value from the ring buffer.
    pub fn pop(&self) -> Option<T> {
        let mut head = self.head.0.load(Ordering::Relaxed);

        loop {
            let cell = &self.buf[head & self.mask];
            let seq = cell.seq.load(Ordering::Acquire);
            let diff = seq as isize - (head + 1) as isize;

            if diff == 0 {
                match self.head.0.compare_exchange_weak(
                    head,
                    head + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let value = unsafe { (*cell.data.get()).assume_init_read() };
                        cell.seq.store(head + self.mask + 1, Ordering::Release);
                        return Some(value);
                    }
                    Err(actual) => {
                        head = actual;
                    }
                }
            } else if diff < 0 {
                return None;
            } else {
                head = self.head.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Push a value into the ring buffer (used in single producer case).
    pub fn push_for_sp(&self, value: T) -> Result<(&mut T, usize)> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let cell = &self.buf[tail & self.mask];
        let seq = cell.seq.load(Ordering::Acquire);

        if seq == tail {
            let v = unsafe { (*cell.data.get()).write(value) };
            cell.seq.store(tail + 1, Ordering::Release);
            self.tail.0.store(tail + 1, Ordering::Relaxed);
            Ok((v, tail & self.mask))
        } else {
            return Err(Error::RingBufferFull);
        }
    }

    /// Pop a value from the ring buffer (used in single consumer case).
    pub fn pop_for_sc(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let cell = &self.buf[head & self.mask];
        let seq = cell.seq.load(Ordering::Acquire);

        if seq == head + 1 {
            let value = unsafe { (*cell.data.get()).assume_init_read() };
            cell.seq.store(head + self.mask + 1, Ordering::Release);
            self.head.0.store(head + 1, Ordering::Relaxed);
            Some(value)
        } else {
            None
        }
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    const ORDER: u8 = 3;
    const CAP: usize = 1 << ORDER;

    #[test]
    fn test_basic_fifo() {
        let rb = RingBuffer::<i32>::new(ORDER).unwrap();

        let (val_ref, idx) = rb.push(10).unwrap();
        assert_eq!(*val_ref, 10);
        assert_eq!(idx, 0);

        rb.push(20).unwrap();

        assert_eq!(rb.pop(), Some(10));
        assert_eq!(rb.pop(), Some(20));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn test_capacity_bound() {
        let rb = RingBuffer::<i32>::new(ORDER).unwrap();

        for i in 0..CAP {
            assert!(rb.push(i as i32).is_ok());
        }

        assert!(rb.push(999).is_err());

        assert_eq!(rb.pop(), Some(0));
        assert!(rb.push(999).is_ok());
    }

    #[test]
    fn test_sp_sc_methods() {
        let rb = RingBuffer::<i32>::new(ORDER).unwrap();

        for i in 0..CAP {
            assert!(rb.push_for_sp(i as i32).is_ok());
        }
        assert!(rb.push_for_sp(999).is_err());

        for i in 0..CAP {
            assert_eq!(rb.pop_for_sc(), Some(i as i32));
        }
        assert_eq!(rb.pop_for_sc(), None);
    }

    #[test]
    fn test_mpmc_concurrent() {
        let rb = Arc::new(RingBuffer::<usize>::new(6).unwrap());
        let num_producers = 4;
        let num_consumers = 4;
        let items_per_producer = 10_000;

        let total_items = num_producers * items_per_producer;
        let sum_atomic = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for p in 0..num_producers {
            let rb_clone = Arc::clone(&rb);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    let val = p * items_per_producer + i + 1;
                    while rb_clone.push(val).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        for _ in 0..num_consumers {
            let rb_clone = Arc::clone(&rb);
            let sum_clone = Arc::clone(&sum_atomic);
            handles.push(thread::spawn(move || {
                let mut local_count = 0;
                while local_count < total_items / num_consumers {
                    if let Some(val) = rb_clone.pop() {
                        sum_clone.fetch_add(val, Ordering::Relaxed);
                        local_count += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let expected_sum = (1..=total_items).sum::<usize>();
        assert_eq!(sum_atomic.load(Ordering::Relaxed), expected_sum);
    }

    #[test]
    fn test_mpsc_hybrid() {
        let rb = Arc::new(RingBuffer::<usize>::new(5).unwrap());
        let num_producers = 4;
        let items_per_producer = 5000;
        let total_items = num_producers * items_per_producer;

        let mut handles = vec![];

        for _ in 0..num_producers {
            let rb_clone = Arc::clone(&rb);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    while rb_clone.push(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        let rb_clone = Arc::clone(&rb);
        let consumer_handle = thread::spawn(move || {
            let mut popped_count = 0;
            while popped_count < total_items {
                if let Some(_) = rb_clone.pop_for_sc() {
                    popped_count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            popped_count
        });

        for h in handles {
            h.join().unwrap();
        }
        let total_popped = consumer_handle.join().unwrap();

        assert_eq!(total_popped, total_items);
    }

    struct DropDetector(Arc<AtomicUsize>);
    impl Drop for DropDetector {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_element_drop_on_clear_and_destroy() {
        let drop_count = Arc::new(AtomicUsize::new(0));

        {
            let rb = RingBuffer::<DropDetector>::new(ORDER).unwrap();

            rb.push(DropDetector(Arc::clone(&drop_count))).unwrap();
            rb.push(DropDetector(Arc::clone(&drop_count))).unwrap();
            rb.push(DropDetector(Arc::clone(&drop_count))).unwrap();

            let popped = rb.pop();
            drop(popped);

            assert_eq!(drop_count.load(Ordering::Relaxed), 1);
        }

        assert_eq!(drop_count.load(Ordering::Relaxed), 3);
    }
}
