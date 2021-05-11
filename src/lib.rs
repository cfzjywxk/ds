use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct LocalError(String);

struct Node<T> {
    value: UnsafeCell<T>,
    seq: usize,
}

pub struct FixedQueue<T> {
    // The capacity of the queue.
    cap: usize,

    // The buffer holding the Node values.
    buffer: *mut Node<T>,

    // The index point for the pop.
    producer: AtomicUsize,

    // The index point for the push.
    consumer: AtomicUsize,

    // The marker indicating the drop.
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Sync for FixedQueue<T> {}
unsafe impl<T: Send> Send for FixedQueue<T> {}

impl<T> FixedQueue<T> {
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "capacity must be non-zero");

        let buffer = {
            let mut v = Vec::<Node<T>>::with_capacity(cap);
            let ptr = v.as_mut_ptr();
            std::mem::forget(v);
            ptr
        };
        for i in 0..cap {
            unsafe {
                (*buffer.add(i)).seq = std::usize::MAX;
            }
        }

        FixedQueue {
            cap,
            buffer,
            producer: AtomicUsize::new(0),
            consumer: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    pub fn push(&self, data: T) -> Result<(), LocalError> {
        let mut old_pos = self.producer.load(Relaxed);
        let mut cmp_pos = old_pos;
        let mut new_pos = old_pos + 1;
        loop {
            let index = old_pos % self.cap;
            unsafe {
                if old_pos == self.producer.load(Relaxed)
                    && (*self.buffer.add(index)).seq != std::usize::MAX
                {
                    return Err(LocalError("The queue is full".to_string()));
                }
            }
            match self
                .producer
                .compare_exchange_weak(cmp_pos, new_pos, SeqCst, Relaxed)
            {
                Ok(_) => {
                    // Write the value.
                    unsafe {
                        (*self.buffer.add(index)).value.get().write(data);
                        (*self.buffer.add(index)).seq = old_pos;
                    }
                    return Ok(());
                }
                Err(x) => {
                    old_pos = x;
                    cmp_pos = old_pos;
                    new_pos = old_pos + 1;
                }
            }
        }
    }

    pub fn pop(&self) -> Result<T, LocalError> {
        let mut old_pos = self.consumer.load(Relaxed);
        let mut cmp_pos = old_pos;
        let mut new_pos = old_pos + 1;
        loop {
            let index = old_pos % self.cap;
            unsafe {
                if old_pos == self.consumer.load(Relaxed)
                    && (*self.buffer.add(index)).seq != old_pos
                {
                    return Err(LocalError("The queue is empty".to_string()));
                }
            }
            match self
                .consumer
                .compare_exchange_weak(cmp_pos, new_pos, SeqCst, Relaxed)
            {
                Ok(_) => unsafe {
                    let res = (*self.buffer.add(index)).value.get().read();
                    (*self.buffer.add(index)).seq = std::usize::MAX;
                    return Ok(res);
                },
                Err(x) => {
                    old_pos = x;
                    cmp_pos = old_pos;
                    new_pos = old_pos + 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_push_pop() {
        let queue = FixedQueue::<i32>::new(3);
        assert!(queue.push(1).is_ok());
        assert!(queue.push(2).is_ok());
        assert!(queue.push(3).is_ok());
        assert_eq!(
            queue.push(4).err().unwrap(),
            LocalError("The queue is full".to_string())
        );
        assert_eq!(queue.pop().unwrap(), 1);
        assert_eq!(queue.pop().unwrap(), 2);
        assert_eq!(queue.pop().unwrap(), 3);
        assert_eq!(
            queue.pop().err().unwrap(),
            LocalError("The queue is empty".to_string())
        );

        assert!(queue.push(1).is_ok());
        assert!(queue.push(2).is_ok());
        assert!(queue.push(3).is_ok());
        assert_eq!(
            queue.push(4).err().unwrap(),
            LocalError("The queue is full".to_string())
        );
        assert_eq!(queue.pop().unwrap(), 1);
        assert_eq!(queue.pop().unwrap(), 2);
        assert_eq!(queue.pop().unwrap(), 3);
        assert_eq!(
            queue.pop().err().unwrap(),
            LocalError("The queue is empty".to_string())
        )
    }

    #[test]
    fn test_basic_concurrent() {
        let nthreads = 10;
        let queue = Arc::new(FixedQueue::<usize>::new(1000));

        let push_sum = Arc::new(AtomicUsize::new(0));
        let mut children = vec![];
        for i in 0..nthreads {
            let sum_val = push_sum.clone();
            let push_queue = queue.clone();
            children.push(thread::spawn(move || {
                for j in (i * 100)..((i + 1) * 100) {
                    loop {
                        if let Ok(_) = push_queue.push(j) {
                            sum_val.fetch_add(j as usize, Relaxed);
                            break;
                        }
                    }
                }
            }))
        }

        for child in children {
            let _ = child.join();
        }

        let start = Arc::new(AtomicBool::new(false));
        children = vec![];
        let actual_sum = Arc::new(AtomicUsize::new(0));
        for i in 0..nthreads {
            let actual_sum_val = actual_sum.clone();
            let pop_queue = queue.clone();
            let let_start = start.clone();
            children.push(thread::spawn(move || {
                loop {
                    if let_start.load(Relaxed) {
                        break;
                    }
                }
                for _ in (i * 100)..((i + 1) * 100) {
                    loop {
                        if let Ok(res) = pop_queue.pop() {
                            println!("the pop out value={:?} from thread={:?}", res, i);
                            actual_sum_val.fetch_add(res as usize, Relaxed);
                            break;
                        }
                    }
                }
            }))
        }

        start.store(true, Relaxed);
        for child in children {
            let _ = child.join();
        }
        println!(
            "the expected push_sum={:?}, the actual_sum={:?}",
            push_sum.load(Relaxed),
            actual_sum
        );
        assert_eq!(push_sum.load(Relaxed), actual_sum.load(Relaxed));
    }
}
