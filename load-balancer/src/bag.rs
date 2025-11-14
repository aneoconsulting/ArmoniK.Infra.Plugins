use std::sync::RwLock;

use crossbeam_deque::{Stealer, Worker};
use thread_local::ThreadLocal;

#[derive(Debug)]
pub struct Bag<T: Send> {
    worker: ThreadLocal<Worker<T>>,
    stealers: RwLock<Vec<Stealer<T>>>,
}

impl<T: Send> Default for Bag<T> {
    fn default() -> Self {
        Self {
            worker: Default::default(),
            stealers: Default::default(),
        }
    }
}

impl<T: Send> Bag<T> {
    pub fn new() -> Self {
        Self::default()
    }

    fn worker(&self) -> &Worker<T> {
        self.worker.get_or(|| {
            let worker = Worker::new_lifo();
            self.stealers.write().unwrap().push(worker.stealer());
            worker
        })
    }

    pub fn pop(&self) -> Option<T> {
        let worker = self.worker();
        if let Some(item) = worker.pop() {
            Some(item)
        } else {
            let mut retry = true;
            while retry {
                retry = false;
                for stealer in self.stealers.read().unwrap().iter() {
                    match stealer.steal_batch_and_pop(worker) {
                        crossbeam_deque::Steal::Empty => (),
                        crossbeam_deque::Steal::Success(item) => return Some(item),
                        crossbeam_deque::Steal::Retry => retry = true,
                    }
                }
            }
            None
        }
    }

    pub fn push(&self, item: T) {
        self.worker().push(item);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, thread};

    use super::*;

    #[test]
    fn bag_auto_traits() {
        struct NotSync(#[allow(unused)] *mut i32);
        unsafe impl Send for NotSync {}

        let bag = Bag::<NotSync>::new();

        _ = &bag as &dyn Send;
        _ = &bag as &dyn Sync;
    }

    #[test]
    #[cfg_attr(miri, ignore)] // crossbeam-epoch is currently incompatible with MIRI: https://github.com/crossbeam-rs/crossbeam/issues/1181
    fn foo() {
        let bag = Bag::new();
        bag.push(1);
        bag.push(2);
        bag.push(3);

        let values = [
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
        ];

        thread::scope(|s| {
            s.spawn(|| {
                _ = values[1].lock().unwrap().insert(bag.pop());
            });
            s.spawn(|| {
                _ = values[2].lock().unwrap().insert(bag.pop());
            });
            s.spawn(|| {
                _ = values[3].lock().unwrap().insert(bag.pop());
            });
            thread::yield_now();
            _ = values[0].lock().unwrap().insert(bag.pop());
        });

        let mut values = values.map(|x| x.into_inner().unwrap().unwrap());
        values.sort();

        assert_eq!(values, [None, Some(1), Some(2), Some(3)]);
    }
}
