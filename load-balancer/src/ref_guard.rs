use std::{
    future::{Future, IntoFuture},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

pub struct RefGuard<G, T>
where
    G: Unpin,
    T: Unpin,
{
    guard: std::mem::ManuallyDrop<G>,
    reference: std::mem::ManuallyDrop<T>,
}

impl<G: Unpin, T: Unpin> Drop for RefGuard<G, T> {
    fn drop(&mut self) {
        unsafe {
            // Order is crucial for safety
            ManuallyDrop::drop(&mut self.reference);
            ManuallyDrop::drop(&mut self.guard);
        }
    }
}

impl<G: Unpin, T> RefGuard<G, &mut T> {
    pub fn new_deref_mut(mut guard: G) -> Self
    where
        G: DerefMut<Target = T>,
    {
        // As G is Unpin, it is safe to move the guard after dereferencing it
        let mut target = std::ptr::NonNull::from(guard.deref_mut());
        Self {
            guard: ManuallyDrop::new(guard),
            reference: ManuallyDrop::new(unsafe { target.as_mut() }),
        }
    }
}

impl<G: Unpin, T> RefGuard<G, &T> {
    pub fn new_deref(guard: G) -> Self
    where
        G: Deref<Target = T>,
    {
        // As G is Unpin, it is safe to move the guard after dereferencing it
        let target = std::ptr::NonNull::from(guard.deref());
        Self {
            guard: ManuallyDrop::new(guard),
            reference: ManuallyDrop::new(unsafe { target.as_ref() }),
        }
    }
}

impl<G: Unpin, T: Unpin> RefGuard<G, T> {
    unsafe fn into_parts(mut self) -> (G, T) {
        unsafe {
            let ref_guard = (
                ManuallyDrop::take(&mut self.guard),
                ManuallyDrop::take(&mut self.reference),
            );
            std::mem::forget(self);
            ref_guard
        }
    }
    unsafe fn from_parts(guard: G, reference: T) -> Self {
        Self {
            guard: ManuallyDrop::new(guard),
            reference: ManuallyDrop::new(reference),
        }
    }
    pub fn get(&self) -> &T {
        let x = self.reference.deref();
        x
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.reference.deref_mut()
    }

    pub fn map<U: Unpin>(self, f: impl FnOnce(T) -> U) -> RefGuard<G, U> {
        unsafe {
            let (guard, reference) = self.into_parts();
            RefGuard::from_parts(guard, f(reference))
        }
    }

    pub async fn map_async<Func, Fut, Out>(self, f: Func) -> RefGuard<G, Out>
    where
        Func: FnOnce(T) -> Fut,
        Fut: Future<Output = Out>,
        Out: Unpin,
    {
        unsafe {
            let (guard, reference) = self.into_parts();
            RefGuard::from_parts(guard, f(reference).await)
        }
    }

    pub async fn map_await(self) -> RefGuard<G, <T as IntoFuture>::Output>
    where
        T: IntoFuture,
        <T as IntoFuture>::Output: Unpin,
    {
        unsafe {
            let (guard, reference) = self.into_parts();
            RefGuard::from_parts(guard, reference.await)
        }
    }
}

impl<'a, G: Unpin, T: Unpin + Deref> RefGuard<G, &'a T> {
    pub fn map_deref(self) -> RefGuard<G, &'a <T as Deref>::Target> {
        self.map(Deref::deref)
    }
}
impl<'a, G: Unpin, T: Unpin + DerefMut> RefGuard<G, &'a mut T> {
    pub fn map_deref_mut(self) -> RefGuard<G, &'a mut <T as Deref>::Target> {
        self.map(DerefMut::deref_mut)
    }
    pub fn map_deref(self) -> RefGuard<G, &'a <T as Deref>::Target> {
        self.map(|r| r.deref_mut() as &_)
    }
}

impl<G: Unpin, T: Unpin> RefGuard<G, Option<T>> {
    pub fn into_option(self) -> Option<RefGuard<G, T>> {
        unsafe {
            let (guard, reference) = self.into_parts();
            reference.map(|reference| RefGuard::from_parts(guard, reference))
        }
    }
}

impl<G: Unpin, T: Unpin, E: Unpin> RefGuard<G, Result<T, E>> {
    pub fn into_result(self) -> Result<RefGuard<G, T>, E> {
        unsafe {
            let (guard, reference) = self.into_parts();
            reference.map(|reference| RefGuard::from_parts(guard, reference))
        }
    }
}

impl<G: Unpin, T: Unpin> Deref for RefGuard<G, &'_ T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}
impl<G: Unpin, T: Unpin> Deref for RefGuard<G, &'_ mut T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<G: Unpin, T: Unpin> DerefMut for RefGuard<G, &'_ mut T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}
