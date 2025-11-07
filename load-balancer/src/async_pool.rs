use std::{
    future::{Future, IntoFuture},
    hint::unreachable_unchecked,
    marker::PhantomData,
    pin::Pin,
};

use lockfree_object_pool::{LinearObjectPool, LinearReusable};

use crate::ref_guard::RefGuard;

pub enum PoolAwaitable<T> {
    Pending(Pin<Box<dyn Future<Output = T> + Send + Sync>>),
    Ready(T),
}

pub struct PoolFuture<'a, T>(*mut PoolAwaitable<T>, PhantomData<&'a mut PoolAwaitable<T>>);

impl<'a, T> IntoFuture for &'a mut PoolAwaitable<T> {
    type Output = &'a mut T;
    type IntoFuture = PoolFuture<'a, T>;

    fn into_future(self) -> Self::IntoFuture {
        PoolFuture(self, PhantomData)
    }
}

unsafe impl<T> Send for PoolFuture<'_, T> where PoolAwaitable<T>: Sync {}
unsafe impl<T> Sync for PoolFuture<'_, T> where PoolAwaitable<T>: Sync {}

impl<'a, T> Future for PoolFuture<'a, T> {
    type Output = &'a mut T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match unsafe { &mut *this.0 } {
            PoolAwaitable::Pending(future) => match future.as_mut().poll(cx) {
                std::task::Poll::Ready(val) => unsafe {
                    *this.0 = PoolAwaitable::Ready(val);
                    let PoolAwaitable::Ready(val) = &mut *this.0 else {
                        unreachable_unchecked()
                    };
                    std::task::Poll::Ready(val)
                },
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            PoolAwaitable::Ready(val) => std::task::Poll::Ready(val),
        }
    }
}

pub struct AsyncPool<T>(LinearObjectPool<PoolAwaitable<T>>);

impl<T> AsyncPool<T> {
    pub fn new<Func, Fut>(create: Func) -> Self
    where
        Func: Fn() -> Fut,
        Func: Clone + Send + Sync + 'static,
        Fut: Future<Output = T> + Send + Sync + 'static,
    {
        AsyncPool(LinearObjectPool::new(
            move || PoolAwaitable::Pending(Box::pin(create())),
            |_| (),
        ))
    }

    pub async fn pull(&self) -> RefGuard<LinearReusable<'_, PoolAwaitable<T>>, &mut T> {
        RefGuard::new_deref_mut(self.0.pull()).map_await().await
    }
}
