use std::{
    future::{Future, IntoFuture},
    pin::Pin,
};

use lockfree_object_pool::{LinearObjectPool, LinearReusable};
use tokio_rusqlite::{Connection, Params};

use crate::ref_guard::RefGuard;

pub enum PoolAwaitable<T> {
    Pending(Pin<Box<dyn Future<Output = T> + Send + Sync>>),
    Ready(T),
    Unreachable,
}

pub enum PoolFuture<'a, T> {
    Pending(
        Pin<Box<dyn Future<Output = T> + Send + Sync>>,
        &'a mut PoolAwaitable<T>,
    ),
    Ready(&'a mut T),
    Unreachable,
}

impl<'a, T> IntoFuture for &'a mut PoolAwaitable<T> {
    type Output = &'a mut T;
    type IntoFuture = PoolFuture<'a, T>;

    fn into_future(self) -> Self::IntoFuture {
        match self {
            PoolAwaitable::Pending(_) => {
                match std::mem::replace(self, PoolAwaitable::Unreachable) {
                    PoolAwaitable::Pending(fut) => PoolFuture::Pending(fut, self),
                    _ => unreachable!(),
                }
            }
            PoolAwaitable::Ready(val) => PoolFuture::Ready(val),
            PoolAwaitable::Unreachable => unreachable!(),
        }
    }
}

impl<'a, T> Future for PoolFuture<'a, T> {
    type Output = &'a mut T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match std::mem::replace(this, PoolFuture::Unreachable) {
            PoolFuture::Pending(mut future, awaitable) => match future.as_mut().poll(cx) {
                std::task::Poll::Ready(val) => {
                    *awaitable = PoolAwaitable::Ready(val);
                    match awaitable {
                        PoolAwaitable::Ready(val) => std::task::Poll::Ready(val),
                        _ => unreachable!(),
                    }
                }
                std::task::Poll::Pending => {
                    *this = PoolFuture::Pending(future, awaitable);
                    std::task::Poll::Pending
                }
            },
            PoolFuture::Ready(val) => std::task::Poll::Ready(val),
            PoolFuture::Unreachable => unreachable!(),
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

impl AsyncPool<Connection> {
    pub async fn execute_batch(
        &self,
        sql: &str,
        span: tracing::Span,
    ) -> Result<(), rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |conn| conn.execute_batch(&sql)).await
    }
    pub async fn execute(
        &self,
        sql: &str,
        params: impl Params + Send + 'static,
        span: tracing::Span,
    ) -> Result<usize, rusqlite::Error> {
        let sql = sql.to_owned();
        self.call(span, move |conn| conn.execute(&sql, params))
            .await
    }

    pub async fn call<Out: Send + 'static>(
        &self,
        span: tracing::Span,
        f: impl FnOnce(&mut rusqlite::Connection) -> Out + Send + 'static,
    ) -> Out {
        self.pull()
            .await
            .call_unwrap(|conn| {
                let _entered = span.entered();
                f(conn)
            })
            .await
    }
}
