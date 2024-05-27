use futures::{StreamExt, TryStreamExt};

pub mod pod;

/// Description of a cost update for a worker
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerUpdate {
    /// Name of the worker
    pub name: String,
    /// Namespace of the worker
    pub namespace: String,
    /// New cost for the worker
    pub cost: i32,
}

/// Trait to update cost of workers
#[async_trait::async_trait]
pub trait WorkerUpdater {
    /// Update a single worker
    async fn update(
        &self,
        update: WorkerUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Get the maximum concurrency for the updater
    fn concurrency(&self) -> usize {
        1
    }

    /// Update many workers
    ///
    /// remarks: It respects the imposed concurrency of the updater
    async fn update_many(
        &self,
        updates: Vec<WorkerUpdate>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let concurrency = self.concurrency();

        let mut stream = futures::stream::iter(updates)
            .map(|update| self.update(update))
            .buffer_unordered(concurrency);

        while stream.try_next().await?.is_some() {}
        Ok(())
    }
}
