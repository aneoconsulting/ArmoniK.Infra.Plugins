use json_patch::{AddOperation, PatchOperation};
use k8s_openapi::api::core::v1::Pod;
use kube::{api::PatchParams, Api};

use super::{WorkerUpdate, WorkerUpdater};

/// [`WorkerUpdater`] for Kubernetes Pods
#[derive(Clone)]
pub struct PodUpdater {
    /// Kubernetes client
    client: kube::Client,
    /// Maximum concurrency for [`PodUpdater::update_many`]
    concurrency: usize,
}

impl PodUpdater {
    /// Create a new [`PodUpdater`]
    ///
    /// The configuration for Kubernetes is fetched from configuration files and/or environment variables
    pub async fn new(concurrency: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            client: kube::Client::try_default().await?,
            concurrency,
        })
    }
}

#[async_trait::async_trait]
impl WorkerUpdater for PodUpdater {
    async fn update(
        &self,
        update: WorkerUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let WorkerUpdate {
            name,
            namespace,
            cost,
        } = update;
        let client: Api<Pod> = Api::namespaced(self.client.clone(), &namespace);

        log::debug!("Update pod {name} with cost {cost}");

        let patch = PatchOperation::Add(AddOperation {
            path: "/metadata/annotations".to_owned(),
            value: serde_json::json!({
                "controller.kubernetes.io/pod-deletion-cost": cost.to_string(),
            }),
        });
        let patch = kube::api::Patch::Json::<()>(json_patch::Patch(vec![patch]));

        let patch_params = PatchParams::default();

        match client.patch(&name, &patch_params, &patch).await {
            Ok(_) => {
                log::trace!("Successfully patched pod {name} with cost {cost}");
                Ok(())
            }
            // Pod not found can be safely ignored as it happens during scale down
            Err(kube::Error::Api(kube::core::ErrorResponse { code: 404, .. })) => {
                log::trace!("Could not patch pod {name} with cost {cost}: not found");
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }
}
