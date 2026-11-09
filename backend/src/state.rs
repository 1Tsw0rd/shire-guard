use std::sync::Arc;

use sqlx::PgPool;

use crate::{common::metrics::Metrics, core::enrichment::service::EnrichmentService};

#[derive(Clone)]
pub struct AppState {
    pub postgres: PgPool,
    pub metrics: Arc<Metrics>,
    pub enrichment: Arc<EnrichmentService>,
}
