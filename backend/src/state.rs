use std::sync::Arc;

use sqlx::PgPool;

use crate::common::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub postgres: PgPool,
    pub metrics: Arc<Metrics>,
}
