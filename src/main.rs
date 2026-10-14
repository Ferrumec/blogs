mod events;
mod models;
mod module;
mod support;

use ferrumec::launch::launch;
use ferrumec::LocalInfra;
use module::BlogsModule;
use std::io;

#[actix_web::main]
async fn main() -> io::Result<()> {
    // Reads DATABASE_URL. Swap in RemoteInfra (feature `distributed`) for
    // Redis + NATS without touching the module.
    let infra = LocalInfra::new()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let modules = ferrumec::modules!(infra.clone(); ("blogs", BlogsModule))
        .map_err(|e| io::Error::other(e.to_string()))?;

    launch(modules, infra).await
}
