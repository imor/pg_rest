//! pg_rest: asynchronous HTTP requests from Postgres, with a pipelined background worker.

use pgrx::prelude::*;

mod api;
mod consts;
mod mem;
mod schema;
mod shmem;
mod worker;

::pgrx::pg_module_magic!(name, version);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    if unsafe { pg_sys::IsBinaryUpgrade } {
        return;
    }

    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "pg_rest is not in shared_preload_libraries",
            "Add pg_rest to the shared_preload_libraries configuration variable in postgresql.conf."
        );
    }

    shmem::init();
    worker::register();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_rest'"]
    }
}
