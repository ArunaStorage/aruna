#[macro_use]
extern crate diesel;

#[macro_use]
extern crate lazy_static;

pub mod config;
pub mod database;
pub mod error;
pub mod server;
#[macro_use]
pub mod aruna_macros;
