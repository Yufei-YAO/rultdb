pub mod error;
pub mod page;
pub mod tx;
pub mod freelist;
pub mod bucket;
pub mod node;
pub mod cursor;
pub mod db;
pub mod config;


const MAX_KEY_SIZE: usize = 32768;

const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;

pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

const DEFAULT_FILL_PERCENT: f64 = 0.5;