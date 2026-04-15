#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unreachable_code
)]

pub mod io_utils;
pub mod join;
pub mod relational;
pub mod sort;

use crate::data::Row;

pub trait Operator {
    fn next(&mut self) -> Option<Row>;
}

// Re-export operators for easier access
pub use join::{CrossOperator, GraceHashJoinOperator};
pub use relational::{FilterOperator, ProjectOperator, ProjectSource, ScanOperator};
pub use sort::SortOperator;
