#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    unused_mut,
    unreachable_code
)]
use anyhow::{Context, Result};
use clap::Parser;
use common::query::{Query, QueryOp};
use db_config::DbContext;
use db_config::statistics::ColumnStat;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};

mod buffer_pool;
mod cli;
mod data;
mod io_setup;
mod operators;

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
    operators::{
        CrossOperator, FilterOperator, Operator, ProjectOperator, ScanOperator, SortOperator,
        relational::compile_predicates,
    },
};

const DEFAULT_MAX_QUERY_RAM_BYTES: usize = 64 * 1024 * 1024;
// Scans/scratch currently use `read_pages_direct_into` (bypassing the page cache), so spending
// large RAM on buffer pool frames doesn't reduce I/O. Keep it small and spend RAM on operators.
const MIN_BUFFER_POOL_BYTES: usize = 1 * 1024 * 1024;
const MAX_BUFFER_POOL_BYTES: usize = 2 * 1024 * 1024;
const MIN_EXECUTION_BUDGET_BYTES: usize = 8 * 1024 * 1024;
const MIN_SCAN_PREFETCH_BYTES: usize = 1 * 1024 * 1024;
const MAX_SCAN_PREFETCH_BYTES: usize = 8 * 1024 * 1024;
const MIN_OPERATOR_BUDGET_BYTES: usize = 512 * 1024;
const DEFAULT_BASE_CARDINALITY: f64 = 1000.0;
const DEFAULT_EQ_SELECTIVITY: f64 = 0.1;
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
const DEFAULT_JOIN_SELECTIVITY: f64 = 0.1;

#[derive(Clone, Copy)]
struct ColumnStatsSummary {
    cardinality: Option<f64>,
    range_min: Option<f64>,
    range_max: Option<f64>,
}

#[derive(Clone, Copy)]
struct TableStatsSummary {
    base_cardinality: f64,
}

struct StatsIndex {
    tables: HashMap<String, TableStatsSummary>,
    columns: HashMap<String, ColumnStatsSummary>,
    table_positions: HashMap<String, usize>,
}

impl StatsIndex {
    fn new(ctx: &DbContext) -> Self {
        let mut tables = HashMap::with_capacity(ctx.get_table_specs().len());
        let mut columns = HashMap::new();
        let mut table_positions = HashMap::with_capacity(ctx.get_table_specs().len());

        for (table_idx, table) in ctx.get_table_specs().iter().enumerate() {
            table_positions.insert(table.name.clone(), table_idx);
            let mut base_cardinality: Option<f64> = None;
            for col in &table.column_specs {
                let mut cardinality = None;
                let mut range_min = None;
                let mut range_max = None;

                if let Some(stats) = &col.stats {
                    for stat in stats {
                        match stat {
                            ColumnStat::CardinalityStat(val) => {
                                let card = val.0.max(1) as f64;
                                cardinality = Some(card);
                                base_cardinality = Some(base_cardinality.unwrap_or(0.0).max(card));
                            }
                            ColumnStat::RangeStat(range) => {
                                range_min = stat_data_as_f64(&range.lower_bound);
                                range_max = stat_data_as_f64(&range.upper_bound);
                            }
                            _ => {}
                        }
                    }
                }

                columns.insert(
                    col.column_name.clone(),
                    ColumnStatsSummary {
                        cardinality,
                        range_min,
                        range_max,
                    },
                );
            }

            tables.insert(
                table.name.clone(),
                TableStatsSummary {
                    base_cardinality: base_cardinality.unwrap_or(DEFAULT_BASE_CARDINALITY),
                },
            );
        }

        Self {
            tables,
            columns,
            table_positions,
        }
    }

    #[inline(always)]
    fn table_cardinality(&self, table_name: &str) -> f64 {
        self.tables
            .get(table_name)
            .map(|stats| stats.base_cardinality)
            .unwrap_or(DEFAULT_BASE_CARDINALITY)
    }

    #[inline(always)]
    fn column_stats(&self, column_name: &str) -> Option<ColumnStatsSummary> {
        self.columns.get(column_name).copied()
    }

    #[inline(always)]
    fn table_spec<'a>(
        &self,
        ctx: &'a DbContext,
        table_name: &str,
    ) -> &'a db_config::table::TableSpec {
        &ctx.get_table_specs()[*self
            .table_positions
            .get(table_name)
            .expect("Table not found")]
    }

    fn predicate_selectivity(&self, predicate: &common::query::Predicate) -> f64 {
        use common::query::{ComparisionOperator, ComparisionValue};

        match (&predicate.operator, &predicate.value) {
            (ComparisionOperator::EQ, ComparisionValue::Column(rhs_col)) => {
                let left_card = self
                    .column_stats(&predicate.column_name)
                    .and_then(|stats| stats.cardinality);
                let right_card = self
                    .column_stats(rhs_col)
                    .and_then(|stats| stats.cardinality);

                match (left_card, right_card) {
                    (Some(left), Some(right)) => (1.0 / left.max(right)).clamp(0.000_001, 1.0),
                    (Some(left), None) | (None, Some(left)) => {
                        (1.0 / left.max(1.0)).clamp(0.000_001, 1.0)
                    }
                    (None, None) => DEFAULT_JOIN_SELECTIVITY,
                }
            }
            (ComparisionOperator::NE, ComparisionValue::Column(rhs_col)) => {
                1.0 - self.predicate_selectivity(&common::query::Predicate {
                    column_name: predicate.column_name.clone(),
                    operator: ComparisionOperator::EQ,
                    value: ComparisionValue::Column(rhs_col.clone()),
                })
            }
            _ => {
                let stats = match self.column_stats(&predicate.column_name) {
                    Some(stats) => stats,
                    None => {
                        return match predicate.operator {
                            ComparisionOperator::EQ => DEFAULT_EQ_SELECTIVITY,
                            ComparisionOperator::NE => 1.0 - DEFAULT_EQ_SELECTIVITY,
                            _ => DEFAULT_RANGE_SELECTIVITY,
                        };
                    }
                };

                match predicate.operator {
                    ComparisionOperator::EQ => stats
                        .cardinality
                        .map(|card| (1.0 / card.max(1.0)).clamp(0.000_001, 1.0))
                        .unwrap_or(DEFAULT_EQ_SELECTIVITY),
                    ComparisionOperator::NE => stats
                        .cardinality
                        .map(|card| (1.0 - (1.0 / card.max(1.0))).clamp(0.01, 1.0))
                        .unwrap_or(1.0 - DEFAULT_EQ_SELECTIVITY),
                    ComparisionOperator::GT
                    | ComparisionOperator::GTE
                    | ComparisionOperator::LT
                    | ComparisionOperator::LTE => {
                        if let (Some(min), Some(max), Some(val)) = (
                            stats.range_min,
                            stats.range_max,
                            comp_val_as_f64(&predicate.value),
                        ) {
                            if max <= min {
                                return DEFAULT_RANGE_SELECTIVITY;
                            }
                            let fraction = match predicate.operator {
                                ComparisionOperator::GT | ComparisionOperator::GTE => {
                                    (max - val) / (max - min)
                                }
                                ComparisionOperator::LT | ComparisionOperator::LTE => {
                                    (val - min) / (max - min)
                                }
                                _ => unreachable!(),
                            };
                            fraction.clamp(0.01, 1.0)
                        } else {
                            DEFAULT_RANGE_SELECTIVITY
                        }
                    }
                }
            }
        }
    }

    fn estimate_cardinality(&self, op: &QueryOp) -> f64 {
        match op {
            QueryOp::Scan(data) => self.table_cardinality(&data.table_id),
            QueryOp::Filter(data) => {
                let child_est = self.estimate_cardinality(&data.underlying);
                let combined_selectivity =
                    data.predicates
                        .iter()
                        .enumerate()
                        .fold(1.0, |acc, (idx, pred)| {
                            let raw = self.predicate_selectivity(pred);
                            let dampened = if idx == 0 { raw } else { raw.powf(0.75) };
                            acc * dampened
                        });
                (child_est * combined_selectivity.max(0.01)).max(1.0)
            }
            QueryOp::Cross(data) => (self.estimate_cardinality(&data.left)
                * self.estimate_cardinality(&data.right))
            .max(1.0),
            QueryOp::Project(data) => self.estimate_cardinality(&data.underlying),
            QueryOp::Sort(data) => self.estimate_cardinality(&data.underlying),
        }
    }
}

struct RequiredColumnIndex {
    exact: HashSet<String>,
    unqualified: HashSet<String>,
}

impl RequiredColumnIndex {
    fn new(required_cols: &HashSet<String>) -> Self {
        let mut unqualified = HashSet::with_capacity(required_cols.len());
        for col in required_cols {
            let suffix = col.rsplit('.').next().unwrap_or(col);
            unqualified.insert(suffix.to_string());
        }
        Self {
            exact: required_cols.clone(),
            unqualified,
        }
    }

    fn scan_mask(&self, spec: &db_config::table::TableSpec) -> Vec<bool> {
        spec.column_specs
            .iter()
            .map(|col| {
                self.exact.contains(&col.column_name) || self.unqualified.contains(&col.column_name)
            })
            .collect()
    }
}

fn resolve_project_mapping<'a>(
    entry: &'a (String, String),
    _child_schema: &HashMap<String, usize>,
) -> (&'a String, &'a String) {
    (&entry.1, &entry.0)
}

fn get_output_schema(op_node: &QueryOp, ctx: &DbContext) -> HashMap<String, usize> {
    match op_node {
        QueryOp::Scan(data) => {
            let mut map = HashMap::new();
            let spec = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.name == data.table_id)
                .unwrap();
            for (i, col) in spec.column_specs.iter().enumerate() {
                map.insert(col.column_name.clone(), i);
            }
            map
        }
        QueryOp::Filter(data) => get_output_schema(&data.underlying, ctx),
        QueryOp::Sort(data) => get_output_schema(&data.underlying, ctx),
        QueryOp::Project(data) => {
            let child_schema = get_output_schema(&data.underlying, ctx);
            let mut map = HashMap::new();
            for (i, mapping) in data.column_name_map.iter().enumerate() {
                let (out_name, _) = resolve_project_mapping(mapping, &child_schema);
                map.insert(out_name.clone(), i);
            }
            map
        }
        QueryOp::Cross(data) => {
            let mut left_map = get_output_schema(&data.left, ctx);
            let right_map = get_output_schema(&data.right, ctx);
            let offset = left_map.len();
            for (k, v) in right_map {
                left_map.insert(k, v + offset);
            }
            left_map
        }
    }
}


fn collect_referenced_columns(op: &QueryOp, out: &mut HashSet<String>) {
    match op {
        QueryOp::Scan(_) => {}
        QueryOp::Filter(data) => {
            for p in &data.predicates {
                out.insert(p.column_name.clone());
                if let common::query::ComparisionValue::Column(ref rhs) = p.value {
                    out.insert(rhs.clone());
                }
            }
            collect_referenced_columns(&data.underlying, out);
        }
        QueryOp::Project(data) => {
            for mapping in &data.column_name_map {
                let src = &mapping.1;
                if !src.starts_with('\'') && !src.starts_with('"') {
                    out.insert(src.clone());
                }
            }
            collect_referenced_columns(&data.underlying, out);
        }
        QueryOp::Sort(data) => {
            for s in &data.sort_specs {
                out.insert(s.column_name.clone());
            }
            collect_referenced_columns(&data.underlying, out);
        }
        QueryOp::Cross(data) => {
            collect_referenced_columns(&data.left, out);
            collect_referenced_columns(&data.right, out);
        }
    }
}

fn stat_data_as_f64(data: &common::Data) -> Option<f64> {
    match data {
        common::Data::Int32(v) => Some(*v as f64),
        common::Data::Int64(v) => Some(*v as f64),
        common::Data::Float32(v) => Some(*v as f64),
        common::Data::Float64(v) => Some(*v),
        common::Data::String(_) => None,
    }
}

fn comp_val_as_f64(val: &common::query::ComparisionValue) -> Option<f64> {
    match val {
        common::query::ComparisionValue::I32(v) => Some(*v as f64),
        common::query::ComparisionValue::I64(v) => Some(*v as f64),
        common::query::ComparisionValue::F32(v) => Some(*v as f64),
        common::query::ComparisionValue::F64(v) => Some(*v),
        _ => None,
    }
}

#[inline(always)]
fn scan_prefetch_blocks(block_size: usize, per_op_budget: usize) -> usize {
    let target_bytes = (per_op_budget / 2).clamp(MIN_SCAN_PREFETCH_BYTES, MAX_SCAN_PREFETCH_BYTES);
    (target_bytes / block_size).max(1)
}

fn flatten_cross(op: QueryOp, out: &mut Vec<QueryOp>) {
    match op {
        QueryOp::Cross(data) => {
            flatten_cross(*data.left, out);
            flatten_cross(*data.right, out);
        }
        other => out.push(other),
    }
}

fn build_left_deep_cross(mut branches: Vec<QueryOp>) -> QueryOp {
    let first = branches.remove(0);
    branches.into_iter().fold(first, |left, right| {
        QueryOp::Cross(common::query::CrossData {
            left: Box::new(left),
            right: Box::new(right),
        })
    })
}

fn predicate_connects_schemas(
    pred: &common::query::Predicate,
    current_schema: &HashMap<String, usize>,
    candidate_schema: &HashMap<String, usize>,
) -> bool {
    let lhs_in_current = current_schema.contains_key(&pred.column_name);
    let lhs_in_candidate = candidate_schema.contains_key(&pred.column_name);
    match &pred.value {
        common::query::ComparisionValue::Column(rhs_col) => {
            let rhs_in_current = current_schema.contains_key(rhs_col);
            let rhs_in_candidate = candidate_schema.contains_key(rhs_col);
            (lhs_in_current && rhs_in_candidate) || (lhs_in_candidate && rhs_in_current)
        }
        _ => false,
    }
}

fn has_join_predicates(predicates: &[common::query::Predicate]) -> bool {
    predicates
        .iter()
        .any(|pred| matches!(pred.value, common::query::ComparisionValue::Column(_)))
}

fn reorder_cross_branches(
    cross_data: common::query::CrossData,
    predicates: &[common::query::Predicate],
    ctx: &DbContext,
    stats_index: &StatsIndex,
) -> common::query::CrossData {
    let mut branches = Vec::new();
    flatten_cross(QueryOp::Cross(cross_data), &mut branches);
    if branches.len() <= 2 {
        if let QueryOp::Cross(data) = build_left_deep_cross(branches) {
            return data;
        }
        unreachable!();
    }
    let mut remaining: Vec<(QueryOp, HashMap<String, usize>, f64)> = branches
        .into_iter()
        .map(|branch| {
            let schema = get_output_schema(&branch, ctx);
            let est = stats_index.estimate_cardinality(&branch);
            (branch, schema, est)
        })
        .collect();

    let start_idx = remaining
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(idx, _)| idx)
        .unwrap();
    let (first_branch, first_schema, _) = remaining.remove(start_idx);
    let mut ordered = vec![first_branch];
    let mut current_schema = first_schema;
    let mut current_est = stats_index.estimate_cardinality(&ordered[0]);

    while !remaining.is_empty() {
        // Prefer branches that connect via join predicates and that minimize the estimated
        // intermediate result size (not just the smallest base relation).
        let mut best_connected: Option<(usize, f64, f64)> = None; // (idx, score, selectivity)
        for (idx, (_, schema, est)) in remaining.iter().enumerate() {
            let mut sel = 1.0f64;
            let mut used = 0usize;
            for pred in predicates {
                if predicate_connects_schemas(pred, &current_schema, schema) {
                    // Combine selectivities; dampen later predicates a bit to avoid extreme products.
                    let raw = stats_index.predicate_selectivity(pred);
                    let damp = if used == 0 { raw } else { raw.powf(0.75) };
                    sel *= damp;
                    used += 1;
                }
            }
            if used == 0 {
                continue;
            }
            // Estimated output cardinality for joining current with candidate.
            let score = (current_est * (*est) * sel.max(0.000_001)).max(1.0);
            match best_connected {
                None => best_connected = Some((idx, score, sel)),
                Some((_, best_score, _)) => {
                    if score < best_score {
                        best_connected = Some((idx, score, sel));
                    }
                }
            }
        }

        let next_idx = remaining
            .iter()
            .enumerate()
            .filter(|(_, (_, schema, _))| {
                predicates
                    .iter()
                    .any(|pred| predicate_connects_schemas(pred, &current_schema, schema))
            })
            .min_by(|(idx_a, a), (idx_b, b)| {
                // If both are connected, compare by estimated intermediate size.
                let score_a = best_connected
                    .as_ref()
                    .and_then(|(best_idx, best_score, _)| (*best_idx == *idx_a).then_some(*best_score))
                    .unwrap_or(a.2);
                let score_b = best_connected
                    .as_ref()
                    .and_then(|(best_idx, best_score, _)| (*best_idx == *idx_b).then_some(*best_score))
                    .unwrap_or(b.2);
                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, _)| idx)
            .or_else(|| {
                remaining
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| {
                        a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(idx, _)| idx)
            })
            .unwrap();

        let (branch, schema, _) = remaining.remove(next_idx);
        let offset = current_schema.len();
        for (k, v) in schema {
            current_schema.insert(k, v + offset);
        }
        ordered.push(branch);

        // Update current estimate using selectivity of predicates that connect the new branch.
        // This keeps the scoring meaningful for the next selection.
        let joined_schema = &current_schema;
        let newly_added_schema = get_output_schema(ordered.last().unwrap(), ctx);
        let mut sel = 1.0f64;
        let mut used = 0usize;
        for pred in predicates {
            if predicate_connects_schemas(pred, joined_schema, &newly_added_schema) {
                let raw = stats_index.predicate_selectivity(pred);
                let damp = if used == 0 { raw } else { raw.powf(0.75) };
                sel *= damp;
                used += 1;
            }
        }
        current_est = (current_est * stats_index.estimate_cardinality(ordered.last().unwrap()) * sel.max(0.000_001)).max(1.0);
    }
    if let QueryOp::Cross(data) = build_left_deep_cross(ordered) {
        data
    } else {
        unreachable!()
    }
}

#[inline(always)]
fn predicate_referenced_columns<'a>(pred: &'a common::query::Predicate) -> (&'a str, Option<&'a str>) {
    match &pred.value {
        common::query::ComparisionValue::Column(rhs) => (pred.column_name.as_str(), Some(rhs.as_str())),
        _ => (pred.column_name.as_str(), None),
    }
}

fn predicate_is_resolvable_in_union(
    pred: &common::query::Predicate,
    left_cols: &std::collections::HashSet<&str>,
    right_cols: &std::collections::HashSet<&str>,
) -> bool {
    let (lhs, rhs) = predicate_referenced_columns(pred);
    let in_union = |c: &str| left_cols.contains(c) || right_cols.contains(c);
    if !in_union(lhs) {
        return false;
    }
    if let Some(r) = rhs {
        in_union(r)
    } else {
        true
    }
}

fn build_join_chain(
    mut branches: Vec<QueryOp>,
    mut predicates: Vec<common::query::Predicate>,
    ctx: &DbContext,
) -> QueryOp {
    debug_assert!(!branches.is_empty());
    if branches.len() == 1 {
        if predicates.is_empty() {
            return branches.remove(0);
        }
        return QueryOp::Filter(common::query::FilterData {
            predicates,
            underlying: Box::new(branches.remove(0)),
        });
    }

    let mut current = branches.remove(0);
    let mut current_schema = get_output_schema(&current, ctx);

    for next in branches {
        let next_schema = get_output_schema(&next, ctx);

        let left_cols: std::collections::HashSet<&str> =
            current_schema.keys().map(|s| s.as_str()).collect();
        let right_cols: std::collections::HashSet<&str> =
            next_schema.keys().map(|s| s.as_str()).collect();

        // Apply any predicates that become evaluatable once `next` is joined in.
        let mut step_preds = Vec::new();
        let mut remaining = Vec::new();
        for p in predicates {
            if predicate_is_resolvable_in_union(&p, &left_cols, &right_cols) {
                step_preds.push(p);
            } else {
                remaining.push(p);
            }
        }
        predicates = remaining;

        let cross = QueryOp::Cross(common::query::CrossData {
            left: Box::new(current),
            right: Box::new(next),
        });
        current = if step_preds.is_empty() {
            cross
        } else {
            QueryOp::Filter(common::query::FilterData {
                predicates: step_preds,
                underlying: Box::new(cross),
            })
        };

        // Update schema for subsequent steps.
        let mut combined_schema = current_schema;
        let offset = combined_schema.len();
        for (k, v) in next_schema {
            combined_schema.insert(k, v + offset);
        }
        current_schema = combined_schema;
    }

    if predicates.is_empty() {
        current
    } else {
        QueryOp::Filter(common::query::FilterData {
            predicates,
            underlying: Box::new(current),
        })
    }
}

fn optimize_ast(op_node: QueryOp, ctx: &DbContext, stats_index: &StatsIndex) -> QueryOp {
    match op_node {
        QueryOp::Scan(_) => op_node,

        QueryOp::Filter(mut filter_data) => {

            filter_data.predicates.sort_by(|a, b| {
                let a_is_string = matches!(a.value, common::query::ComparisionValue::String(_));
                let b_is_string = matches!(b.value, common::query::ComparisionValue::String(_));
                let a_key = (a_is_string, stats_index.predicate_selectivity(a));
                let b_key = (b_is_string, stats_index.predicate_selectivity(b));
                a_key
                    .partial_cmp(&b_key)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let optimized_child = optimize_ast(*filter_data.underlying, ctx, stats_index);

            match optimized_child {
                QueryOp::Sort(mut sort_data) => {
                    filter_data.underlying = sort_data.underlying;
                    sort_data.underlying =
                        Box::new(optimize_ast(QueryOp::Filter(filter_data), ctx, stats_index));
                    QueryOp::Sort(sort_data)
                }
                QueryOp::Project(mut proj_data) => {
                    let child_schema = get_output_schema(&proj_data.underlying, ctx);
                    for pred in &mut filter_data.predicates {
                        for mapping in &proj_data.column_name_map {
                            let (out_name, src_name) =
                                resolve_project_mapping(mapping, &child_schema);
                            if pred.column_name == *out_name {
                                pred.column_name = src_name.clone();
                                break;
                            }
                        }
                        if let common::query::ComparisionValue::Column(ref mut rhs_col) = pred.value
                        {
                            for mapping in &proj_data.column_name_map {
                                let (out_name, src_name) =
                                    resolve_project_mapping(mapping, &child_schema);
                                if *rhs_col == *out_name {
                                    *rhs_col = src_name.clone();
                                    break;
                                }
                            }
                        }
                    }
                    filter_data.underlying = proj_data.underlying;
                    proj_data.underlying =
                        Box::new(optimize_ast(QueryOp::Filter(filter_data), ctx, stats_index));
                    QueryOp::Project(proj_data)
                }
                QueryOp::Cross(cross_data) => {
                    let mut cross_data = reorder_cross_branches(
                        cross_data,
                        &filter_data.predicates,
                        ctx,
                        stats_index,
                    );
                    let left_schema = get_output_schema(&cross_data.left, ctx);
                    let right_schema = get_output_schema(&cross_data.right, ctx);
                    let mut left_p = Vec::new();
                    let mut right_p = Vec::new();
                    let mut keep_p = Vec::new();

                    for p in filter_data.predicates {
                        let mut all_in_left = left_schema.contains_key(&p.column_name);
                        let mut all_in_right = right_schema.contains_key(&p.column_name);
                        if let common::query::ComparisionValue::Column(ref rhs_col) = p.value {
                            all_in_left = all_in_left && left_schema.contains_key(rhs_col);
                            all_in_right = all_in_right && right_schema.contains_key(rhs_col);
                        }
                        if all_in_left {
                            left_p.push(p);
                        } else if all_in_right {
                            right_p.push(p);
                        } else {
                            keep_p.push(p);
                        }
                    }

                    if !left_p.is_empty() {
                        cross_data.left = Box::new(optimize_ast(
                            QueryOp::Filter(common::query::FilterData {
                                predicates: left_p,
                                underlying: cross_data.left,
                            }),
                            ctx,
                            stats_index,
                        ));
                    }
                    if !right_p.is_empty() {
                        cross_data.right = Box::new(optimize_ast(
                            QueryOp::Filter(common::query::FilterData {
                                predicates: right_p,
                                underlying: cross_data.right,
                            }),
                            ctx,
                            stats_index,
                        ));
                    }

                    if keep_p.is_empty() {
                        QueryOp::Cross(cross_data)
                    } else {
                        // Turn N-way CROSS + big FILTER into a chain of FILTER(CROSS(...)) nodes.
                        // This allows the pipeline builder to repeatedly replace each step with a real join,
                        // rather than only using a single join predicate and post-filtering the rest.
                        let mut branches = Vec::new();
                        flatten_cross(QueryOp::Cross(cross_data), &mut branches);
                        build_join_chain(branches, keep_p, ctx)
                    }
                }
                other => {
                    filter_data.underlying = Box::new(other);
                    QueryOp::Filter(filter_data)
                }
            }
        }

        QueryOp::Project(proj_data) => {
            let proj_child_schema = get_output_schema(&proj_data.underlying, ctx);
            let optimized_child = optimize_ast(*proj_data.underlying, ctx, stats_index);
            match optimized_child {
                QueryOp::Scan(scan_data) => QueryOp::Project(common::query::ProjectData {
                    column_name_map: proj_data.column_name_map,
                    underlying: Box::new(QueryOp::Scan(scan_data)),
                }),
                QueryOp::Sort(mut sort_data) => {
                    let mut new_map = Vec::new();
                    let mut added = std::collections::HashSet::new();
                    for mapping in &proj_data.column_name_map {
                        let (_, src) = resolve_project_mapping(mapping, &proj_child_schema);
                        if added.insert(src.clone()) {
                            new_map.push((src.clone(), src.clone()));
                        }
                    }
                    for s in &sort_data.sort_specs {
                        if added.insert(s.column_name.clone()) {
                            new_map.push((s.column_name.clone(), s.column_name.clone()));
                        }
                    }
                    sort_data.underlying = Box::new(optimize_ast(
                        QueryOp::Project(common::query::ProjectData {
                            column_name_map: new_map,
                            underlying: sort_data.underlying,
                        }),
                        ctx,
                        stats_index,
                    ));
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(QueryOp::Sort(sort_data)),
                    })
                }
                QueryOp::Filter(mut filter_data) => {
                    if has_join_predicates(&filter_data.predicates) {
                        if let QueryOp::Cross(mut cross_data) = *filter_data.underlying {
                            let left_schema = get_output_schema(&cross_data.left, ctx);
                            let right_schema = get_output_schema(&cross_data.right, ctx);
                            let mut left_map = Vec::new();
                            let mut right_map = Vec::new();
                            let mut l_added = std::collections::HashSet::new();
                            let mut r_added = std::collections::HashSet::new();

                            for mapping in &proj_data.column_name_map {
                                let (_, src) = resolve_project_mapping(mapping, &proj_child_schema);
                                if left_schema.contains_key(src) {
                                    if l_added.insert(src.clone()) {
                                        left_map.push((src.clone(), src.clone()));
                                    }
                                } else if right_schema.contains_key(src) {
                                    if r_added.insert(src.clone()) {
                                        right_map.push((src.clone(), src.clone()));
                                    }
                                }
                            }

                            for pred in &filter_data.predicates {
                                if left_schema.contains_key(&pred.column_name) {
                                    if l_added.insert(pred.column_name.clone()) {
                                        left_map.push((
                                            pred.column_name.clone(),
                                            pred.column_name.clone(),
                                        ));
                                    }
                                } else if right_schema.contains_key(&pred.column_name) {
                                    if r_added.insert(pred.column_name.clone()) {
                                        right_map.push((
                                            pred.column_name.clone(),
                                            pred.column_name.clone(),
                                        ));
                                    }
                                }
                                if let common::query::ComparisionValue::Column(ref rhs) = pred.value
                                {
                                    if left_schema.contains_key(rhs) {
                                        if l_added.insert(rhs.clone()) {
                                            left_map.push((rhs.clone(), rhs.clone()));
                                        }
                                    } else if right_schema.contains_key(rhs) {
                                        if r_added.insert(rhs.clone()) {
                                            right_map.push((rhs.clone(), rhs.clone()));
                                        }
                                    }
                                }
                            }

                            cross_data.left = Box::new(optimize_ast(
                                QueryOp::Project(common::query::ProjectData {
                                    column_name_map: left_map,
                                    underlying: cross_data.left,
                                }),
                                ctx,
                                stats_index,
                            ));
                            cross_data.right = Box::new(optimize_ast(
                                QueryOp::Project(common::query::ProjectData {
                                    column_name_map: right_map,
                                    underlying: cross_data.right,
                                }),
                                ctx,
                                stats_index,
                            ));
                            filter_data.underlying = Box::new(QueryOp::Cross(cross_data));
                            return QueryOp::Project(common::query::ProjectData {
                                column_name_map: proj_data.column_name_map,
                                underlying: Box::new(QueryOp::Filter(filter_data)),
                            });
                        }
                        return QueryOp::Project(common::query::ProjectData {
                            column_name_map: proj_data.column_name_map,
                            underlying: Box::new(QueryOp::Filter(filter_data)),
                        });
                    }

                    let mut new_map = Vec::new();
                    let mut added = std::collections::HashSet::new();
                    for mapping in &proj_data.column_name_map {
                        let (_, src) = resolve_project_mapping(mapping, &proj_child_schema);
                        if added.insert(src.clone()) {
                            new_map.push((src.clone(), src.clone()));
                        }
                    }
                    for pred in &filter_data.predicates {
                        if added.insert(pred.column_name.clone()) {
                            new_map.push((pred.column_name.clone(), pred.column_name.clone()));
                        }
                        if let common::query::ComparisionValue::Column(ref rhs) = pred.value {
                            if added.insert(rhs.clone()) {
                                new_map.push((rhs.clone(), rhs.clone()));
                            }
                        }
                    }

                    let pushed_project = QueryOp::Project(common::query::ProjectData {
                        column_name_map: new_map,
                        underlying: filter_data.underlying,
                    });
                    let optimized_pushed_project = optimize_ast(pushed_project, ctx, stats_index);
                    let new_filter = QueryOp::Filter(common::query::FilterData {
                        predicates: filter_data.predicates,
                        underlying: Box::new(optimized_pushed_project),
                    });
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(new_filter),
                    })
                }

                QueryOp::Cross(mut cross_data) => {
                    let left_schema = get_output_schema(&cross_data.left, ctx);
                    let right_schema = get_output_schema(&cross_data.right, ctx);
                    let mut child_schema = left_schema.clone();
                    let offset = child_schema.len();
                    for (k, v) in &right_schema {
                        child_schema.insert(k.clone(), v + offset);
                    }

                    let mut left_map = Vec::new();
                    let mut right_map = Vec::new();
                    let mut l_added = std::collections::HashSet::new();
                    let mut r_added = std::collections::HashSet::new();

                    for mapping in &proj_data.column_name_map {
                        let (_, src) = resolve_project_mapping(mapping, &child_schema);
                        if left_schema.contains_key(src) {
                            if l_added.insert(src.clone()) {
                                left_map.push((src.clone(), src.clone()));
                            }
                        } else if right_schema.contains_key(src) {
                            if r_added.insert(src.clone()) {
                                right_map.push((src.clone(), src.clone()));
                            }
                        }
                    }

                    cross_data.left = Box::new(optimize_ast(
                        QueryOp::Project(common::query::ProjectData {
                            column_name_map: left_map,
                            underlying: cross_data.left,
                        }),
                        ctx,
                        stats_index,
                    ));
                    cross_data.right = Box::new(optimize_ast(
                        QueryOp::Project(common::query::ProjectData {
                            column_name_map: right_map,
                            underlying: cross_data.right,
                        }),
                        ctx,
                        stats_index,
                    ));
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(QueryOp::Cross(cross_data)),
                    })
                }

                other => QueryOp::Project(common::query::ProjectData {
                    column_name_map: proj_data.column_name_map,
                    underlying: Box::new(other),
                }),
            }
        }

        QueryOp::Sort(mut d) => {
            d.underlying = Box::new(optimize_ast(*d.underlying, ctx, stats_index));
            QueryOp::Sort(d)
        }
        QueryOp::Cross(mut d) => {
            d.left = Box::new(optimize_ast(*d.left, ctx, stats_index));
            d.right = Box::new(optimize_ast(*d.right, ctx, stats_index));
            QueryOp::Cross(d)
        }
    }
}


fn build_pipeline<'a, R: Read, W: Write>(
    op_node: QueryOp,
    ctx: &'a DbContext,
    stats_index: &StatsIndex,
    pool: &'a mut buffer_pool::BufferPoolManager<R, W>,
    total_budget_bytes: usize,
    tree_depth: usize,
    required_cols: &RequiredColumnIndex,
) -> Box<dyn Operator + 'a> {
    let per_op_budget = (total_budget_bytes / (tree_depth + 1)).max(MIN_OPERATOR_BUDGET_BYTES);

    match op_node {
        QueryOp::Scan(data) => {
            let spec = stats_index.table_spec(ctx, &data.table_id);
            let required_column_mask = required_cols.scan_mask(spec);

            let prefetch_blocks = scan_prefetch_blocks(pool.disk_manager.block_size, per_op_budget);

            let empty_predicates: Vec<crate::operators::relational::CompiledPredicate> = Vec::new();
            let empty_zone_maps: HashMap<u64, crate::operators::relational::BlockZoneMap> =
                HashMap::new();

            Box::new(ScanOperator::new(
                &data.table_id,
                spec,
                pool,
                empty_predicates,
                empty_zone_maps,
                required_column_mask,
                prefetch_blocks,
            ))
        }

        QueryOp::Filter(mut data) => {
            if let QueryOp::Scan(scan_data) = *data.underlying {
                let spec = stats_index.table_spec(ctx, &scan_data.table_id);
                let scan_schema = get_output_schema(
                    &QueryOp::Scan(common::query::ScanData {
                        table_id: scan_data.table_id.clone(),
                    }),
                    ctx,
                );
                let compiled_predicates = compile_predicates(data.predicates, &scan_schema);
                let required_column_mask = required_cols.scan_mask(spec);
                let prefetch_blocks =
                    scan_prefetch_blocks(pool.disk_manager.block_size, per_op_budget);
                let empty_zone_maps: HashMap<u64, crate::operators::relational::BlockZoneMap> =
                    HashMap::new();

                return Box::new(ScanOperator::new(
                    &scan_data.table_id,
                    spec,
                    pool,
                    compiled_predicates,
                    empty_zone_maps,
                    required_column_mask,
                    prefetch_blocks,
                ));
            }

            if let QueryOp::Cross(cross_data) = *data.underlying {
                let left_schema = get_output_schema(&cross_data.left, ctx);
                let right_schema = get_output_schema(&cross_data.right, ctx);

                let mut join_pred_idx = None;
                for (i, pred) in data.predicates.iter().enumerate() {
                    if matches!(pred.operator, common::query::ComparisionOperator::EQ) {
                        if let common::query::ComparisionValue::Column(ref rhs_col) = pred.value {
                            let is_bridge = (left_schema.contains_key(&pred.column_name)
                                && right_schema.contains_key(rhs_col))
                                || (left_schema.contains_key(rhs_col)
                                    && right_schema.contains_key(&pred.column_name));
                            if is_bridge {
                                join_pred_idx =
                                    Some((i, pred.column_name.clone(), rhs_col.clone()));
                                break;
                            }
                        }
                    }
                }

                if let Some((idx, l_col_name, r_col_name)) = join_pred_idx {
                    let (left_col_idx, right_col_idx) = if left_schema.contains_key(&l_col_name) {
                        (left_schema[&l_col_name], right_schema[&r_col_name])
                    } else {
                        (left_schema[&r_col_name], right_schema[&l_col_name])
                    };

                    let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;
                    let scratch_block_size = unsafe { &*pool_ptr }.disk_manager.block_size;

                    let left_est_rows = stats_index.estimate_cardinality(&cross_data.left);
                    let right_est_rows = stats_index.estimate_cardinality(&cross_data.right);

                    let left_child = build_pipeline(
                        *cross_data.left,
                        ctx,
                        stats_index,
                        pool,
                        total_budget_bytes,
                        tree_depth + 1,
                        required_cols,
                    );
                    let right_child = build_pipeline(
                        *cross_data.right,
                        ctx,
                        stats_index,
                        unsafe { &mut *pool_ptr },
                        total_budget_bytes,
                        tree_depth + 1,
                        required_cols,
                    );

                    let join_op: Box<dyn Operator + 'a> =
                        Box::new(crate::operators::join::AdaptiveJoinOperator::new(
                            left_child,
                            right_child,
                            left_col_idx,
                            right_col_idx,
                            per_op_budget,
                            left_est_rows,
                            right_est_rows,
                            pool_ptr,
                            scratch_block_size,
                        ));

                    data.predicates.remove(idx);
                    if data.predicates.is_empty() {
                        return join_op;
                    } else {
                        let mut combined_schema = left_schema;
                        let offset = combined_schema.len();
                        for (k, v) in right_schema {
                            combined_schema.insert(k, v + offset);
                        }
                        return Box::new(FilterOperator::new(
                            join_op,
                            data.predicates,
                            combined_schema,
                        ));
                    }
                }
                data.underlying = Box::new(QueryOp::Cross(cross_data));
            }

            let schema = get_output_schema(&data.underlying, ctx);
            Box::new(FilterOperator::new(
                build_pipeline(
                    *data.underlying,
                    ctx,
                    stats_index,
                    pool,
                    total_budget_bytes,
                    tree_depth + 1,
                    required_cols,
                ),
                data.predicates,
                schema,
            ))
        }

        QueryOp::Project(data) => {
            let schema = get_output_schema(&data.underlying, ctx);
            let sources = data
                .column_name_map
                .iter()
                .map(|mapping| {
                    let (_, src) = resolve_project_mapping(mapping, &schema);
                    if let Some(&idx) = schema.get(src) {
                        crate::operators::ProjectSource::Index(idx)
                    } else {
                        let literal = if (src.starts_with('\'') && src.ends_with('\''))
                            || (src.starts_with('"') && src.ends_with('"'))
                        {
                            if src.len() >= 2 {
                                src[1..src.len() - 1].to_string()
                            } else {
                                src.clone()
                            }
                        } else {
                            src.clone()
                        };
                        crate::operators::ProjectSource::Literal(
                            crate::data::global_dict().get_or_intern(&literal),
                        )
                    }
                })
                .collect();

            Box::new(ProjectOperator::new(
                build_pipeline(
                    *data.underlying,
                    ctx,
                    stats_index,
                    pool,
                    total_budget_bytes,
                    tree_depth + 1,
                    required_cols,
                ),
                sources,
            ))
        }

        QueryOp::Sort(data) => {
            let schema = get_output_schema(&data.underlying, ctx);
            let indices = data
                .sort_specs
                .iter()
                .map(|s| (schema[&s.column_name], s.ascending))
                .collect();
            let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;
            let scratch_block_size = pool.disk_manager.block_size;

            Box::new(SortOperator::new(
                build_pipeline(
                    *data.underlying,
                    ctx,
                    stats_index,
                    pool,
                    total_budget_bytes,
                    tree_depth + 1,
                    required_cols,
                ),
                indices,
                per_op_budget,
                pool_ptr,
                scratch_block_size,
            ))
        }

        QueryOp::Cross(data) => {
            let left_est = stats_index.estimate_cardinality(&data.left);
            let right_est = stats_index.estimate_cardinality(&data.right);
            let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;

            let left = build_pipeline(
                *data.left,
                ctx,
                stats_index,
                pool,
                total_budget_bytes,
                tree_depth + 1,
                required_cols,
            );
            let right = build_pipeline(
                *data.right,
                ctx,
                stats_index,
                unsafe { &mut *pool_ptr },
                total_budget_bytes,
                tree_depth + 1,
                required_cols,
            );

            Box::new(CrossOperator::new(
                left,
                right,
                left_est <= right_est,
                pool_ptr,
                unsafe { &*pool_ptr }.disk_manager.block_size,
                per_op_budget,
            ))
        }
    }
}

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    let (disk_in, disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();
    let disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    let mut input_line = String::new();
    monitor_buf_reader.read_line(&mut input_line)?;
    let mut query: Query = serde_json::from_str(&input_line).context("JSON Parse Error")?;
    let stats_index = StatsIndex::new(&ctx);

    // Ask monitor for the configured memory limit (in MB) so we can use available RAM
    // to reduce spills/I/O while staying under RLIMIT_AS.
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    let mut mem_line = String::new();
    monitor_buf_reader.read_line(&mut mem_line)?;
    let memory_limit_mb: usize = mem_line.trim().parse().unwrap_or(64);
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;

    query.root = optimize_ast(query.root, &ctx, &stats_index);
    let mut required_cols = HashSet::new();
    collect_referenced_columns(&query.root, &mut required_cols);
    let required_col_index = RequiredColumnIndex::new(&required_cols);

    // Memory budgeting:
    // - Keep some headroom for Rust/serde/query structs, row buffers, and scratch readers.
    // - Spend the rest on operator budgets to avoid external spill/sort/join I/O.
    let reserve_bytes = (memory_limit_bytes / 5).clamp(8 * 1024 * 1024, 16 * 1024 * 1024);
    let mut pool_mem = (memory_limit_bytes / 32).clamp(MIN_BUFFER_POOL_BYTES, MAX_BUFFER_POOL_BYTES);
    if pool_mem + reserve_bytes >= memory_limit_bytes {
        pool_mem = MIN_BUFFER_POOL_BYTES;
    }
    let total_budget_bytes = memory_limit_bytes
        .saturating_sub(pool_mem + reserve_bytes)
        .max(MIN_EXECUTION_BUDGET_BYTES);
    // Hard-clamp: never promise more memory than what's actually available.
    let total_budget_bytes = total_budget_bytes.min(
        memory_limit_bytes
            .saturating_sub(pool_mem + reserve_bytes)
    );

    let disk_manager = buffer_pool::DiskManager::new(disk_buf_reader, disk_out);
    let num_frames = (pool_mem / disk_manager.block_size).max(1);
    let mut pool = buffer_pool::BufferPoolManager::new(disk_manager, num_frames);

    let mut root_operator = build_pipeline(
        query.root,
        &ctx,
        &stats_index,
        &mut pool,
        total_budget_bytes,
        0,
        &required_col_index,
    );

    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    while let Some(row) = root_operator.next() {
        let row_string = row.to_output_string();
        if monitor_out.write_all(row_string.as_bytes()).is_err() {
            return Ok(());
        }
    }

    let _ = monitor_out.write_all(b"!\n");
    let _ = monitor_out.flush();
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "Database Error")
}
