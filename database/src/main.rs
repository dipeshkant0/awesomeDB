use anyhow::{Context, Result};
use clap::Parser;
use common::query::{Query, QueryOp}; 
use db_config::DbContext;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};

mod cli;
mod io_setup;
mod buffer_pool;
mod data;
mod operators;


use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
    operators::{Operator, ScanOperator, FilterOperator, ProjectOperator, SortOperator, CrossOperator, HashJoinOperator},
};

fn resolve_project_mapping<'a>(
    entry: &'a (String, String),
    _child_schema: &HashMap<String, usize>,
) -> (&'a String, &'a String) {
    (&entry.1, &entry.0)
}

// Tracks the actual column layout as it changes through Projections and Cross Joins
fn get_output_schema(op_node: &QueryOp, ctx: &DbContext) -> HashMap<String, usize> {

    match op_node {
        QueryOp::Scan(data) => {
            let mut map = HashMap::new();
            let spec = ctx.get_table_specs().iter().find(|t| t.name == data.table_id).unwrap();
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
                let (out_name, _src_name) = resolve_project_mapping(mapping, &child_schema);
                map.insert(out_name.clone(), i); // Projection redefines the indices!
            }
            map
        }
        QueryOp::Cross(data) => {
            let mut left_map = get_output_schema(&data.left, ctx);
            let right_map = get_output_schema(&data.right, ctx);
            let offset = left_map.len();
            // Append right columns to left schema, shifted by the left's size
            for (k, v) in right_map {
                left_map.insert(k, v + offset);
            }
            left_map
        }
    }
}

// Helper to get base table size from JSON stats
fn get_base_cardinality(table_name: &str, ctx: &DbContext) -> f64 {
    use db_config::statistics::ColumnStat;
    let spec = ctx.get_table_specs().iter().find(|t| t.name == table_name);
    if let Some(table) = spec {
        let mut max_cardinality: Option<f64> = None;
        for col in &table.column_specs {
            if let Some(ref stats) = col.stats {
                for stat in stats {
                    if let ColumnStat::CardinalityStat(val) = stat {
                        max_cardinality = Some(max_cardinality.unwrap_or(0.0).max(val.0 as f64));
                    }
                }
            }
        }
        if let Some(cardinality) = max_cardinality {
            return cardinality;
        }
    }
    1000.0
}

// Helper to cast `Data` from statistics to f64 for math
fn stat_data_as_f64(data: &common::Data) -> Option<f64> {
    match data {
        common::Data::Int32(v) => Some(*v as f64),
        common::Data::Int64(v) => Some(*v as f64),
        common::Data::Float32(v) => Some(*v as f64),
        common::Data::Float64(v) => Some(*v),
        common::Data::String(_) => None,
    }
}

// Helper to cast `ComparisionValue` from queries to f64
fn comp_val_as_f64(val: &common::query::ComparisionValue) -> Option<f64> {
    match val {
        common::query::ComparisionValue::I32(v) => Some(*v as f64),
        common::query::ComparisionValue::I64(v) => Some(*v as f64),
        common::query::ComparisionValue::F32(v) => Some(*v as f64),
        common::query::ComparisionValue::F64(v) => Some(*v),
        _ => None,
    }
}

// Dynamically calculates the selectivity fraction (0.0 to 1.0) of a predicate
fn calculate_selectivity(predicate: &common::query::Predicate, ctx: &DbContext) -> f64 {
    use db_config::statistics::ColumnStat;
    use common::query::ComparisionOperator;

    // Locate the column's statistics in the catalog
    let mut column_stats = None;
    for table in ctx.get_table_specs() {
        if let Some(col) = table.column_specs.iter().find(|c| c.column_name == predicate.column_name) {
            column_stats = col.stats.as_ref();
            break;
        }
    }

    let default_eq = 0.1;
    let default_range = 0.33;

    let stats = match column_stats {
        Some(s) => s,
        None => return match predicate.operator { ComparisionOperator::EQ => default_eq, _ => default_range },
    };

    match predicate.operator {
        ComparisionOperator::EQ => {
            for stat in stats {
                if let ColumnStat::CardinalityStat(cardinality) = stat {
                    return (1.0 / cardinality.0.max(1) as f64).clamp(0.000_001, 1.0);
                }
            }
            for stat in stats {
                if let ColumnStat::DensityStat(density) = stat {
                    return (1.0 - density.0 as f64).clamp(0.000_001, 1.0);
                }
            }
            default_eq
        }
        ComparisionOperator::NE => {
            for stat in stats {
                if let ColumnStat::CardinalityStat(cardinality) = stat {
                    return (1.0 - (1.0 / cardinality.0.max(1) as f64)).clamp(0.01, 1.0);
                }
            }
            for stat in stats {
                if let ColumnStat::DensityStat(density) = stat {
                    return density.0 as f64;
                }
            }
            0.9
        }
        ComparisionOperator::GT | ComparisionOperator::GTE | ComparisionOperator::LT | ComparisionOperator::LTE => {
            // For Range queries we need min and max to estimate the fraction of the data that satisfies the predicate
            let mut min_val = None;
            let mut max_val = None;
            
            for stat in stats {
                if let ColumnStat::RangeStat(range) = stat {
                    min_val = stat_data_as_f64(&range.lower_bound);
                    max_val = stat_data_as_f64(&range.upper_bound);
                    break;
                }
            }

            if let (Some(min), Some(max), Some(val)) = (min_val, max_val, comp_val_as_f64(&predicate.value)) {
                if max <= min { return default_range; } // Avoid division by zero
                
                let fraction = match predicate.operator {
                    ComparisionOperator::GT | ComparisionOperator::GTE => (max - val) / (max - min),
                    ComparisionOperator::LT | ComparisionOperator::LTE => (val - min) / (max - min),
                    _ => unreachable!(),
                };
                fraction.clamp(0.01, 1.0)
            } else {
                default_range
            }
        }
    }
}

fn estimate_cardinality(op: &QueryOp, ctx: &DbContext) -> f64 {
    match op {
        QueryOp::Scan(data) => get_base_cardinality(&data.table_id, ctx),
        QueryOp::Filter(data) => {
            let child_est = estimate_cardinality(&data.underlying, ctx);
            
            // Multiply selectivity of all predicates together (assuming they are independent)
            let mut combined_selectivity = 1.0;
            for p in &data.predicates {
                combined_selectivity *= calculate_selectivity(p, ctx);
            }
            
            child_est * combined_selectivity
        }
        QueryOp::Cross(data) => estimate_cardinality(&data.left, ctx) * estimate_cardinality(&data.right, ctx),
        QueryOp::Project(data) => estimate_cardinality(&data.underlying, ctx),
        QueryOp::Sort(data) => estimate_cardinality(&data.underlying, ctx),
    }
}

fn estimate_value_width_bytes(column_name: &str, ctx: &DbContext) -> usize {
    let base_name = column_name.rsplit('.').next().unwrap_or(column_name);
    for table in ctx.get_table_specs() {
        if let Some(col) = table.column_specs.iter().find(|c| c.column_name == base_name) {
            return match col.data_type {
                common::DataType::Int32 | common::DataType::Float32 => 8,
                common::DataType::Int64 | common::DataType::Float64 => 16,
                common::DataType::String => 48,
            };
        }
    }
    24
}

fn estimate_row_width_bytes(op: &QueryOp, ctx: &DbContext) -> usize {
    match op {
        QueryOp::Scan(data) => {
            ctx.get_table_specs()
                .iter()
                .find(|t| t.name == data.table_id)
                .map(|t| {
                    t.column_specs
                        .iter()
                        .map(|c| estimate_value_width_bytes(&c.column_name, ctx))
                        .sum::<usize>()
                })
                .unwrap_or(64)
        }
        QueryOp::Filter(data) => estimate_row_width_bytes(&data.underlying, ctx),
        QueryOp::Sort(data) => estimate_row_width_bytes(&data.underlying, ctx),
        QueryOp::Project(data) => data
            .column_name_map
            .iter()
            .map(|(_, src)| estimate_value_width_bytes(src, ctx))
            .sum(),
        QueryOp::Cross(data) => {
            estimate_row_width_bytes(&data.left, ctx) + estimate_row_width_bytes(&data.right, ctx)
        }
    }
}

fn estimated_bytes_for_op(op: &QueryOp, ctx: &DbContext) -> usize {
    let rows = estimate_cardinality(op, ctx).max(1.0);
    let width = estimate_row_width_bytes(op, ctx).max(1);
    let bytes = rows * width as f64 * 2.0;
    bytes.min(usize::MAX as f64) as usize
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
    predicates.iter().any(|pred| matches!(pred.value, common::query::ComparisionValue::Column(_)))
}

fn reorder_cross_branches(
    cross_data: common::query::CrossData,
    predicates: &[common::query::Predicate],
    ctx: &DbContext,
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
            let est = estimate_cardinality(&branch, ctx);
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

    while !remaining.is_empty() {
        let next_idx = remaining
            .iter()
            .enumerate()
            .filter(|(_, (_, schema, _))| {
                predicates
                    .iter()
                    .any(|pred| predicate_connects_schemas(pred, &current_schema, schema))
            })
            .min_by(|(_, a), (_, b)| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(idx, _)| idx)
            .or_else(|| {
                remaining
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(idx, _)| idx)
            })
            .unwrap();

        let (branch, schema, _) = remaining.remove(next_idx);
        let offset = current_schema.len();
        for (k, v) in schema {
            current_schema.insert(k, v + offset);
        }
        ordered.push(branch);
    }

    if let QueryOp::Cross(data) = build_left_deep_cross(ordered) {
        data
    } else {
        unreachable!()
    }
}

// possibly optimize the AST by pushing down filters and projections, and reordering cross joins based on predicate connectivity and estimated size
fn optimize_ast(op_node: QueryOp, ctx: &DbContext) -> QueryOp {
    match op_node {
        QueryOp::Scan(_) => op_node,

        QueryOp::Filter(mut filter_data) => {
            let optimized_child = optimize_ast(*filter_data.underlying, ctx);

            match optimized_child {
                // Filter Pushdown
                QueryOp::Sort(mut sort_data) => {
                    filter_data.underlying = sort_data.underlying;
                    sort_data.underlying = Box::new(optimize_ast(QueryOp::Filter(filter_data), ctx));
                    QueryOp::Sort(sort_data)
                }
                //Filter Pushdown (Through Projection)
                QueryOp::Project(mut proj_data) => {
                    let child_schema = get_output_schema(&proj_data.underlying, ctx);
                    for pred in &mut filter_data.predicates {
                        for mapping in &proj_data.column_name_map {
                            let (out_name, src_name) = resolve_project_mapping(mapping, &child_schema);
                            if pred.column_name == *out_name {
                                pred.column_name = src_name.clone();
                                break;
                            }
                        }
                        if let common::query::ComparisionValue::Column(ref mut rhs_col) = pred.value {
                            for mapping in &proj_data.column_name_map {
                                let (out_name, src_name) = resolve_project_mapping(mapping, &child_schema);
                                if *rhs_col == *out_name {
                                    *rhs_col = src_name.clone();
                                    break;
                                }
                            }
                        }
                    }
                    filter_data.underlying = proj_data.underlying;
                    proj_data.underlying = Box::new(optimize_ast(QueryOp::Filter(filter_data), ctx));
                    QueryOp::Project(proj_data)
                }
                // Filter Pushdown (Through Cross Join) with Dynamic Reordering
                QueryOp::Cross(cross_data) => {
                    let mut cross_data = reorder_cross_branches(cross_data, &filter_data.predicates, ctx);
                    let left_schema = get_output_schema(&cross_data.left, ctx);
                    let right_schema = get_output_schema(&cross_data.right, ctx);
                    let mut left_p = Vec::new();
                    let mut right_p = Vec::new();
                    let mut keep_p = Vec::new();

                    for p in filter_data.predicates {
                
                        // Determine if the predicate can be fully evaluated in the Left branch, the Right branch, or if it must be kept above the Cross Join
                        let mut all_in_left = left_schema.contains_key(&p.column_name);
                        let mut all_in_right = right_schema.contains_key(&p.column_name);

                        // Check RHS columns in predicates to determine if they also fit entirely in one branch
                        if let common::query::ComparisionValue::Column(ref rhs_col) = p.value {
                            all_in_left = all_in_left && left_schema.contains_key(rhs_col);
                            all_in_right = all_in_right && right_schema.contains_key(rhs_col);
                        }

                        // Push down only if both columns exist in that branch
                        if all_in_left {
                            left_p.push(p);
                        } else if all_in_right {
                            right_p.push(p);
                        } else {
                            keep_p.push(p); 
                        }
                    }

                    if !left_p.is_empty() {
                        cross_data.left = Box::new(optimize_ast(QueryOp::Filter(common::query::FilterData {
                            predicates: left_p, underlying: cross_data.left,
                        }), ctx));
                    }
                    if !right_p.is_empty() {
                        cross_data.right = Box::new(optimize_ast(QueryOp::Filter(common::query::FilterData {
                            predicates: right_p, underlying: cross_data.right,
                        }), ctx));
                    }

                    if keep_p.is_empty() { 
                        QueryOp::Cross(cross_data) 
                    } else {
                        filter_data.predicates = keep_p;
                        filter_data.underlying = Box::new(QueryOp::Cross(cross_data));
                        QueryOp::Filter(filter_data)
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
            let optimized_child = optimize_ast(*proj_data.underlying, ctx);
            match optimized_child {
                //Intermediate Projection below Sort 
                QueryOp::Sort(mut sort_data) => {
                    let mut new_map = Vec::new();
                    let mut added = std::collections::HashSet::new();
                    
                    for mapping in &proj_data.column_name_map {
                        let (_, src) = resolve_project_mapping(mapping, &proj_child_schema);
                        if added.insert(src.clone()) { new_map.push((src.clone(), src.clone())); } 
                    }
                    for s in &sort_data.sort_specs { 
                        if added.insert(s.column_name.clone()) { new_map.push((s.column_name.clone(), s.column_name.clone())); } 
                    }
                    
                    sort_data.underlying = Box::new(optimize_ast(QueryOp::Project(common::query::ProjectData {
                        column_name_map: new_map, underlying: sort_data.underlying,
                    }), ctx));
                    
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(QueryOp::Sort(sort_data)),
                    })
                }

                //Intermediate projection below filter
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
                                    if l_added.insert(src.clone()) { left_map.push((src.clone(), src.clone())); }
                                } else if right_schema.contains_key(src) {
                                    if r_added.insert(src.clone()) { right_map.push((src.clone(), src.clone())); }
                                }
                            }

                            for pred in &filter_data.predicates {
                                if left_schema.contains_key(&pred.column_name) {
                                    if l_added.insert(pred.column_name.clone()) {
                                        left_map.push((pred.column_name.clone(), pred.column_name.clone()));
                                    }
                                } else if right_schema.contains_key(&pred.column_name) {
                                    if r_added.insert(pred.column_name.clone()) {
                                        right_map.push((pred.column_name.clone(), pred.column_name.clone()));
                                    }
                                }

                                if let common::query::ComparisionValue::Column(ref rhs) = pred.value {
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

                            cross_data.left = Box::new(optimize_ast(QueryOp::Project(common::query::ProjectData {
                                column_name_map: left_map,
                                underlying: cross_data.left,
                            }), ctx));
                            cross_data.right = Box::new(optimize_ast(QueryOp::Project(common::query::ProjectData {
                                column_name_map: right_map,
                                underlying: cross_data.right,
                            }), ctx));

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
                        if added.insert(src.clone()) { new_map.push((src.clone(), src.clone())); }
                    }
                    // Join keys are explicitly preserved here
                    for pred in &filter_data.predicates {
                        if added.insert(pred.column_name.clone()) { new_map.push((pred.column_name.clone(), pred.column_name.clone())); }
                        if let common::query::ComparisionValue::Column(ref rhs) = pred.value {
                            if added.insert(rhs.clone()) { new_map.push((rhs.clone(), rhs.clone())); }
                        }
                    }
                    
                    let pushed_project = QueryOp::Project(common::query::ProjectData {
                        column_name_map: new_map, underlying: filter_data.underlying,
                    });
                    
                    let optimized_pushed_project = optimize_ast(pushed_project, ctx);
                    let new_filter = QueryOp::Filter(common::query::FilterData {
                        predicates: filter_data.predicates,
                        underlying: Box::new(optimized_pushed_project),
                    });
                    
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(new_filter),
                    })
                }

                // If it's a cross join, we can push projections down both sides to minimize data early on
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
                            if l_added.insert(src.clone()) { left_map.push((src.clone(), src.clone())); }
                        } else if right_schema.contains_key(src) {
                            if r_added.insert(src.clone()) { right_map.push((src.clone(), src.clone())); }
                        }
                    }
                    
                    cross_data.left = Box::new(optimize_ast(QueryOp::Project(common::query::ProjectData {
                        column_name_map: left_map, underlying: cross_data.left,
                    }), ctx));
                    
                    cross_data.right = Box::new(optimize_ast(QueryOp::Project(common::query::ProjectData {
                        column_name_map: right_map, underlying: cross_data.right,
                    }), ctx));
                    
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(QueryOp::Cross(cross_data)),
                    })
                }

                other => {
                    QueryOp::Project(common::query::ProjectData {
                        column_name_map: proj_data.column_name_map,
                        underlying: Box::new(other),
                    })
                }
            }
        }

        QueryOp::Sort(mut d) => { d.underlying = Box::new(optimize_ast(*d.underlying, ctx)); QueryOp::Sort(d) }
        QueryOp::Cross(mut d) => { 
            d.left = Box::new(optimize_ast(*d.left, ctx)); 
            d.right = Box::new(optimize_ast(*d.right, ctx)); 
            QueryOp::Cross(d) 
        }
    }
}


fn build_pipeline<'a, R: Read, W: Write>(
    op_node: QueryOp,
    ctx: &'a DbContext,
    pool: &'a mut buffer_pool::BufferPoolManager<R, W>,
    sort_memory_limit_bytes: usize,
) -> Box<dyn Operator + 'a> {
    match op_node {
        QueryOp::Scan(data) => {
            let spec = ctx.get_table_specs().iter().find(|t| t.name == data.table_id).expect("Table not found");
            Box::new(ScanOperator::new(&data.table_id, spec, pool))
        }
        QueryOp::Filter(mut data) => {
            if let QueryOp::Cross(cross_data) = *data.underlying {
                let left_schema = get_output_schema(&cross_data.left, ctx);
                let right_schema = get_output_schema(&cross_data.right, ctx);

                // Only intercept if the condition spans both sides.
                let mut join_pred_idx = None;
                for (i, pred) in data.predicates.iter().enumerate() {
                    if matches!(pred.operator, common::query::ComparisionOperator::EQ) {
                        if let common::query::ComparisionValue::Column(ref rhs_col) = pred.value {
                            let is_bridge = (left_schema.contains_key(&pred.column_name) && right_schema.contains_key(rhs_col)) ||
                                            (left_schema.contains_key(rhs_col) && right_schema.contains_key(&pred.column_name));

                            if is_bridge {
                                join_pred_idx = Some((i, pred.column_name.clone(), rhs_col.clone()));
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

                    let left_est = estimate_cardinality(&cross_data.left, ctx);
                    let right_est = estimate_cardinality(&cross_data.right, ctx);
                    let build_on_left = left_est < right_est;
                    let estimated_build_bytes = if build_on_left {
                        estimated_bytes_for_op(&cross_data.left, ctx)
                    } else {
                        estimated_bytes_for_op(&cross_data.right, ctx)
                    };

                    // Hash join has no spill path. Use a conservative but realistic threshold:
                    // sort_memory_limit_bytes is only the external-sort budget, not the whole process budget.
                    let hash_join_memory_budget = sort_memory_limit_bytes.saturating_mul(3);
                    if estimated_build_bytes > hash_join_memory_budget {
                        data.underlying = Box::new(QueryOp::Cross(cross_data));
                        let schema = get_output_schema(&data.underlying, ctx);
                        return Box::new(FilterOperator::new(
                            build_pipeline(*data.underlying, ctx, pool, sort_memory_limit_bytes),
                            data.predicates,
                            schema,
                        ));
                    }

                    let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;
                    let left_child = build_pipeline(*cross_data.left, ctx, pool, sort_memory_limit_bytes);
                    let right_child = build_pipeline(*cross_data.right, ctx, unsafe { &mut *pool_ptr }, sort_memory_limit_bytes);

                    let hash_join = Box::new(HashJoinOperator::new(
                        left_child,
                        right_child,
                        left_col_idx,
                        right_col_idx,
                        build_on_left,
                    ));

                    data.predicates.remove(idx);

                    if data.predicates.is_empty() {
                        return hash_join; 
                    } else {
                        let mut combined_schema = left_schema;
                        let offset = combined_schema.len();
                        for (k, v) in right_schema {
                            combined_schema.insert(k, v + offset);
                        }
                        return Box::new(FilterOperator::new(hash_join, data.predicates, combined_schema));
                    }
                }
                data.underlying = Box::new(QueryOp::Cross(cross_data));
            }

            let schema = get_output_schema(&data.underlying, ctx);
            Box::new(FilterOperator::new(build_pipeline(*data.underlying, ctx, pool, sort_memory_limit_bytes), data.predicates, schema))
        }

        QueryOp::Project(data) => {
            let schema = get_output_schema(&data.underlying, ctx);
            let indices = data
                .column_name_map
                .iter()
                .map(|mapping| {
                    let (_, src) = resolve_project_mapping(mapping, &schema);
                    schema[src]
                })
                .collect();
            Box::new(ProjectOperator::new(build_pipeline(*data.underlying, ctx, pool, sort_memory_limit_bytes), indices))
        }

        QueryOp::Sort(data) => {
            let schema = get_output_schema(&data.underlying, ctx);
            let indices = data.sort_specs.iter().map(|s| (schema[&s.column_name], s.ascending)).collect();
            let scratch_block_size = pool.disk_manager.block_size;
            let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;
            Box::new(SortOperator::new(
                build_pipeline(*data.underlying, ctx, pool, sort_memory_limit_bytes),
                indices,
                sort_memory_limit_bytes,
                pool_ptr,
                scratch_block_size,
            ))
        }
        QueryOp::Cross(data) => {
            let left_est = estimate_cardinality(&data.left, ctx);
            let right_est = estimate_cardinality(&data.right, ctx);
            let materialize_left = left_est <= right_est;
            let pool_ptr = pool as *mut buffer_pool::BufferPoolManager<R, W>;
            let left = build_pipeline(*data.left, ctx, pool, sort_memory_limit_bytes);
            let right = build_pipeline(*data.right, ctx, unsafe { &mut *pool_ptr }, sort_memory_limit_bytes);
            Box::new(CrossOperator::new(left, right, materialize_left))
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

    // Read the Query AST
    let mut input_line = String::new();
    monitor_buf_reader.read_line(&mut input_line)?;
    let mut query: Query = serde_json::from_str(&input_line).context("JSON Parse Error")?;

    query.root = optimize_ast(query.root, &ctx);

    // Read the Memory Limit
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;

    let memory_limit_mb: u32 = input_line.trim().parse().context("Memory parse error")?;
    let disk_manager = buffer_pool::DiskManager::new(disk_buf_reader, disk_out); // DiskManager will take ownership of the disk reader and writer, and handle all disk I/O through them.
    let total_mem = memory_limit_mb as u64 * 1024 * 1024; // convert MB to Bytes
    let pool_mem = (total_mem / 16).max(4 * 1024 * 1024);
    let num_frames = (pool_mem / 4096) as usize;
    let sort_memory_limit_bytes = ((total_mem / 8).max(8 * 1024 * 1024)) as usize;
    
    let mut pool = buffer_pool::BufferPoolManager::new(disk_manager, num_frames); 

    // Build the Volcano Pipeline 
    let mut root_operator = build_pipeline(query.root, &ctx, &mut pool, sort_memory_limit_bytes);

    // Send the validate command first, so the Monitor starts listening
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?; 

    // Pull rows one-by-one and send them instantly to the pipe
    while let Some(row) = root_operator.next() {
        let row_string = row.to_output_string();
        
        if monitor_out.write_all(row_string.as_bytes()).is_err() {
            return Ok(());
        }
    }
    
    // Terminate successfully
    let _ = monitor_out.write_all(b"!\n");
    let _ = monitor_out.flush();
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "Database Error")
}
