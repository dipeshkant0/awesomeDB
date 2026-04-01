use anyhow::{Context, Result};
use clap::Parser;
use common::query::{Query, QueryOp}; 
use db_config::DbContext;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};

// Import our custom logic
mod cli;
mod io_setup;
mod buffer_pool;
mod data;
mod operators;


use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
    operators::{Operator, ScanOperator, FilterOperator, ProjectOperator},
};

// Find the table name associated with a branch of the query tree.
fn get_table_name_from_op(op: &QueryOp) -> &String {
    match op {
        QueryOp::Scan(data) => &data.table_id,
        QueryOp::Filter(data) => get_table_name_from_op(&data.underlying),
        QueryOp::Project(data) => get_table_name_from_op(&data.underlying),
        QueryOp::Sort(data) => get_table_name_from_op(&data.underlying),
        QueryOp::Cross(data) => get_table_name_from_op(&data.left),
    }
}

/// Recursively builds the Volcano operator pipeline.
/// Note: We take ownership of QueryOp (no &) so we can move data out of it.
fn build_pipeline<'a, R: Read, W: Write>(
    op_node: QueryOp,
    ctx: &'a DbContext,
    pool: &'a mut buffer_pool::BufferPoolManager<R, W>,
) -> Box<dyn Operator + 'a> {
    match op_node {
        QueryOp::Scan(data) => {
            let table_spec = ctx.get_table_specs().iter()
                .find(|t| t.name == data.table_id)
                .expect("Table not found");
            
            // Move the table_id string into the operator
            Box::new(ScanOperator::new(&data.table_id, table_spec, pool))
        }
        
        QueryOp::Filter(data) => {
            // Peek at the table name before we move 'underlying'
            let table_name = get_table_name_from_op(&data.underlying).clone();
            
            // Recursively build child by moving the boxed underlying node
            let child = build_pipeline(*data.underlying, ctx, pool);
            
            let table_spec = ctx.get_table_specs().iter()
                .find(|t| t.name == table_name).unwrap();
            
            let mut schema_map = HashMap::new();
            for (i, col) in table_spec.column_specs.iter().enumerate() {
                schema_map.insert(col.column_name.clone(), i);
            }

            // Move predicates directly into the FilterOperator
            Box::new(FilterOperator::new(child, data.predicates, schema_map))
        }
        
        QueryOp::Project(data) => {
            let table_name = get_table_name_from_op(&data.underlying).clone();
            let child = build_pipeline(*data.underlying, ctx, pool);
            
            let table_spec = ctx.get_table_specs().iter()
                .find(|t| t.name == table_name).unwrap();
            
            let mut name_to_idx = HashMap::new();
            for (i, col) in table_spec.column_specs.iter().enumerate() {
                name_to_idx.insert(col.column_name.clone(), i);
            }

            // Map the source names to indices
            let indices_to_keep: Vec<usize> = data.column_name_map.iter()
                .map(|(_out_name, src_name)| {
                    *name_to_idx.get(src_name)
                        .expect("Column not found in project")
                })
                .collect();

            Box::new(ProjectOperator::new(child, indices_to_keep))
        }
        
        QueryOp::Sort(data) => {
            // Sort is currently a pass-through
            build_pipeline(*data.underlying, ctx, pool)
        }
        
        QueryOp::Cross(data) => {
            // Join is currently a pass-through to the left child
            build_pipeline(*data.left, ctx, pool)
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
    let query: Query = serde_json::from_str(&input_line).context("JSON Parse Error")?;

    // Read the Memory Limit
    input_line.clear();
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse().context("Memory parse error")?;

   // Initialize Buffer Pool (Drop to 25% to leave plenty of RAM for the output buffer)
    let disk_manager = buffer_pool::DiskManager::new(disk_buf_reader, disk_out);
    let total_mem = memory_limit_mb as u64 * 1024 * 1024;
    let pool_mem = total_mem / 4;
    let num_frames = (pool_mem / 4096) as usize;
    
    let mut pool = buffer_pool::BufferPoolManager::new(disk_manager, num_frames);

    // Build the Volcano Pipeline 
    let mut root_operator = build_pipeline(query.root, &ctx, &mut pool);


    let mut output_buffer: Vec<u8> = Vec::new();
    
    eprintln!("=> STARTING VOLCANO PIPELINE...");

    while let Some(row) = root_operator.next() {
        output_buffer.extend_from_slice(row.to_output_string().as_bytes());
    }
    
    eprintln!("=> PIPELINE FINISHED! Generated {} bytes of output data.", output_buffer.len());
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?; 

    eprintln!("=> STREAMING DATA TO MONITOR ROW BY ROW...");
    let mut rows_sent = 0;

    for row in output_buffer.split(|&b| b == b'\n') {

        if row.is_empty() { continue; } 

        let result = monitor_out.write_all(row).and_then(|_| monitor_out.write_all(b"\n"));

        if let Err(e) = result {
            if e.kind() == std::io::ErrorKind::BrokenPipe {
                eprintln!("=> MONITOR ABORTED AT ROW {}. Check for mismatch above.", rows_sent + 1);
            }
            return Ok(());
        }

        rows_sent += 1;
    }
    
    let _ = monitor_out.write_all(b"!\n");
    let _ = monitor_out.flush();

    eprintln!("=> DATABASE EXITED SUCCESSFULLY WITH ROWS SENT: {}", rows_sent);
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "Database Error")
}