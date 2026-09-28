# Out-of-Core Query Execution Engine

![Language](https://img.shields.io/badge/Language-Rust_2021-orange?logo=rust)
![Course](https://img.shields.io/badge/Course-COL7362_DBMS-blue)
![Institution](https://img.shields.io/badge/Institution-IIT_Delhi-red)
![Memory Limit](https://img.shields.io/badge/Memory_Limit-%E2%89%A4_64_MB_(RLIMIT__AS)-brightgreen)
![Execution](https://img.shields.io/badge/Execution-Single--Threaded_%7C_Out--of--Core-purple)

> **A high-performance, single-threaded, out-of-core relational query execution engine written in Rust that processes multi-gigabyte datasets (TPC-H) under a strict $\le 64\text{ MB}$ virtual address space limit (`RLIMIT_AS`), featuring a Cost- & Rule-Based AST Optimizer, Deserialization-Level Projection Pushdown, Adaptive Hash/Sort-Merge Joins, External $K$-Way Merge Sort, and a 2Q Buffer Pool with Sparse Scratch Block Recycling.**

---

## Overview & Problem Statement

Modern database management systems must process datasets that far exceed available RAM while minimizing slow disk I/O and CPU overhead. Built for **COL7362 (Database Management Systems) at IIT Delhi**, this project implements the core `database` execution engine inside a three-process testbed (`Monitor`, `Database`, and `Disk Simulator`).

Our engine ingests a JSON-serialized relational query plan (`Scan`, `Filter`, `Project`, `Sort`, `Cross`), optimizes the Abstract Syntax Tree (AST) using table/column statistics, and streams pipe-delimited result tuples while respecting strict kernel-enforced `rlimit` sandboxing.

---

## System Architecture & IPC Protocol

The execution environment consists of three isolated processes communicating exclusively over pre-mapped POSIX File Descriptors (`FD 3` through `FD 6`):

```text
 ┌──────────────────────────────────────────────────────┐
 │                       Monitor                        │
 │  (spawns processes, sends queries, validates output) │
 └──────────────┬───────────────────────┬───────────────┘
                │ FD 5 (query JSON in)  │ FD 6 (results / commands out)
                ▼                       ▲
 ┌──────────────────────────────────────────────────────┐
 │              Database Engine (This Repo)             │
 │  AST Optimizer ──► Volcano Pipeline ──► 2Q Pool      │
 └──────────────┬───────────────────────┬───────────────┘
                │ FD 4 (commands out)   │ FD 3 (raw block data in)
                ▼                       ▲
 ┌──────────────────────────────────────────────────────┐
 │                    Disk Simulator                    │
 │  (block storage, tracks I/O patterns for scoring)    │
 └──────────────────────────────────────────────────────┘
```

### File Descriptor Mapping (`database/src/io_setup.rs`)

| FD | Direction | Constant | Purpose |
| :--- | :--- | :--- | :--- |
| **`3`** | `Read` | `DISK_INPUT_FD` | Receive block metadata and raw block bytes from the Disk Simulator |
| **`4`** | `Write` | `DISK_OUPUT_FD` | Send `get` and `put` block commands to the Disk Simulator |
| **`5`** | `Read` | `MONITOR_INPUT_FD` | Receive the single-line JSON-encoded `Query` AST from the Monitor |
| **`6`** | `Write` | `MONITOR_OUPUT_FD` | Stream `validate\n`, formatted result rows (`col1\|col2\|...\n`), and `!\n` |

---

## Hard OS Constraints & Memory Budget Allocation

The `Monitor` process enforces strict Linux resource limits (`setrlimit`) on the `database` process:

1. **Virtual Memory Limit (`RLIMIT_AS` $\ge 64\text{ MB}$):** Total virtual address space—including heap, code segments, and stack (`RLIMIT_STACK`)—cannot exceed the configured limit (minimum $64\text{ MB}$), while input tables span several gigabytes.
2. **Single-Threaded Execution (`RLIMIT_NPROC = 1`):** Spawning threads or child processes is forbidden by the kernel.
3. **Zero Native Filesystem I/O (`RLIMIT_FSIZE = 0`):** Creating or writing files on the host OS filesystem fails immediately. All external memory spilling (sorting runs, hash partitions, materialized cross chunks) must use the Disk Simulator's **Read-Write Anonymous Region** (`block_id >= anon-start-block`).

### How We Partition the $64\text{ MB}$ Address Space

To prevent Out-Of-Memory (`OOM`) crashes caused by allocator fragmentation or deep operator trees, `db_main()` enforces a conservative, depth-aware memory budget:

$$
\text{Per-Operator Budget} = \max\left(1\text{ MB},\; \left\lfloor \frac{32\text{ MB}}{\text{Tree Depth} + 1} \right\rfloor\right)
$$

* **Global Operator Memory Budget:** $32\text{ MB}$ (`total_budget_bytes`), dynamically divided across pipeline depth (`tree_depth + 1`).
* **Sort Safe Spill Threshold:** $\frac{1}{2} \times \text{Per-Operator Budget}$ (`safe_spill_threshold`), leaving headroom for `ScratchRunWriter` buffers ($128 \times 4\text{ KB} = 512\text{ KB}$) and Rust `Vec` reallocation doubling.
* **2Q Buffer Pool Allocation:** $12\text{ MB}$ (`3,072` frames $\times 4,096\text{ B}$).
* **Adaptive Scan Prefetch Window:** $\text{clamp}\left(\left\lfloor \frac{2048}{\text{Tree Depth} + 1} \right\rfloor, 128, 2048\right)$ blocks ($512\text{ KB}$ to $8\text{ MB}$ sequential read-ahead).

---

## Key Engine Optimizations & Architecture

```text
                       ┌──────────────────────────┐
                       │  Raw JSON Query (FD 5)   │
                       └────────────┬─────────────┘
                                    ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                 1. AST Optimizer (optimize_ast)                     │
  │  • Predicate Pushdown (through Sort, Project, Cross)                │
  │  • Predicate Short-Circuiting (Int/Float checks before Strings)     │
  │  • Projection Pushdown & Column Aliasing Resolution                 │
  │  • Cost-Based Multi-Way Cross Reordering (Left-Deep Greedy Tree)    │
  └─────────────────────────────────┬───────────────────────────────────┘
                                    ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │            2. Pipeline Builder (build_pipeline + AQE)               │
  │  • Dead-Column Pruning (collect_referenced_columns)                 │
  │  • Equi-Join Detection: Filter(A.x = B.y) + Cross ──► AdaptiveJoin  │
  └─────────────────────────────────┬───────────────────────────────────┘
                                    ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │               3. Out-of-Core Volcano Execution Engine               │
  │  • ScanOperator: Multi-block prefetch + Zero-alloc string skipping  │
  │  • AdaptiveJoinOperator: In-Memory FNV-1a Hash Join                 │
  │                          └──► Fallback: External Sort-Merge Join    │
  │  • SortOperator: External K-Way Merge Sort (Fan-in = 32)            │
  │  • CrossOperator: Cardinality-guided RAM / Spilled Block-Nested Loop│
  └─────────────────────────────────┬───────────────────────────────────┘
                                    ▼
                       ┌──────────────────────────┐
                       │  Stream Results (FD 6)   │
                       └──────────────────────────┘
```

### 1. Rule-Based & Cost-Based Query Optimizer (`main.rs`)

Before instantiating physical operators, `optimize_ast()` transforms the query tree through multiple optimization passes:

* **Predicate Short-Circuiting:** Within every `Filter` node, predicates are sorted so fast 32/64-bit integer and floating-point comparisons execute first, short-circuiting before expensive `String` comparisons:
  ```rust
  filter_data.predicates.sort_by_key(|p| {
      if matches!(p.value, ComparisionValue::String(_)) { 1 } else { 0 }
  });
  ```
* **Predicate Pushdown:**
  * Pushes `Filter` nodes beneath `Sort` nodes so fewer rows are sorted and spilled.
  * Pushes `Filter` nodes beneath `Project` nodes by translating output alias names back to source column names (`resolve_project_mapping`).
  * Splits conjunctive predicates above a `Cross` node into `left_p` (pushed into the left child), `right_p` (pushed into the right child), and `keep_p` (bridge/join predicates retained above the join).
* **Projection Pushdown:** Pushes `Project` nodes down through `Sort`, `Filter`, and `Cross` branches, retaining only the columns required by upstream operators plus any columns referenced in sort specifications or filter predicates.
* **Statistical Cardinality Estimation & Multi-Way Join Reordering (`reorder_cross_branches`):**
  * Uses `CardinalityStat` and `RangeStat` from `db_config.json` to compute predicate selectivities:
    * **Equality (`EQ`):** $\text{sel} = \text{clamp}\left(\frac{1}{\max(1, \text{Cardinality})}, 10^{-6}, 1.0\right)$ (default $0.1$).
    * **Inequality (`NE`):** $\text{sel} = 1.0 - \text{sel}_{EQ}$ (default $0.9$).
    * **Range (`GT`, `GTE`, `LT`, `LTE`):** Linear interpolation over $[\text{min}, \text{max}]$ clamped to $[0.01, 1.0]$ (default $0.33$).
  * Flattens $N$-way `Cross` trees and greedily constructs a **left-deep tree**, starting with the lowest-cardinality branch and iteratively selecting the smallest remaining branch connected by a join predicate (`predicate_connects_schemas`).

### 2. Deserialization-Level Projection Pushdown & Zero-Copy Strings (`data.rs`)

In disk-bound workloads like TPC-H, tables contain wide variable-length `String` columns (such as `c_comment` or `o_comment`) that are often unused in the final `SELECT` or `WHERE` clauses.

* **Zero-Allocation String Skipping:** `collect_referenced_columns()` computes the exact set of columns referenced anywhere in the optimized AST and passes their indices (`required_indices`) to `ScanOperator`. During `deserialize_block()`, if a column is not required:
  * Fixed-width numbers (`Int32`, `Int64`, `Float32`, `Float64`) advance the byte offset and insert a dummy `Value::Int32(0)` to preserve schema index alignment.
  * Variable-length `String` columns scan to the `0x00` null terminator and **skip UTF-8 validation and heap allocation entirely**.
* **Reference-Counted Strings (`Arc<str>`):** Required strings are stored as `Value::String(Arc<str>)`. Cloning a `Row` or `Value` during hash joins, sort-merge joins, or projections performs an $O(1)$ pointer copy rather than duplicating string bytes on the heap.

### 3. Adaptive Query Execution (AQE) Join Engine (`operators/join.rs`)

When `build_pipeline()` encounters a `Filter` with an equality join predicate (`colA = colB`) atop a `Cross` node, it replaces the pair with `AdaptiveJoinOperator`:

1. **Runtime Memory Probing:** The operator begins pulling rows from the right child (`right_child`), tracking exact byte usage via `estimate_row_size()`.
2. **Path A — In-Memory Hash Join (`InMemoryHashJoinOperator`):**
   * If the right child fits within `sort_memory_limit`, the engine immediately builds an in-memory hash table (`FastMap<Value, Vec<usize>>`) using a custom inline **64-bit FNV-1a Hasher** (`Fnv1aHasher`) and streams the left child in $O(N + M)$ time with **zero disk writes**.
3. **Path B — Seamless Fallback to External Sort-Merge Join (`SortMergeJoinOperator`):**
   * If `current_memory > max_memory` is breached mid-stream, the operator **does not restart the right child scan**. Instead, it wraps the already-buffered rows and the remaining right child stream into a zero-cost `BufferedOperator`.
   * Both the left child and the reconstructed `BufferedOperator` are piped into external `SortOperator`s sorted on the join key, which then feed `SortMergeJoinOperator`.
   * This guarantees **infinite scalability** under tight memory limits while avoiding any duplicate table scans.

### 4. External $K$-Way Merge Sort (`operators/sort.rs`)

`SortOperator` seamlessly handles both in-memory and out-of-core sorting:

* **In-Memory Fast Path:** If the input stream finishes before reaching `safe_spill_threshold`, rows are sorted in place and yielded directly from RAM without touching the disk simulator.
* **Run Generation & Multi-Block Spilling:** When memory reaches `safe_spill_threshold`, the current batch is sorted according to `sort_indices: Rc<[(usize, bool)]>` and flushed to the anonymous disk region via `ScratchRunWriter` (using $128$-block contiguous writes) before freeing the vector (`shrink_to_fit()`).
* **Multi-Pass $K$-Way Merge (`fan_in = 32`):**
  * If the number of spilled runs exceeds $32$, `collapse_runs()` merges batches of $32$ runs into consolidated runs.
  * Final streaming uses a binary min-heap (`BinaryHeap<HeapEntry>`) over active `ScratchRunReader`s. Using `heap.peek_mut()`, the top element is replaced in-place with the next row from the same run, avoiding unnecessary heap pop/push reallocations.

### 5. Out-of-Core Block Nested Loop Cross Operator (`operators/join.rs`)

For pure Cartesian products or non-equi joins (`CrossOperator`):

* **Cardinality-Guided Materialization:** Uses optimizer cardinality estimates (`left_est <= right_est`) to materialize the smaller relation.
* **Automatic Spill-to-Disk:** If the materialized relation exceeds $12\text{ MB}$, it spills to an anonymous `ScratchRun`. The streaming child is then read in $12\text{ MB}$ chunks (`stream_chunk`), scanning the spilled run once per chunk to minimize simulated disk seek and transfer time.

### 6. 2Q Buffer Pool & Sparse Anonymous Block Recycling (`buffer_pool.rs`, `operators/io_utils.rs`)

* **2Q Cache Replacement Policy:** Manages page frames using three queues:
  * `a1in`: FIFO probation queue for newly faulted pages.
  * `a1out`: Ghost FIFO history (`kout = num_frames / 2`) tracking block IDs recently evicted from `a1in`.
  * `am`: Main LRU queue for hot pages re-accessed while present in `a1out`.
* **Contiguous Multi-Block Read-Ahead:** `ScratchRunReader::ensure_block_loaded()` inspects upcoming block IDs in a spilled run and coalesces up to **`64` contiguous blocks** ($256\text{ KB}$) into a single `get block <start> <count>` request, drastically cutting IPC overhead and simulated disk seek cost.
* **Automatic Scratch Block Recycling (`Drop`):** Because the Disk Simulator lazily allocates $4\text{ KB}$ of host RAM for every unique anonymous block ID ever written, monotonically increasing block IDs would crash the simulator on large queries. Our `DiskManager` maintains a `free_blocks` stack, and `ScratchRunReader` (with `free_on_drop = true`) and `CrossOperator` implement `Drop` to return anonymous block IDs to `free_blocks` as soon as a run is consumed.

---

## Repository & Module Structure

All core implementation resides inside the `database/src/` crate:

```text
database/src/
├── main.rs                 # Entry point, AST optimizer, cardinality estimator & pipeline builder
├── cli.rs                  # Clap CLI argument parser (--config <path>)
├── io_setup.rs             # RawFD (3, 4, 5, 6) reader/writer setup for Disk & Monitor IPC
├── buffer_pool.rs          # DiskManager protocol client & 2Q BufferPoolManager
├── data.rs                 # Value (Arc<str>), Row binary codec & projection-pushed block deserializer
└── operators/
    ├── mod.rs              # Volcano Operator trait definition & module exports
    ├── relational.rs       # ScanOperator (prefetch + zone maps), FilterOperator & ProjectOperator
    ├── join.rs             # AdaptiveJoin, InMemoryHashJoin, SortMergeJoin, BufferedOp & CrossOp
    ├── sort.rs             # External K-Way Merge SortOperator (fan-in = 32)
    └── io_utils.rs         # Fnv1aHasher, BloomFilter, ScratchRunWriter/Reader & SharedBufferManager
```

---

## Disk & Data Format Specifications

### Block Layout (`.bin` Table Files)
Every table is stored in the Read-Only region (`0` to `anon-start-block - 1`) as a contiguous sequence of `block_size` blocks (default $4,096\text{ bytes}$):

```text
byte 0                                                       byte block_size - 1
┌──────────────────────────────────────────────────────────────────────────────┐
│ <Row 1><Row 2>...<Row N> [unused padding bytes] <row_count: u16 LE (2 bytes)>│
└──────────────────────────────────────────────────────────────────────────────┘
```

* **Usable Payload:** `block_size - 2` bytes; rows are packed contiguously starting at byte `0` and never span across two blocks.
* **Row Count Trailer:** The last $2\text{ bytes}$ of every block store `row_count` as an unsigned 16-bit little-endian integer (`u16`).

### On-Disk Row Encoding vs. Scratch Run Encoding

| Data Type | Table Block Encoding (`.bin`) | Scratch Run Binary Encoding (`Row::encode`) |
| :--- | :--- | :--- |
| **`Int32`** | $4\text{ B}$ signed little-endian | `0x01` tag ($1\text{ B}$) + $4\text{ B}$ signed LE |
| **`Int64`** | $8\text{ B}$ signed little-endian | `0x02` tag ($1\text{ B}$) + $8\text{ B}$ signed LE |
| **`Float32`** | $4\text{ B}$ IEEE-754 little-endian | `0x03` tag ($1\text{ B}$) + $4\text{ B}$ IEEE-754 LE |
| **`Float64`** | $8\text{ B}$ IEEE-754 little-endian | `0x04` tag ($1\text{ B}$) + $8\text{ B}$ IEEE-754 LE |
| **`String`** | UTF-8 bytes + `0x00` null byte | `0x05` tag ($1\text{ B}$) + `u32` length ($4\text{ B}$ LE) + UTF-8 bytes |

---

## Getting Started: Build, Dataset Generation & Execution

### Prerequisites
* **OS:** Linux, macOS, or Windows with WSL2
* **Toolchain:** Rust (`cargo`, `rustc` stable) and `sqlite3` CLI tool

### Step 1: Build All Binaries in Release Mode
From the workspace root:
```bash
cargo build -r
# Or build only the database executable after making changes:
cargo build -r --bin database
```
Compiled binaries are placed in `target/release/` (`database`, `disk`, `monitor`, `generator`, `demo_query_printer`).

### Step 2: Import & Compile the TPC-H Dataset
Extract `tpch_scratch.tar.gz` in the repository root to create `scratch/datasets/tpch/`, then compile the CSVs into `.bin` block files, an equivalent `sqlite.db`, and runtime JSON configs:
```bash
tar -xf tpch_scratch.tar.gz

cargo run -r --bin generator -- all \
    -d scratch/datasets/tpch \
    -c scratch/compiled_datasets/tpch \
    -r scratch/runtimes/tpch \
    -b target/release \
    -s 4096
```

### Step 3: Generate Expected Output with SQLite
Remember to append an empty string column `''` in SQLite so every output line ends with a trailing pipe `|`:
```sql
-- my_query.sql
SELECT a1 AS id, b2, '' FROM A JOIN B ON A.a1 = B.b1 WHERE b3 > 0 ORDER BY a2 ASC, b2 DESC;
```
```bash
sqlite3 scratch/compiled_datasets/tpch/sqlite.db < my_query.sql > scratch/runtimes/tpch/expected_1.csv
```

### Step 4: Run the Monitor & Validate Queries
```bash
cargo run -r --bin monitor -- --config scratch/runtimes/tpch/monitor_config.json
```

## AI Usage Declaration

In the interest of full academic and engineering transparency, generative AI tools were utilized strictly as a coding assistant to help translate implementation details into idiomatic Rust syntax, resolve borrow-checker constraints, and structure boilerplate code. 

**All core database systems engineering, architectural design, memory budgeting strategies, and algorithmic logic solely belong to the author.** This includes, but is not limited to:
* The **Rule-Based & Cost-Based AST Optimizer** (predicate pushdown, string predicate short-circuiting, projection pushdown, and greedy left-deep join reordering).
* **Deserialization-Level Projection Pushdown** and zero-allocation string skipping (`Arc<str>`).
* The **Adaptive Query Execution (AQE) Join Engine** (runtime memory probing and zero-restart fallback from In-Memory FNV-1a Hash Join to External Sort-Merge Join via `BufferedOperator`).
* The **Out-of-Core $K$-Way External Merge Sort** and **Cardinality-Guided Block Nested Loop Cross Operator**.
* The **2Q Buffer Pool Manager**, multi-block contiguous read-ahead coalescing, and `Drop`-based sparse anonymous block recycling.
