use common::query::{ComparisionOperator, ComparisionValue, MultiProjectBuilder, QueryOp};

fn main() {
    // SELECT o_orderkey, o_totalprice FROM orders WHERE o_totalprice > 100000.00;

    let query = QueryOp::scan("orders")
        .filter(
            "o_totalprice",
            ComparisionOperator::GT,
            ComparisionValue::F64(100000.00),
        )
        .project_multiple(
            MultiProjectBuilder::new("o_orderkey", "o_orderkey")
                .add("o_totalprice", "o_totalprice"),
        )
        .build();

    let query_json = serde_json::to_string_pretty(&query).expect("Failed to serialize query");

    println!("{}", query_json);
}
