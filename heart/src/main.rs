use zebra::database::{Database, TableTransaction};
use std::time::{Instant};

fn main() {
    let mut db = Database::<String, i32>::new();
    let test_table = db.empty_table("test");

    let transactions = Vec::new();

    for i in 0..1000 {
        let mut modify = TableTransaction::new();
        for j in 0..1000 {
            modify.set(format!("{}{}", i, j), j).unwrap();
        }
        transactions.push(modify);
    }

    let start = Instant::now();
    for transaction in transactions.iter() {
        test_table.execute(*transaction);
    }
    let duration = start.elapsed();

    println!("Time elapsed in expensive_function() is: {:?}", duration);

    println!("Hello, world!");
}
