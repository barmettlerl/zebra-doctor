use std::{fmt::format, time::Instant};

use crate::helpers::with_percentage_true;
use rocksdb::{DB as RocksDB};
use tenaciouszebra_dashmap::database::{
    Database as DashMapDatabase, TableTransaction as DashMapTableTransaction,
};
use tenaciouszebra_file_store::database::{
    Database as FileStoreDatabase, TableTransaction as FileStoreTableTransaction,
};
use tenaciouszebra_singe_rocksdb::database::{Database, TableTransaction};

mod commands;
mod helpers;

fn get_test_method(args: &Vec<String>) -> String {
    for i in 0..args.len() {
        if args[i] == format!("--{}", "method") {
            return args[i + 1].clone();
        }
    }
    String::from("none")
}

fn get_backup_type(args: &Vec<String>) -> String {
    for i in 0..args.len() {
        if args[i] == format!("--{}", "backup-type") {
            return args[i + 1].clone();
        }
    }
    String::from("none")
}

fn write_to_csv(results: Vec<(u128, i32, usize, usize)>, file_name: &str) {
    std::fs::create_dir_all("results").unwrap();
    let mut wtr = csv::Writer::from_path(format!("results/{}", file_name)).unwrap();
    for (duration, write_percentage, transaction_size, transaction_count) in results {
        wtr.serialize((
            duration,
            write_percentage,
            transaction_size,
            transaction_count,
        ))
        .unwrap();
    }
    wtr.flush().unwrap();
}

fn create_percentage_test(function_under_test: fn(i32, usize, usize) -> u128, file_name: &str) {
    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for write_percentage in (0..100).step_by(10) {
        results.push((
            function_under_test(write_percentage, 10000, 100),
            write_percentage,
            10000,
            100,
        ));
    }

    write_to_csv(results, file_name);
}

fn create_transaction_size_test(
    function_under_test: fn(i32, usize, usize) -> u128,
    file_name: &str,
) {
    const NUMBER_OF_OPERATIONS_POWER: u32 = 7;

    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for i in 2..NUMBER_OF_OPERATIONS_POWER {
        results.push((
            function_under_test(
                10,
                usize::pow(10, i),
                usize::pow(10, NUMBER_OF_OPERATIONS_POWER - i),
            ),
            10,
            usize::pow(10, i),
            usize::pow(10, NUMBER_OF_OPERATIONS_POWER - i),
        ));
    }

    write_to_csv(results, file_name);
}

fn run_single_rocksdb_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
) -> u128 {
    let path = "test";
    let db = Database::<String, usize>::new(path);
    let test_table = db.empty_table("test");

    let mut first_transaction = TableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<TableTransaction<String, usize>>::new();

    let mut get_count = 0;

    for i in 0..transaction_count {
        let mut modify = TableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(format!("first {}", get_count)).unwrap();
                get_count += 1;
            }
        }
        transactions.push(modify);
    }

    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();

    std::fs::remove_dir_all(path).unwrap();

    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}

fn run_rocksdb_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
) -> u128 {
    let path = "test";

    let db = RocksDB::open_default(path).unwrap();
    db.put(b"first", b"1").unwrap();
    let start: Instant = Instant::now();

    for i in 0..transaction_count {
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                db.put(format!("{}{}", i, j), format!("{}", j)).unwrap();
            } else {
                db.get(b"first").unwrap();
            }
        }
    }

    let duration: std::time::Duration = start.elapsed();

    std::fs::remove_dir_all(path).unwrap();

    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}

fn run_no_backup_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
) -> u128 {
    let db = FileStoreDatabase::<String, usize>::new();
    let test_table: std::sync::Arc<tenaciouszebra_file_store::database::Table<String, usize>> = db.empty_table("test");

    let mut first_transaction = FileStoreTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<FileStoreTableTransaction<String, usize>>::new();

    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        modify.set(String::from("first"), i).unwrap();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&String::from("first")).unwrap();
            }
        }
        transactions.push(modify);
    }

    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();

    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}

fn run_no_backup_dashmap_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
) -> u128 {
    let db = DashMapDatabase::<String, usize>::new();
    let mut test_table = db.empty_table();

    let mut first_transaction = DashMapTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<DashMapTableTransaction<String, usize>>::new();

    for i in 0..transaction_count {
        let mut modify = DashMapTableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&String::from("first")).unwrap();
            }
        }
        transactions.push(modify);
    }

    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();

    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}

fn run_file_backup_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
) -> u128 {
    let mut db = FileStoreDatabase::<String, usize>::new();
    db.empty_table("test");
    db.backup("./backup");
    let test_table: std::sync::Arc<tenaciouszebra_file_store::database::Table<String, usize>> = db.get_table("test").unwrap();

    let mut first_transaction = FileStoreTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let start: Instant = Instant::now();

    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        modify.set(String::from("first"), i).unwrap();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&String::from("first")).unwrap();
            }
        }
        db = FileStoreDatabase::restore("./backup");

        test_table.execute(modify);
        db.backup("./backup");
    }

    let duration = start.elapsed();

    std::fs::remove_dir_all("./backup").unwrap();
    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}

fn main() {
    // let args: Vec<String> = env::args().collect();

    // let test_method = get_test_method(&args);
    // let backup_type = get_backup_type(&args);

    create_percentage_test(run_single_rocksdb_test, "write_percentage_single_rocksdb");
    create_percentage_test(run_no_backup_test, "write_percentage_no_backup");
    create_percentage_test(run_file_backup_test, "write_percentage_with_file_backup");
    create_percentage_test(
        run_no_backup_dashmap_test,
        "write_percentage_no_backup_dashmap",
    );
    create_percentage_test(run_rocksdb_test, "write_percentage_rocksdb");

    create_transaction_size_test(run_single_rocksdb_test, "transaction_size_single_rocksdb");
    create_transaction_size_test(run_no_backup_test, "transaction_size_no_backup");
    create_transaction_size_test(run_file_backup_test, "transaction_size_with_file_backup");
    create_transaction_size_test(
        run_no_backup_dashmap_test,
        "transaction_size_no_backup_dashmap",
    );
    create_transaction_size_test(run_rocksdb_test, "transaction_size_rocksdb");
}
