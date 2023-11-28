use std::env;
use std::collections::HashMap;
use std::time::Instant;

use tenaciouszebra_singe_rocksdb::database::{TableTransaction, Database};
use tenaciouszebra_file_store::database::{TableTransaction as FileStoreTableTransaction, Database as FileStoreDatabase};
use crate::helpers::with_percentage_true;

mod commands;
mod helpers;


fn get_test_method(args: &Vec<String>) -> String {
    for i in 0..args.len() {
        if args[i] == format!("--{}", "method") {
            return args[i+1].clone();
        }
    }
    String::from("none")
}


fn get_backup_type(args: &Vec<String>) -> String {
    for i in 0..args.len() {
        if args[i] == format!("--{}", "backup-type") {
            return args[i+1].clone();
        }
    }
    String::from("none")
}

fn write_to_csv(results: Vec<(u128, i32, usize, usize)>, file_name: &str) {
    let mut wtr = csv::Writer::from_path(file_name).unwrap();
    for (duration, write_percentage, transaction_size, transaction_count) in results {
        wtr.serialize((duration, write_percentage, transaction_size, transaction_count)).unwrap();
    }    
    wtr.flush().unwrap();
}

fn run_single_rocksdb_test(write_percentage: i32, transaction_size: usize, transaction_count: usize) -> u128 {
    let path = "test";
    let db = Database::<String, usize>::new(path);
    db.empty_table("test");

    let mut transactions = Vec::<TableTransaction<String, usize>>::new();
    for i in 0..transaction_count {
        let mut modify = TableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("{}{}", i, j)).unwrap();
            }
        }
        transactions.push(modify);
    }
    let test_table = db.get_table("test").unwrap();

    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();

    std::fs::remove_dir_all(path).unwrap();

    println!("Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}", duration, write_percentage, transaction_size, transaction_count);
    duration.as_millis()
}


fn run_no_backup_test(write_percentage: i32, transaction_size: usize, transaction_count: usize) -> u128 {
    let db= FileStoreDatabase::<String, usize>::new();
    let mut test_table = db.empty_table("test");

    let start = Instant::now();
    let mut transactions = Vec::<FileStoreTableTransaction<String, usize>>::new();

    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("{}{}", i, j)).unwrap();
            }
        }
        transactions.push(modify);
    }
    let test_table = db.get_table("test").unwrap();

    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();

    println!("Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}", duration, write_percentage, transaction_size, transaction_count);
    duration.as_millis()
}


fn run_file_backup_test(write_percentage: i32, transaction_size: usize, transaction_count: usize) -> u128 {
    let mut db= FileStoreDatabase::<String, usize>::new();
    db.empty_table("test");
    db.backup("./backup");

    let start: Instant = Instant::now();

    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("{}{}", i, j)).unwrap();
            }
        }
        db = FileStoreDatabase::restore("./backup");
        let test_table: std::sync::Arc<tenaciouszebra_file_store::database::Table<String, usize>> = db.get_table("test").unwrap();
        test_table.execute(modify);
        db.backup("./backup");
    }

    let duration = start.elapsed();

    std::fs::remove_dir_all("./backup").unwrap();
    println!("Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}", duration, write_percentage, transaction_size, transaction_count);
    duration.as_millis()
}

fn run_write_percentage_single_rocksdb() {
    println!("Running run_write_percentage_single_rocksdb with dynamic write percentage");

    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for write_percentage in (0..100).step_by(10)  {
            results.push((run_single_rocksdb_test(write_percentage, 10000, 100), 
            write_percentage,
            10000,
            100
        ));
    }

    write_to_csv(results, "run_write_percentage_single_rocksdb.csv");
}

fn run_write_percentage_no_backup() {
    println!("Running run_write_percentage_no_backup with dynamic write percentage");

    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for write_percentage in (0..100).step_by(10)  {
            results.push((run_no_backup_test(write_percentage, 10000, 100), 
            write_percentage,
            10000,
            100
        ));
    }

    write_to_csv(results, "run_write_percentage_no_backup.csv");
}

fn run_write_percentage_with_file_backup() {
    println!("Running run_write_percentage_with_file_backup with dynamic write percentage");

    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for write_percentage in (0..100).step_by(10)  {
            results.push((run_file_backup_test(write_percentage, 10000, 100), 
            write_percentage,
            10000,
            100
        ));
    }

    write_to_csv(results, "run_write_percentage_file_backup.csv");
}

fn run_transaction_size_single_rocksdb() {
    println!("Running run_write_percentage_single_rocksdb with dynamic transaction size");

    const NUMBER_OF_OPERATIONS_POWER: u32 = 7;

    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for i in 2..NUMBER_OF_OPERATIONS_POWER  {
            results.push((
                run_single_rocksdb_test(10, usize::pow(10,i), 
                usize::pow(10, NUMBER_OF_OPERATIONS_POWER - i)), i as i32,
                usize::pow(10,i),
                usize::pow(10, NUMBER_OF_OPERATIONS_POWER - i)
            )
            );
    }

    write_to_csv(results, "run_transaction_size_single_rocksdb.csv");
}


fn main() {
    let args: Vec<String> = env::args().collect();

    let test_method = get_test_method(&args);
    let backup_type = get_backup_type(&args);



    run_write_percentage_single_rocksdb();
    run_transaction_size_single_rocksdb();
    run_write_percentage_no_backup();
    run_write_percentage_with_file_backup();
    
}
