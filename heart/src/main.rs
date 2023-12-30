use std::{time::Instant, sync::mpsc::Sender};
use helpers::{create_percentage_test, create_transaction_size_test, CPUStatsCommand};

use tenaciouszebra_dashmap::database::{
    Database as DashMapDatabase, TableTransaction as DashMapTableTransaction,
};
use tenaciouszebra_file_store::database::{
    Database as FileStoreDatabase, TableTransaction as FileStoreTableTransaction,
};
use tenaciouszebra_singe_rocksdb::database::{Database, TableTransaction};

use tenaciouszebra_okaywal::database::{Database as OkayWalDatabase, TableTransaction as OkayWalTableTransaction};

use crate::helpers::with_percentage_true;

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


fn run_single_rocksdb_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
    tx: &Sender<CPUStatsCommand>,
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

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

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
    tx: &Sender<CPUStatsCommand>,
) -> u128 {
    let path = "test";
    let db = rocksdb::DB::open_default(path).unwrap();

    db.put("first", "1").unwrap();

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for i in 0..transaction_count {
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                db.put(format!("{}{}", i, j), format!("{}", j)).unwrap();
            } else {
                db.get("first").unwrap();
            }
        }
    }

    let duration: std::time::Duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

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
    tx: &Sender<CPUStatsCommand>,
) -> u128 {
    let db = FileStoreDatabase::<String, usize>::new();
    let test_table: std::sync::Arc<tenaciouszebra_file_store::database::Table<String, usize>> = db.empty_table("test");

    let mut first_transaction = FileStoreTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<FileStoreTableTransaction<String, usize>>::new();
    let mut get_count = 0;
    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        modify.set(String::from("first"), i).unwrap();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("first {}", get_count)).unwrap();
                get_count += 1;
            }
        }
        transactions.push(modify);
    }

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

    println!(
        "Time elapsed  {:?}, write percentage: {}, transaction_size {}, transaction_count {}",
        duration, write_percentage, transaction_size, transaction_count
    );
    duration.as_millis()
}


fn run_okaywal_test(
    write_percentage: i32,
    transaction_size: usize,
    transaction_count: usize,
    tx: &Sender<CPUStatsCommand>,
) -> u128 {
    let path = "test";
    let db = OkayWalDatabase::<String, usize>::new(path);
    let test_table = db.empty_table("test");

    let mut first_transaction = OkayWalTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<OkayWalTableTransaction<String, usize>>::new();

    let mut get_count = 0;

    for i in 0..transaction_count {
        let mut modify = OkayWalTableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("first {}", get_count)).unwrap();
                get_count += 1;
            }
        }
        transactions.push(modify);
    }

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

    std::fs::remove_dir_all(path).unwrap();

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
    tx: &Sender<CPUStatsCommand>,
) -> u128 {
    let db = DashMapDatabase::<String, usize>::new();
    let mut test_table = db.empty_table();

    let mut first_transaction = DashMapTableTransaction::new();
    for i in 0..transaction_size {
        first_transaction.set(format!("first {}", i), i).unwrap();
    }
    test_table.execute(first_transaction);

    let mut transactions = Vec::<DashMapTableTransaction<String, usize>>::new();
    let mut get_count = 0;
    for i in 0..transaction_count {
        let mut modify = DashMapTableTransaction::new();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("first {}", get_count)).unwrap();
                get_count += 1;
            }
        }
        transactions.push(modify);
    }

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for transaction in transactions {
        test_table.execute(transaction);
    }

    let duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

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
    tx: &Sender<CPUStatsCommand>,
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

    let mut get_counter = 0;

    tx.send(CPUStatsCommand::Start).unwrap();
    let start: Instant = Instant::now();

    for i in 0..transaction_count {
        let mut modify = FileStoreTableTransaction::new();
        modify.set(String::from("first"), i).unwrap();
        for j in 0..transaction_size {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("first {}", get_counter)).unwrap();
                get_counter += 1;
            }
        }
        db = FileStoreDatabase::restore("./backup");

        test_table.execute(modify);
        db.backup("./backup");
    }

    let duration = start.elapsed();
    tx.send(CPUStatsCommand::Stop).unwrap();

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

    std::fs::create_dir_all("results").unwrap();


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
