use std::env;
use std::collections::HashMap;
use std::time::Instant;

use tenacious_zebra::database::{TableTransaction, Database, Table};

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

fn run_no_backup_test(write_percentage: f64) {
    let db = Database::<String, i32>::new();
    let mut test_table = db.empty_table("test");

    let start = Instant::now();
    for i in 0..100 {
        let mut modify = TableTransaction::new();

        for j in 0..1000 {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("{}{}", i, j)).unwrap();
            }
        }
        test_table.execute(modify);
    }

    let duration = start.elapsed();

    println!("Time elapsed in run_no_backup_test() is: {:?} with write percentage: {}", duration, write_percentage);

}

fn run_file_save_backup_test(write_percentage: f64) {
    let mut db = Database::<String, i32>::new();
    db.empty_table("test");

    db.backup("./backup");

    let start: Instant = Instant::now();
    for i in 0..100 {
        db = Database::restore("./backup");
        let test_table = db.get_table("test").unwrap();
        let mut modify = TableTransaction::new();
        for j in 0..1000 {
            if with_percentage_true(write_percentage) {
                modify.set(format!("{}{}", i, j), j).unwrap();
            } else {
                modify.get(&format!("{}{}", i, j)).unwrap();
            }
        }
        test_table.execute(modify);
        db.backup("./backup");
    }

    let duration = start.elapsed();

    println!("Time elapsed in run_file_save_backup_test() is: {:?} with write percentage: {}", duration, write_percentage);
}

fn run_hash_table_operations(write_percentage: f64) {
    let mut db = HashMap::<String, i32>::new();
    db.insert(String::from("1"), 1);
    let start: Instant = Instant::now();
    for i in 0..100 {
        for j in 0..1000 {
            if with_percentage_true(write_percentage) {
                db.insert(format!("{}{}", i, j), j);
            } else {
                db.get(&"1".to_string());
            }
        }
    }
    let duration = start.elapsed();

    println!("Time elapsed in run_hash_table_operations() is: {:?} with write percentage: {}", duration, write_percentage);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let test_method = get_test_method(&args);
    let backup_type = get_backup_type(&args);


    run_no_backup_test(50.0);
    run_no_backup_test(100.0);
    run_file_save_backup_test(50.0);
    run_file_save_backup_test(100.0);
    run_hash_table_operations(50.0);
    run_hash_table_operations(100.0);
    

    println!("Hello, world!");
}
