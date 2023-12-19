use std::{thread, sync::mpsc};

use csv::StringRecordIter;
use sysinfo::{System, SystemExt, CpuExt};
use rand::Rng;


pub fn with_percentage_true(x: i32) -> bool {
    if !(0.0..=100.0).contains(&(x as f64)) {
        panic!("Percentage must be between 0 and 100");
    }

    let rand_value: f64 = rand::thread_rng().gen_range(0.0..100.0);
    rand_value < x.into()
}

fn read_and_store_cpu_stats(rx: mpsc::Receiver<()>, test_name: String) -> impl FnOnce() {
    move || {
        let mut sys = System::new();

        // create csv file to store cpu stats
        let mut wtr = csv::Writer::from_path(format!("results/{}_cpu_stats.csv", test_name)).unwrap();

        println!("length of cpus: {}", sys.cpus().len());
        sys.refresh_cpu(); // Refreshing CPU information.

        // write column for each cpu
        wtr.write_record(sys.cpus().iter().map(|x| x.name()).collect::<Vec<_>>()).unwrap();

        loop {
            match rx.try_recv() { 
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => (),
            }
            sys.refresh_cpu(); // Refreshing CPU information.
            wtr.write_record(sys.cpus().iter().map(|cpu| cpu.cpu_usage().to_string()).collect::<Vec<_>>()).unwrap();

            // Sleeping to let time for the system to run for long
            // enough to have useful information.
            std::thread::sleep(System::MINIMUM_CPU_UPDATE_INTERVAL);
        }

        wtr.flush().unwrap();
    }
}

fn write_to_csv(results: Vec<(u128, i32, usize, usize)>, file_name: &str) {
    let mut wtr = csv::Writer::from_path(format!("results/{}.csv", file_name)).unwrap();
    wtr.write_record(["duration", "write_percentage", "transaction_size", "transaction_count"]).unwrap();
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

pub fn create_percentage_test(function_under_test: fn(i32, usize, usize) -> u128, file_name: &str) {
    println!("Running percentage test with fn {}", file_name);
    let (tx, rx) = mpsc::channel();
    thread::spawn(read_and_store_cpu_stats(rx, file_name.to_string()));
    let mut results = Vec::<(u128, i32, usize, usize)>::new();
    for write_percentage in (0..100).step_by(10) {
        results.push((
            function_under_test(write_percentage, 10000, 100),
            write_percentage,
            10000,
            100,
        ));
    }
    tx.send(()).unwrap();

    write_to_csv(results, file_name);
}

pub fn create_transaction_size_test(
    function_under_test: fn(i32, usize, usize) -> u128,
    file_name: &str,
) {
    const NUMBER_OF_OPERATIONS_POWER: u32 = 7;

    println!("Running transaction size test with fn {}", file_name);
    let (tx, rx) = mpsc::channel();
    thread::spawn(read_and_store_cpu_stats(rx, file_name.to_string()));
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
    tx.send(()).unwrap();

    write_to_csv(results, file_name);
}
