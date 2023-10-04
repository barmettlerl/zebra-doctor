use std::env;
use node::TestMode;
use rocket::serde::{Deserialize, json::Json};
use rocket::State;
use zebra::database::{Database, TableTransaction};
#[macro_use] extern crate rocket;

#[derive(Deserialize)]
struct Transaction{
    key: String,
    value: i32
}



struct RunnerState {
    db: Database<String, i32>,
    mode: TestMode,
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[post("/transaction", data = "<transaction>")]
fn transaction(transaction: Json<Transaction>, s: &State<RunnerState>) -> &'static str {
    if s.mode == TestMode::NoBackup {
        transaction_no_backup(transaction, s)
    } else {
        transaction_serialize_backup(transaction, s)
    }

    "Transaction executed"
}

fn transaction_no_backup(transaction: Json<Transaction>, s: &State<RunnerState>) {

    let table = s.db.get_table("test").unwrap();
    let mut modify = TableTransaction::new();
    modify.set(transaction.0.key, transaction.0.value).unwrap();
    table.execute(modify);
}

fn transaction_serialize_backup(transaction: Json<Transaction>, s: &State<RunnerState>) {

    let table = s.db.get_table("test").unwrap();
    let mut modify = TableTransaction::new();
    modify.set(transaction.0.key, transaction.0.value).unwrap();
    table.execute(modify);

    s.db.backup("./backup");
}

#[launch]
fn rocket() -> _ {
    let args: Vec<_> = env::args().collect();
    let test_programm = args[2].clone();
    
    let test_mode = TestMode::from_string(test_programm);
    let mut db = Database::<String, i32>::new();

    if test_mode == TestMode::SerializeBackup && std::path::Path::new("./backup").exists(){
        db = Database::<String, i32>::restore("./backup");
    }

    db.empty_table("test");
    rocket::build()
    .configure(rocket::Config::figment().merge(("port", 3000)))
    .manage(RunnerState{
        db,
        mode: test_mode
    })
    .mount("/", routes![index, transaction])
}
