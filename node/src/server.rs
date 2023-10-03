use std::{env, process::Child, sync::Mutex};
use dotenv::dotenv;
use rocket::serde::{Deserialize, json::Json};
use std::process::Command;
use rocket::State;

// fn main() {
//     dotenv().ok();
//     println!("Hello, world!");
//     let ignore_case = env::var("NODE_NAME").unwrap();
//     println!("NODE_NAME: {}", ignore_case)

// }


#[macro_use] extern crate rocket;

#[derive(Deserialize)]
enum TestProgramm {
    SingleClientSingleServer,
}

struct ServerState {
    database_runner_path: String,
    child: Mutex<Option<Child>>,
}


#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/start")] 
fn start(state: &State<ServerState>) -> &'static str {
    *state.child.lock().unwrap() = Some(Command::new(state.database_runner_path.clone())
    .spawn()
    .expect("failed to execute child"));

    "server started"
}

#[get("/stop")] 
fn stop(state: &State<ServerState>) -> &'static str {
    if let Some(child) = state.child.lock().unwrap().as_mut() {
        child.kill().expect("failed to kill child");
    }
    "server stopped"
}


#[launch]
fn rocket() -> _ {
    dotenv().ok();

    rocket::build()
    .manage(ServerState {
        database_runner_path: env::var("DATABASE_RUNNER_PATH").unwrap(),
        child: Mutex::new(None),
    })
    .mount("/", routes![index, start, stop])
}
