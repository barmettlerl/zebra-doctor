use std::{env, process::Child, sync::Mutex};
use dotenv::dotenv;
use rocket::serde::{Deserialize, json::Json};
use std::process::Command;
use rocket::State;

#[macro_use] extern crate rocket;

#[derive(Deserialize, Debug)]
struct StartProgramParams {
    test_mode: node::TestMode,
}

struct ServerState {
    database_runner_path: String,
    child: Mutex<Option<Child>>,
}


#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[post("/start", data = "<test_programm>")] 
fn start(test_programm: Json<StartProgramParams>, state: &State<ServerState>) -> &'static str {
    print!("Starting server with test programm: {:?}", test_programm.0);
    *state.child.lock().unwrap() = Some(Command::new(state.database_runner_path.clone())
    .args(["--test-programm", test_programm.0.test_mode.to_string().as_str()])
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
