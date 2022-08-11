use std::env;
use std::process::Command;

pub fn up_database() {
    Command::new("tests/sources/startup.sh")
        .output()
        .expect("failed to execute startup");
}

pub fn load_sql() {
    Command::new("tests/sources/load.sh")
        .output()
        .expect("failed to execute load_db");
}

#[test]
#[ignore]
fn init_db() {
    // Only run docker commands if the test is run locally
    match env::var("GITHUB_ACTIONS") {
        Ok(_) => (),
        Err(_) => up_database(),
    };

    load_sql();
}
