use std::env;
use std::process::Command;

pub fn cleanup() {
    Command::new("tests/sources/down.sh")
        .output()
        .expect("failed to execute process");
}

#[test]
#[ignore]
fn down_db() {
    // Only run docker commands if the test is run locally
    match env::var("GITHUB_ACTIONS") {
        Ok(_) => (),
        Err(_) => cleanup(),
    };
}
