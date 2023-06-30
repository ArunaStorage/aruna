use serial_test::serial;
use std::env;
use std::process::Command;

pub fn cleanup() {
    let teardown_output = Command::new("tests/sources/down.sh")
        .output()
        .expect("failed to execute process");
    println!("{teardown_output:?}");
    assert!(teardown_output.status.success())
}

#[test]
#[ignore]
#[serial(db)]
fn down_db() {
    // Only run docker commands if the test is run locally
    match env::var("GITHUB_ACTIONS") {
        Ok(_) => (),
        Err(_) => cleanup(),
    };
}
