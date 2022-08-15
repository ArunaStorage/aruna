use serial_test::serial;

use std::process::Command;

pub fn up_database() {
    let startup_output = Command::new("tests/sources/startup.sh")
        .output()
        .expect("failed to execute startup");
    println!("{:?}", startup_output);
    assert!(startup_output.status.success())
}

pub fn load_sql() {
    let load_output = Command::new("tests/sources/load.sh")
        .output()
        .expect("failed to execute load_db");

    println!("{:?}", load_output);
    assert!(load_output.status.success())
}

#[test]
#[ignore]
#[serial(db)]
fn init_db() {
    // Only run docker commands if the test is run locally
    // match env::var("GITHUB_ACTIONS") {
    //     Ok(_) => (),
    //     Err(_) => up_database(),
    // };
    up_database();
    load_sql();
}
