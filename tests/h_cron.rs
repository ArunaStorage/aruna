use aruna_server::database;
use aruna_server::database::cron::{Scheduler, Task};
use serial_test::serial;
use std::sync::Arc;

#[test]
#[ignore]
#[serial(db)]
fn task_sched_test() {
    let db = Arc::new(database::connection::Database::new(
        "postgres://root:test123@localhost:26257/test",
    ));

    let mut test_task = Task::new(|_| println!(), "testtask", 1, db);

    test_task.tick();

    let mut sched = Scheduler::default();

    sched.add(test_task);
}
