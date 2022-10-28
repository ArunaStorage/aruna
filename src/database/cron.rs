use std::sync::Arc;

use tokio::time::{sleep, Duration};

use super::connection::Database;

pub struct Task {
    name: String,
    intervall: usize,
    func: fn(db: Arc<Database>),
    ticked: usize,
    db: Arc<Database>,
}

impl Task {
    pub fn tick(&mut self) {
        self.ticked += 1;
        if self.ticked >= self.intervall {
            (self.func)(self.db.clone());
            self.ticked = 0;
            log::info!("[CRON] {} ticked", self.name);
        }
    }

    pub fn new(
        func: fn(db: Arc<Database>),
        name: &str,
        intervall: usize,
        db: Arc<Database>,
    ) -> Self {
        Task {
            name: name.to_string(),
            intervall,
            func,
            ticked: 0,
            db,
        }
    }
}

pub struct Scheduler {
    tasklist: Vec<Task>,
}

impl Scheduler {
    pub async fn run(&mut self) {
        loop {
            sleep(Duration::from_millis(1000)).await;

            for t in self.tasklist.iter_mut() {
                t.tick();
            }
        }
    }

    pub fn new() -> Self {
        Scheduler {
            tasklist: Vec::new(),
        }
    }

    pub fn add(&mut self, t: Task) {
        self.tasklist.push(t)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
