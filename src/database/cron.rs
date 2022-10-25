use tokio::time::{sleep, Duration};

struct Task {
    name: String,
    intervall: usize,
    func: fn(),
    ticked: usize,
}

impl Task {
    fn tick(&mut self) {
        self.ticked += 1;
        if self.ticked >= self.intervall {
            (self.func)();
            self.ticked = 0;
            log::info!("[CRON] {} ticked", self.name);
        }
    }

    fn new(func: fn(), name: &str, intervall: usize) -> Self {
        Task {
            name: name.to_string(),
            intervall,
            func,
            ticked: 0,
        }
    }
}

struct Scheduler {
    tasklist: Vec<Task>,
}

impl Scheduler {
    async fn run(&mut self) {
        loop {
            sleep(Duration::from_millis(1000)).await;

            for t in self.tasklist.iter_mut() {
                t.tick();
            }
        }
    }

    fn new() -> Self {
        Scheduler {
            tasklist: Vec::new(),
        }
    }

    fn add(&mut self, t: Task) {
        self.tasklist.push(t)
    }
}
