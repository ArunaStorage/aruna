use diesel_ulid::DieselUlid;

pub struct Cache {}

impl Cache {
    pub fn new() -> Self {
        Cache {}
    }

    pub fn get_pid(&self, did: &DieselUlid) -> &DieselUlid {
        todo!()
    }
}
