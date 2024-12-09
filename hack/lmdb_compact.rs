fn main() {
    let file = "./01J69YGPPZY80206PGHHXBR863/events/";
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(file)
            .unwrap()
    };

    env.copy_to_file(
        "./aruna-server/tests/test_db/events/data.mdb",
        heed::CompactionOption::Enabled,
    )
    .unwrap();

    let file = "./01J69YGPPZY80206PGHHXBR863/store/";
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(file)
            .unwrap()
    };

    env.copy_to_file(
        "./aruna-server/tests/test_db/store/data.mdb",
        heed::CompactionOption::Enabled,
    )
    .unwrap();
}
