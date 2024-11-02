fn main() {
    let file = "./tests/test_db/store/";
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(file)
            .unwrap()
    };

    env.copy_to_file(
        "./aruna-server/tests/test_db/store/store_small.mdb",
        heed::CompactionOption::Enabled,
    )
    .unwrap();
}
