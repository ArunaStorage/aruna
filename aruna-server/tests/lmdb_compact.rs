




#[cfg(test)]
mod create_tests {

    #[test]
    fn test_realm() {
        let file = "./tests/test_db/store/";


        let env = unsafe {heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(file)
            .unwrap()};

        env.copy_to_file("./tests/test_db/store/store_small.mdb", heed::CompactionOption::Enabled).unwrap();

    }
}