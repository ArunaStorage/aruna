extern crate tonic_build;

use std::fs;

fn main() {
    let mut protos: Vec<String> = Vec::new();

    let service_entries = fs::read_dir("protos/aruna/api/storage/services/v1/").unwrap();

    for entry in service_entries {
        let dir = entry.unwrap();
        let rel_path = format!(
            "{}{}",
            "protos/aruna/api/storage/services/v1/",
            dir.file_name().to_str().unwrap()
        );
        protos.push(rel_path);
    }

    let service_entries = fs::read_dir("protos/aruna/api/notification/services/v1/").unwrap();

    for entry in service_entries {
        let dir = entry.unwrap();
        let rel_path = format!(
            "{}{}",
            "protos/aruna/api/notification/services/v1/",
            dir.file_name().to_str().unwrap()
        );
        protos.push(rel_path);
    }

    let service_entries = fs::read_dir("protos/aruna/api/internal/v1/").unwrap();

    for entry in service_entries {
        let dir = entry.unwrap();
        let rel_path = format!(
            "{}{}",
            "protos/aruna/api/internal/v1/",
            dir.file_name().to_str().unwrap()
        );
        protos.push(rel_path);
    }

    tonic_build::configure()
        .build_server(true)
        .out_dir("src/api")
        .compile(
            &protos,
            &[
                "./protos".to_string(),
                "protos/aruna/api/google".to_string(),
            ],
        )
        .unwrap();
}
