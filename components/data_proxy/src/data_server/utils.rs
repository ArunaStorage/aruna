use aruna_rust_api::api::storage::{models::v1::DataClass, services::v1::StageObject};

pub fn construct_path(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

pub fn create_stage_object(key: &str, content_len: i64) -> StageObject {
    let (fname, sub_path) = extract_filename_path(key);
    StageObject {
        filename: fname,
        content_len: content_len,
        source: None,
        dataclass: DataClass::Private as i32,
        labels: Vec::new(),
        hooks: Vec::new(),
        sub_path: sub_path,
    }
}

pub fn extract_filename_path(path: &str) -> (String, String) {
    let mut splits: Vec<&str> = path.split('/').collect();
    (
        String::from(splits.pop().unwrap_or_else(|| "")),
        splits.join("/"),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_filename_path_test() {
        use super::*;
        let test = "";

        assert_eq!(
            extract_filename_path(test),
            ("".to_string(), "".to_string())
        );

        let test = "a/a";
        assert_eq!(
            extract_filename_path(test),
            ("a".to_string(), "a".to_string())
        );

        let test = "/a";
        assert_eq!(
            extract_filename_path(test),
            ("a".to_string(), "".to_string())
        );
    }
}
