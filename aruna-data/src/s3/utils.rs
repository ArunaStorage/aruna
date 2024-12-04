// Create an increasing list of "permutated" paths
// bucket: foo key: bar/baz/bat
// Returns: ["foo/bar", "foo/bar/baz", "foo/bar/baz/bat"]
// All subpaths for a given path

pub fn permute_path(bucket: &str, path: &str) -> Vec<String> {
    let parts = path.split('/').collect::<Vec<&str>>();

    let mut current = String::from(bucket);

    let mut result = Vec::new();
    for part in parts {
        current.push_str("/");
        current.push_str(part);
        result.push(current.clone());
    }

    result
}
