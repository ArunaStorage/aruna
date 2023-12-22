use postgres_types::ToSql;

pub fn create_multi_query(vec: &[&(dyn ToSql + Sync)]) -> String {
    let mut result = "(".to_string();
    for count in 1..vec.len() {
        result.push_str(format!("${count},").as_str());
    }
    let last = vec.len();
    result.push_str(format!("${last})").as_str());
    result
}
