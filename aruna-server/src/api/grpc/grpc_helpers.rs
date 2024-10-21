use tonic::metadata::MetadataMap;

pub fn get_token(md: &MetadataMap) -> Option<String> {
    md.get("Authorization")
        .and_then(|v| Some(v.to_str().ok()?.split(" ").nth(1)?.to_string()))
}
