fn main() {
    tonic_build::configure()
        .out_dir("src/api") // you can change the generated code's location
        .compile(
            &[
                "protos/API/sciobjsdb/api/storage/models/v1/new_api_models.proto",
                "protos/API/sciobjsdb/api/storage/services/v1/collection_service.proto",
                "protos/API/sciobjsdb/api/storage/services/v1/collection_service_models.proto",
            ],
            &["protos/API"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
