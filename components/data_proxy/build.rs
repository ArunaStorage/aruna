fn main() {
    tonic_build::configure()
        .out_dir("src/api") // you can change the generated code's location
        .compile(
            &["ArunaAPI/aruna/api/internal/v1/proxy.proto"],
            &["ArunaAPI"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
