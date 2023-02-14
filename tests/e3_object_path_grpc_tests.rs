use serial_test::serial;
mod common;

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating an object with a specific subpath
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_object_with_path_grpc_test() {
   todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Try creating a duplicate default path for the same object
/// 3) Creating an additional path for the same object
#[ignore]
#[tokio::test]
#[serial(db)]
async fn create_additional_object_path_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get each path individually
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_path_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get all active object paths
/// 4) Get all object paths including the inactive
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_paths_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Modify visibility of some paths
#[ignore]
#[tokio::test]
#[serial(db)]
async fn set_object_path_visibility_grpc_test() {
    todo!()
}

/// The individual steps of this test function contains:
/// 1) Creating an object with the default subpath
/// 2) Creating some additional paths for the same object
/// 3) Get object via each path individually
#[ignore]
#[tokio::test]
#[serial(db)]
async fn get_object_by_path_grpc_test() {
    todo!()
}
