use aruna_rust_api::api::storage::models::v2::{
    generic_resource, Collection, Dataset, Object, Project,
};
use tonic::Result;

pub fn type_name_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

pub trait IntoGenericInner<T> {
    fn into_inner(self) -> Result<T, tonic::Status>;
}

impl IntoGenericInner<Project> for generic_resource::Resource {
    fn into_inner(self) -> Result<Project, tonic::Status> {
        match self {
            generic_resource::Resource::Project(project) => Ok(project),
            _ => Err(tonic::Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Collection> for generic_resource::Resource {
    fn into_inner(self) -> Result<Collection> {
        match self {
            generic_resource::Resource::Collection(collection) => Ok(collection),
            _ => Err(tonic::Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Dataset> for generic_resource::Resource {
    fn into_inner(self) -> Result<Dataset> {
        match self {
            generic_resource::Resource::Dataset(dataset) => Ok(dataset),
            _ => Err(tonic::Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Object> for generic_resource::Resource {
    fn into_inner(self) -> Result<Object> {
        match self {
            generic_resource::Resource::Object(object) => Ok(object),
            _ => Err(tonic::Status::invalid_argument("Invalid conversion")),
        }
    }
}
