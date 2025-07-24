pub mod config;
pub mod controller;
pub mod proxy;
pub mod grpc {
    tonic::include_proto!("vkcp");
}
