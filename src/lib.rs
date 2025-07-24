pub mod config;
pub mod controller;
pub mod proxy;
mod state_machine;
pub mod grpc {
    tonic::include_proto!("vkcp");
}
