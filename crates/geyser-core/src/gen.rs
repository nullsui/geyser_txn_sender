#[allow(clippy::all)]
#[allow(warnings)]
pub mod sui_rpc_v2 {
    include!("gen/sui.rpc.v2.rs");
}

pub use sui_rpc_v2::*;
