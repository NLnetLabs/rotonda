use axum::extract::State;

use crate::{cli::CliApi, http_ng::{Api, ApiState}, representation::JsonFormat};

pub struct IngressApi { }

impl IngressApi {


    //// Add ingress register specific endpoints to a HTTP API
    pub fn register_routes(router: &mut Api) {
        router.add_get("/bgp/neighbors", Self::bgp_neighbors);
        router.add_get("/bmp/routers", Self::bmp_routers);
    }


    async fn bgp_neighbors(state: State<ApiState>) -> Result<Vec<u8>, String> {

            // trigger the CLI one as well just to test it
            CliApi{ ingress_register: state.ingress_register.clone()}.bgp_neighbors();

            let mut res = Vec::new();
            state.ingress_register.bgp_neighbors(JsonFormat(&mut res));
            Ok(res.into())
        }

    async fn bmp_routers(state: State<ApiState>) -> Result<Vec<u8>, String> {

            // trigger the CLI one as well just to test it
            CliApi{ ingress_register: state.ingress_register.clone()}.bmp_routers();

            let mut res = Vec::new();
            state.ingress_register.bmp_routers(JsonFormat(&mut res));
            Ok(res.into())
    }
}
