use std::{net::SocketAddr, sync::Arc};

use axum::{extract::{Path, State}, routing::get, response::IntoResponse};

use crate::{cli::CliApi, ingress::{self, JsonFormat}};



pub struct Api {
    interfaces: Vec<SocketAddr>,
    ingress_register: Arc<ingress::Register>,
    router: axum::Router<ApiState>,
}

#[derive(Clone)]
pub struct ApiState {
    //store: Arc<Rib>,
    ingress_register: Arc<ingress::Register>,
    // roto::Compiled: < >
}



impl Api {

    pub fn new(
        interfaces: Vec<SocketAddr>,
        ingress_register: Arc<ingress::Register>,
    ) -> Self {
        let state = ApiState { ingress_register: ingress_register.clone() };

        let router = axum::Router::<ApiState>::new()
            .route("/", get(|| async {"new HTTP api"}))
            .with_state(state)
            ;

        let mut res = Self {
            interfaces,
            ingress_register,
            router
        };

        res.ingress_register_routes();
        res
    }

    pub fn set_interfaces(&mut self, interfaces: Vec<SocketAddr>) {
        self.interfaces = interfaces;
    }


    pub fn add_path(&mut self)  {
        self.router = self.router.clone().route("/new", get(|| async { "jeej new"}));
    }

    pub fn start(&self) {
        for interface in self.interfaces.clone() {
            let app = self.router.clone().with_state(
                ApiState{ingress_register: self.ingress_register.clone()}
            );
            tokio::spawn(async move {
                let listener = tokio::net::TcpListener::bind(interface).await.unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        }
    }

    fn ingress_register_routes(&mut self) {
        self.router = self.router.clone()
            .route("/bgp/neighbors", get(IngressApi::bgp_neighbors))
            // TODO: /bmp/routers is not (yet) part of our Yang model.
            .route("/bmp/routers", get(IngressApi::bmp_routers))
        ;
    }
}

struct IngressApi { }

//tmp
struct IngressResponse { }

impl IngressApi {
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
