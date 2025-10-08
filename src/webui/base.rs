use std::net::IpAddr;

use axum::{extract::State, response::Html};
use inetnum::asn::Asn;
use rshtml::{RsHtml, traits::RsHtml};

use crate::{http_ng::{Api, ApiState}, ingress::{IngressId, IngressInfo}};

pub struct WebUI { }

impl WebUI {
    pub fn register_routes(router: &mut Api) {
        router.add_get("/", Self::index);
    }


    async fn index(state: State<ApiState>) -> Result<Html<String>, String> {
        let mut page = Index::default();
        let _ = state.ingress_register.bmp_routers(&mut page);
        page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }
}



#[derive(Default, RsHtml)]
pub struct Index {
    pub bmp_routers: Vec<(IngressId, IngressInfo)>,
    pub bgp_routers: Vec<(IngressId, Option<IpAddr>, Option<Asn>)>,
}
