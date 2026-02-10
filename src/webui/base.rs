use std::{collections::HashMap, net::IpAddr};

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
        page.bmp_tree = Self::bmp_tree(state);
        page.render()
            .map(Into::into)
            .map_err(|e| format!("rendering error: {}", e))
    }

    // create mapping from BMP to BGP sessions
    // 
    // TODO: aggregate on
    //   - peer AS
    //   - rib view
    // e.g. show
    fn bmp_tree(state: State<ApiState>) -> HashMap<IngressId, (IngressInfo, Vec<(IngressId, IngressInfo)>)> {
        let register = state.ingress_register.cloned_info();
        let mut res = HashMap::new();
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::Bmp)
        }) {
            res.insert(*id, (info.clone(), vec![]));
        }
        for (id, info) in register.iter().filter(|(_id, info)| {
            info.ingress_type == Some(crate::ingress::IngressType::BgpViaBmp)
        }) {
            let (_bmp_info, bgp) = res.get_mut(&info.parent_ingress.expect("should have parent"))
                .expect("should be in hashmap already");
            bgp.push((*id, info.clone()))
        }

        res
    }
}



#[derive(Default, RsHtml)]
pub struct Index {
    pub bmp_routers: Vec<(IngressId, IngressInfo)>,
    pub bgp_routers: Vec<(IngressId, Option<IpAddr>, Option<Asn>)>,
    pub bmp_tree: HashMap<IngressId, (IngressInfo, Vec<(IngressId, IngressInfo)>)>,
}
