use hyper::{Body, Response};
use indoc::formatdoc;

use crate::{units::rib_unit::{http::PrefixesApi, unit::RibValue}};

impl PrefixesApi {
    pub fn mk_looking_glass_like_response(
        router: &str,
        res: &rotonda_store::QueryResult<RibValue>,
    ) -> Response<Body> {
        let mut lg_text = String::new();

        if let Some(prefix) = res.prefix {
            if let Some(meta) = &res.prefix_meta {
                if let Some(routes) = meta.get(router.to_string()) {
                    let n_routes = routes.len();

                    lg_text = formatdoc!(
                        "
                            Router: {router}
                            Command: n/a
                            
                            BGP routing table entry for {prefix}
                            Paths: ({n_routes} available)
                            "
                    );

                    for (n, route) in routes.iter().enumerate() {
                        todo!();
                        // let loaded_route = route.load();

                        // if let Some(advert) = &loaded_route.advert {
                        //     let path_idx = n + 1;

                        //     let as_path = match loaded_route.advert.as_ref() {
                        //         Some(advert) => advert
                        //             .as_path
                        //             .iter()
                        //             .map(|asn| format!("{}", asn.into_u32()))
                        //             .collect::<Vec<String>>()
                        //             .join(" "),
                        //         _ => String::new(),
                        //     };

                        //     use std::fmt::Write;
                        //     let mut community_text = String::new();
                        //     for community in advert.standard_communities() {
                        //         let _ = write!(community_text, "{} ", community);
                        //     }
                        //     for community in advert.ext_communities() {
                        //         let _ = write!(community_text, "{} ", community);
                        //     }
                        //     for community in advert.large_communities() {
                        //         let _ = write!(community_text, "{} ", community);
                        //     }

                        //     let received = loaded_route.received;
                        //     let rib = loaded_route.routing_information_base;

                        //     let path_text = formatdoc!(
                        //         "
                        //             Path #{path_idx}: {as_path}
                        //             Received: {received}
                        //             RIB: {rib}
                        //             Communities: {community_text}
                        //             "
                        //     );

                        //     lg_text.push_str(&path_text);
                        // }
                    }
                }
            }
        }

        Response::builder()
            .header("Content-Type", "text/plain")
            .body(Body::from(lg_text))
            .unwrap()
    }
}
