use hyper::{Body, Response};

use crate::{payload::RotondaRoute, units::rib_unit::http::PrefixesApi};

impl PrefixesApi {
    pub fn mk_dump_response(
        //res: &rotonda_store::QueryResult<RibValue>,
        res: &rotonda_store::QueryResult<RotondaRoute>,
    ) -> Response<Body> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .body(format!("{:#?}", res).into())
            .unwrap()
    }
}
