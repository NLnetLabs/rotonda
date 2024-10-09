use hyper::{Body, Response};

use crate::{payload::{RotondaPaMap, RotondaRoute}, units::rib_unit::http::PrefixesApi};

impl PrefixesApi {
    pub fn mk_dump_response(
        //res: &rotonda_store::QueryResult<RibValue>,
        //res: &rotonda_store::QueryResult<RotondaRoute>,
        res: &rotonda_store::QueryResult<RotondaPaMap>,
    ) -> Response<Body> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .body(format!("{:#?}", res).into())
            .unwrap()
    }
}
