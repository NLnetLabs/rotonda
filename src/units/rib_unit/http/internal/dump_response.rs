use hyper::{Body, Response};

use crate::{units::rib_unit::{http::PrefixesApi, rib_value::RibValue}};

impl PrefixesApi {
    pub fn mk_dump_response(res: &rotonda_store::QueryResult<RibValue>) -> Response<Body> {
        Response::builder()
            .header("Content-Type", "text/plain")
            .body(format!("{:#?}", res).into())
            .unwrap()
    }
}
