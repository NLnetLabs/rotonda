use axum::extract::{Path, State};

use crate::{http_ng::ApiState, ingress::IngressId};

// XXX The actual querying of the store should be similar to how we query the ingress register,
// i.e. with the Rib unit constructing one type of responses (so a wrapper around the
// Vec<PrefixRecord> or some such) with impls for ToJson and ToCli)
pub async fn ipv4unicast_for_ingress(
    Path((ingress_id)): Path<(IngressId)>,
    state: State<ApiState>
) -> String {
    let s = state.store.clone();
    let res: String = match s.get().map(|store|
        store.match_ingress_id(ingress_id)
    ) {
        Some(Ok(records)) => {
            //takes 1.5s, but that's because the store does a full scan
            records.into_iter().map(|r|
                format!("{}\t{} (x {})", r.meta[0].status, r.prefix, r.meta.len())
            ).collect::<Vec<_>>().join("\n")
        }
        None | Some(Err(_)) => "empty or error".into()
    };
    res.into()
}
