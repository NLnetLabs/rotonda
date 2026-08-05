use std::{collections::BTreeMap, sync::Arc};

use log::{debug, warn};
use routecore::{
    bgp::message_ng::common::SessionConfig,
    bmp::message_ng::common::{
        PerPeerHeader, PerPeerHeaderV3, PerPeerHeaderV4, PphFlags,
    },
};

use crate::ingress::{self, IngressId};

//#[derive(Default)]
pub struct PphRegister {
    // based on peer type byte
    // currently, types [0..3] (inclusive) are defined
    // so we create an array of length 4
    //partitions_types: [BTreeMap<Vec<u8>, (IngressId, PduParseInfo)>; 4],
    per_peer_type: [RibViewRegister; 4], // FIXME 256? or how to prevent indexing out of bounds
    ingress_register: Arc<ingress::Register>,
}

#[derive(Debug, Default)]
pub struct RibViewRegister {
    // based on peer flags byte
    // there flags are defined per peer type
    // currently, the largest space is the peer type 0-2 one,
    // with types [0..5] (inclusive) defined although 4 is deprecated.
    // Anyway, we use an array of 5.
    // TODO instead of keying on the full PPH, we want to key on the PPH-type-flags ?
    // FIXME make 256 as well?
    per_rib_view: [BTreeMap<
        [u8; std::mem::size_of::<PerPeerHeaderV3>() - 2 - 8], // NB: we
        // need to key
        // on an array
        // that is the
        // same size for
        // both V3 and
        // V4. Maybe
        // define a type
        // for that, and
        // add methods
        // to the
        // PerPeerHeader
        // trait to
        // return that.
        (IngressId, SessionConfig),
    >; 5],
}

impl PphRegister {
    pub fn new(ingress_register: Arc<ingress::Register>) -> Self {
        Self {
            ingress_register,
            per_peer_type: Default::default(),
        }
    }

    // TODO how to do V3 vs V4 ?
    pub fn get(
        &self,
        //pph: &PerPeerHeaderV3,
        pph: &impl PerPeerHeader,
    ) -> Option<&(IngressId, SessionConfig)> {
        //dbg!(&self.per_peer_type);
        //eprintln!("looking in partition 0x{:x} , 0x{:x}", u8::from(pph.peer_type), pph.flags);

        //eprintln!("PphRegister.getting partition 0x{:x} , 0x{:x}\n{:?}",
        //    u8::from(pph.peer_type), pph.flags,
        //    HexFormatted(&pph.as_bytes())
        //    );
        let map = &self.per_peer_type[u8::from(pph.peer_type()) as usize]
            .per_rib_view[pph.flags().reverse_bits() as usize];
        //eprintln!("entries: {}", map.len());
        map.get(pph.without_type_and_flags())
    }

    pub fn find_other_ribviews(
        &self,
        //pph: &PerPeerHeaderV3,
        pph: &impl PerPeerHeader,
    ) -> Option<&(IngressId, SessionConfig)> {
        //eprintln!("in find_other_ribviews to find\n{:?}", HexFormatted(pph.without_type_and_flags()));
        for peer_type in &self.per_peer_type {
            for rib_view in &peer_type.per_rib_view {
                //eprintln!("looking ribview with {} entries", rib_view.len());
                if let Some(res) = rib_view.get(pph.without_type_and_flags()) {
                    return Some(res);
                }
            }
        }
        None
    }

    pub fn insert(
        &mut self,
        pph: &impl PerPeerHeader,
        session_config: SessionConfig,
    ) -> IngressId {
        // Check whether we already registered a peer_id in the ingress::Register, by going over
        // our own cache.     ■■ change this to: `pph`
        //let peer_id = if let Some((ingress_id, _)) =
        //    self.find_other_ribviews(&pph)
        //{
        //    //eprintln!("found peer_id for this peer: {}", ingress_id.peer_id());
        //    //ingress_id.peer_id()
        //    ingress_id
        //} else {
        //    // TODO this should be a call to ingress::Register
        //    self.ingress_register.register()
        //};

        // XXX shortcut for now, until we settle on what
        // peer_id/ingress_id/mui actually looks like, and update the
        // ingress::Register code accordingly
        let peer_id = self.ingress_register.register();

        // TODO move this into the ingress::Register, that is responsible and authoritative for
        // this kind of logic
        //let mui = u32::from(peer_id) << 16
        //    | u32::from(u8::from(pph.peer_type)) << 8
        //    | u32::from(pph.flags);
        let mui = peer_id;

        //eprintln!(
        //    "inserting into partition 0x{:x} , 0x{:x}\n{:?}",
        //    u8::from(pph.peer_type),
        //    pph.flags,
        //    HexFormatted(pph.without_type_and_flags())
        //);

        if let Some((mui, _sc)) = self.per_peer_type
            [u8::from(pph.peer_type()) as usize]
            .per_rib_view[pph.flags().reverse_bits() as usize]
            .insert(
                pph.without_type_and_flags().try_into().unwrap(),
                (mui, session_config),
            )
        {
            warn!(
                "inserting already existing PPH into PphRegister, mui {mui}"
            );
        }
        mui
    }
}
