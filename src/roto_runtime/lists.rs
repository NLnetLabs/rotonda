use std::{borrow::Cow, str::FromStr, sync::{Arc, Mutex}};

use inetnum::{addr::Prefix, asn::Asn};
use log::warn;
use smallvec::SmallVec;

//------------ AsnLists -------------------------------------------------------

const ASN_LIST_COUNT: usize = 64;
const ASN_LIST_SIZE: usize = 8;

pub type MutNamedAsnLists = Arc<Mutex<NamedAsnLists>>;

#[derive(Clone, Debug, Default)]
pub struct NamedAsnLists {
    pub inner: micromap::Map<Arc<str>, AsnList, ASN_LIST_COUNT>
}

impl NamedAsnLists {
    pub fn add(&mut self, name: Arc<str>, asn_list: AsnList) {
        if self.inner.checked_insert(name.clone(), asn_list).is_none() {
            warn!(
                "maximum number of ASN lists defined ({ASN_LIST_COUNT}), \
                not registering '{name}'
            ");
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AsnList {
    asns: SmallVec<[Asn; ASN_LIST_SIZE]>
}

impl AsnList {
    pub fn contains(&self, asn: Asn) -> bool {
        self.asns.contains(&asn)
    }
    pub fn new(asns: SmallVec<[Asn; ASN_LIST_SIZE]>) -> Self {
        Self { asns }
    }
}

impl From<Vec<Asn>> for AsnList {
    fn from(value: Vec<Asn>) -> Self {
        Self { asns: value.into() }
    }
}

impl FromStr for AsnList {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = s.split_whitespace().flat_map(|s|
            Asn::from_str(s)
            .inspect_err(|e| warn!("failed to parse {s} as ASN: {e}"))
        ).collect::<SmallVec<_>>();
        Ok(AsnList::new(res))
    }
}

//------------ PrefixLists ---------------------------------------------------

const PREFIX_LIST_COUNT: usize = 64;
const PREFIX_LIST_SIZE: usize = 8;

pub type MutNamedPrefixLists = Arc<Mutex<NamedPrefixLists>>;

#[derive(Clone, Debug, Default)]
pub struct NamedPrefixLists {
    pub inner: micromap::Map<Arc<str>, PrefixList, PREFIX_LIST_COUNT>
}

impl NamedPrefixLists {
    pub fn add(&mut self, name: Arc<str>, list: PrefixList) {
        if self.inner.checked_insert(name.clone(), list).is_none() {
            warn!(
                "maximum number of prefix lists defined ({PREFIX_LIST_COUNT}), \
                not registering '{name}'
            ");
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PrefixList {
    prefixes: SmallVec<[Prefix; PREFIX_LIST_SIZE]>
}

impl PrefixList {
    pub fn new(prefixes: SmallVec<[Prefix; PREFIX_LIST_SIZE]>) -> Self {
        Self { prefixes }
    }
    pub fn contains(&self, prefix: Prefix) -> bool {
        self.prefixes.contains(&prefix)
    }
    pub fn covers(&self, prefix: Prefix) -> bool {
        self.prefixes.iter().any(|&p| p.covers(prefix))
    }
}

impl FromStr for PrefixList {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = s.split_whitespace().flat_map(|s|
            Prefix::from_str(s)
            .inspect_err(|e| warn!("failed to parse {s} as prefix: {e}"))
        ).collect::<SmallVec<_>>();
        Ok(PrefixList::new(res))
    }
}
