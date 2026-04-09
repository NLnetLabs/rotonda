use std::{borrow::Cow, str::FromStr, sync::{Arc, Mutex}};

use inetnum::{addr::Prefix, asn::Asn};
use log::warn;
use smallvec::SmallVec;

//------------ AsnLists -------------------------------------------------------

const ASN_LIST_COUNT: usize = 64;
const ASN_LIST_SIZE: usize = 8;

#[derive(Clone, Debug, Default)]
pub struct MutNamedAsnLists(Arc<Mutex<NamedAsnLists>>);

impl MutNamedAsnLists {
    pub fn add(&self, name: impl AsRef<str>, asn_list: AsnList) {
        self.0.lock().unwrap().add(name, asn_list);
    }

    pub fn contains(&self, name: impl AsRef<str>, asn: Asn) -> bool {
        self.0.lock().unwrap().inner.get(name.as_ref())
            .is_some_and(|list| list.contains(asn))
    }
}

#[derive(Clone, Debug, Default)]
pub struct NamedAsnLists {
    pub inner: micromap::Map<String, AsnList, ASN_LIST_COUNT>
}

impl PartialEq for MutNamedAsnLists {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check this
        false
    }
}

impl NamedAsnLists {
    pub fn add(&mut self, name: impl AsRef<str>, asn_list: AsnList) {
        if self.inner.checked_insert(name.as_ref().to_string(), asn_list).is_none() {
            warn!(
                "maximum number of ASN lists defined ({ASN_LIST_COUNT}), \
                not registering '{}'
            ", name.as_ref());
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

#[derive(Clone, Debug, Default)]
pub struct MutNamedPrefixLists(Arc<Mutex<NamedPrefixLists>>);

impl PartialEq for MutNamedPrefixLists {
    fn eq(&self, _other: &Self) -> bool {
        // XXX double check this
        false
    }
}

impl MutNamedPrefixLists {
    pub fn add(&self, name: impl AsRef<str>, prefix_list: PrefixList) {
        self.0.lock().unwrap().add(name, prefix_list);
    }

    pub fn contains(&self, name: impl AsRef<str>, prefix: Prefix) -> bool {
        self.0.lock().unwrap().inner.get(name.as_ref())
            .is_some_and(|list| list.contains(prefix))
    }

    pub fn covers(&self, name: impl AsRef<str>, prefix: Prefix) -> bool {
        self.0.lock().unwrap().inner.get(name.as_ref())
            .is_some_and(|list| list.covers(prefix))
    }
}

#[derive(Clone, Debug, Default)]
pub struct NamedPrefixLists {
    pub inner: micromap::Map<String, PrefixList, PREFIX_LIST_COUNT>
}

impl NamedPrefixLists {
    pub fn add(&mut self, name: impl AsRef<str>, list: PrefixList) {
        if self.inner.checked_insert(name.as_ref().to_string(), list).is_none() {
            warn!(
                "maximum number of prefix lists defined ({PREFIX_LIST_COUNT}), \
                not registering '{}'
            ",
            name.as_ref()
            );
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
