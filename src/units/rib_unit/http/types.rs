use routecore::{
    asn::Asn,
    bgp::communities::Community,
};

use crate::payload::RoutingInformationBase;

#[derive(Debug, Default)]
pub struct Includes {
    pub less_specifics: bool,
    pub more_specifics: bool,
}

#[derive(Debug, Default)]
pub struct Details {
    pub communities: bool,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum FilterOp {
    #[default]
    Any,

    All,
}

#[derive(Debug)]
pub enum FilterKind {
    AsPath(Vec<Asn>),
    SourceAs(Asn),
    Community(Community),
    RoutingInformationBaseName(RoutingInformationBase),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FilterMode {
    Select,
    Discard,
}

#[derive(Debug)]
pub struct Filter {
    kind: FilterKind,
    mode: FilterMode,
}

impl Filter {
    pub fn new(kind: FilterKind, mode: FilterMode) -> Self {
        Self { kind, mode }
    }

    pub fn kind(&self) -> &FilterKind {
        &self.kind
    }

    pub fn mode(&self) -> FilterMode {
        self.mode
    }
}

#[derive(Debug, Default)]
pub struct Filters {
    op: FilterOp,
    selects: Vec<Filter>,
    discards: Vec<Filter>,
}

impl Filters {
    pub fn new(op: FilterOp, filters: Vec<Filter>) -> Self {
        let (selects, discards) = filters.into_iter().partition(|filter| match filter.mode() {
            FilterMode::Select => true,
            FilterMode::Discard => false,
        });
        Self {
            op,
            selects,
            discards,
        }
    }

    pub fn op(&self) -> FilterOp {
        self.op
    }

    pub fn selects(&self) -> &[Filter] {
        self.selects.as_ref()
    }

    pub fn discards(&self) -> &[Filter] {
        self.discards.as_ref()
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SortKey {
    None,

    // #[cfg(test)]
    // RouterId,
}

impl Default for SortKey {
    fn default() -> Self {
        Self::None
    }
}
