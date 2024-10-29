#![allow(dead_code)]
use octseq::{Octets, OctetsFrom, Parser};
use routecore::bgp::message::PduParseInfo;
use routecore::bgp::path_attributes::{PaMap, PathAttributes};
use routecore::bgp::types::AfiSafiType;
use routecore::{bgp::types::Afi, typeenum};
use inetnum::{addr::Prefix, asn::Asn};

use std::fmt;
use std::net::IpAddr;
use std::ops::Index;
use std::slice::SliceIndex;

use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;

//
//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                           Timestamp                           |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |             Type              |            Subtype            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                             Length                            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                      Message... (variable)
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

#[derive(Copy, Clone, Debug)]
pub struct CommonHeader<'a, Octs> {
    timestamp: u32,
    msg_type: MessageType,
    msg_subtype: MessageSubType,
    length: u32,
    message: Parser<'a, Octs>
}
impl<Octs: Octets> CommonHeader<'_, Octs> {
    pub fn length(&self) -> u32  {
        self.length
    }
    pub fn msgtype(&self) -> MessageType {
        self.msg_type
    }
    pub fn subtype(&self) -> MessageSubType {
        self.msg_subtype
    }
}

impl<'a, Octs: Octets> CommonHeader<'a, Octs> {
    pub fn parse(parser: &mut Parser<'a, Octs>) -> Result<Self, ParseError> {
        let timestamp = parser.parse_u32_be()?;
        let msg_type = parser.parse_u16_be()?.into();
        let msg_subtype = match msg_type {
            MessageType::TableDumpv2 => {
                MessageSubType::TableDumpv2SubType(
                    parser.parse_u16_be()?.into()
                )
            }
            MessageType::Bgp4Mp => {
                MessageSubType::Bgp4MpSubType(
                    parser.parse_u16_be()?.into()
                )
            }
            n => todo!("TODO parse {n}")
        };

        let length = parser.parse_u32_be()?;
        let message = parser.parse_parser(length as usize)?;

        Ok( CommonHeader {
                timestamp,
                msg_type,
                msg_subtype,
                length,
                message
        })
    }
}

//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                           Timestamp                           |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |             Type              |            Subtype            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                             Length                            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                      Microsecond Timestamp                    |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                      Message... (variable)
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
pub struct ExtendedHeader<'a, Octs> {
    timestamp: u32,
    msg_type: MessageType,
    msg_subtype: MessageSubType,
    length: u32,
    timestamp_ms: u32,
    message: Parser<'a, Octs>
}

typeenum!(MessageType, u16,
    {
    11 => Ospfv2,
    12 => TableDump,
    13 => TableDumpv2,
    16 => Bgp4Mp,
    17 => Bgp4MpEt,
    32 => Isis,
    33 => IsisEt,
    48 => Ospfv3,
    49 => Ospfv3Et,
    }
);

#[derive(Copy, Clone, Debug)]
pub enum MessageSubType {
    TableDumpv2SubType(TableDumpv2SubType),
    Bgp4MpSubType(Bgp4MpSubType),
}

typeenum!(TableDumpv2SubType, u16,
    {
    1 => PeerIndexTable,
    2 => RibIpv4Unicast,
    3 => RibIpv4Multicast,
    4 => RibIpv6Unicast,
    5 => RibIpv6Multicast,
    6 => RibGeneric,
    }
);

typeenum!(Bgp4MpSubType, u16,
    {
    0 => StateChange,
    1 => Message,
    4 => MessageAs4,
    5 => StateChangeAs4,
    6 => MessageLocal,
    7 => MessageAs4Local,
    }
);

//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                      Collector BGP ID                         |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |       View Name Length        |     View Name (variable)      |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |          Peer Count           |    Peer Entries (variable)
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

pub struct PeerIndexTable<'a, Octs> {
    collector_bgp_id: [u8; 4],
    view: Option<String>,
    peer_count: u16,
    peer_entries: Parser<'a, Octs>
}

impl<'a, Octs: Octets> PeerIndexTable<'a, Octs> {
    pub fn parse(parser: &mut Parser<'a, Octs>) -> Result<Self, ParseError> {
        let collector_bgp_id = parser.parse_u32_be()?.to_be_bytes();
        let view_len = parser.parse_u16_be()?;
        let view = if view_len > 0 {
            let mut buf = vec![0u8; view_len.into()];
            parser.parse_buf(&mut buf[..])?;
            Some(String::from_utf8_lossy(&buf).into_owned())
        } else {
            None
        };


        let peer_count = parser.parse_u16_be()?;
        let peer_entries = parser.parse_parser(parser.remaining())?;

        Ok( PeerIndexTable {
            collector_bgp_id,
            view,
            peer_count,
            peer_entries
        })

    }

    pub fn view(&self) -> Option<&String> {
        self.view.as_ref()
    }

    pub fn peer_count(&self) -> u16 {
        self.peer_count
    }

    pub fn entries(&mut self) -> Parser<'_, Octs> {
        self.peer_entries
    }
}

//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |   Peer Type   |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                         Peer BGP ID                           |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                   Peer IP Address (variable)                  |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                        Peer AS (variable)                     |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct PeerEntry {
    pub bgp_id: [u8; 4],
    pub addr: IpAddr,
    pub asn: Asn,
}

impl PeerEntry {
    pub fn parse<Octs: Octets>(parser: &mut Parser<'_, Octs>) -> Result<Self, ParseError> {
        let peer_type = parser.parse_u8()?;
        let bgp_id = parser.parse_u32_be()?.to_be_bytes();
        let addr = if peer_type & 0x01 == 0x00 {
            // ipv4
            let mut buf = [0u8; 4];
            parser.parse_buf(&mut buf)?;
            buf.into()
        } else {
            // ipv6
            let mut buf = [0u8; 16];
            parser.parse_buf(&mut buf)?;
            buf.into()
        };
        let asn: Asn = if peer_type & 0x02 == 0x02  {
            // asn32
            parser.parse_u32_be()?.into()
        } else {
            // asn16
            u32::from(parser.parse_u16_be()?).into()
        };

        Ok( PeerEntry { bgp_id, addr, asn } )
    }
}


//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                         Sequence Number                       |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       | Prefix Length |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                        Prefix (variable)                      |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |         Entry Count           |  RIB Entries (variable)
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

#[derive(Copy, Clone, Debug)]
pub struct RibEntryHeader<'a, Octs> {
    seq_number: u32,
    prefix: Prefix,
    entry_count: u16,
    entries: Parser<'a, Octs>,
}

impl<'a, Octs: Octets> RibEntryHeader<'a, Octs> {
    pub fn parse(parser: &mut Parser<'a, Octs>, afi: Afi)
        -> Result<Self, ParseError>
    {
        let seq_number = parser.parse_u32_be()?;
        let prefix_len = parser.parse_u8()?;
        let prefix = parse_prefix(parser, prefix_len, afi)?;
        let entry_count = parser.parse_u16_be()?;
        let entries = parser.parse_parser(parser.remaining())?;
        Ok( RibEntryHeader {
            seq_number,
            prefix,
            entry_count,
            entries,
        })
    }

    pub fn seq_number(&self) -> u32 {
        self.seq_number
    }

    pub fn prefix(&self) -> Prefix {
        self.prefix
    }

    pub fn entries(&mut self) -> Parser<'_, Octs> {
        self.entries
    }
}

impl<Octs: Octets> fmt::Display for RibEntryHeader<'_, Octs> {
    fn fmt (&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{:>10}] rib entry for {}",
               self.seq_number(),
               self.prefix()
        )
    }
}

//        0                   1                   2                   3
//        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |         Peer Index            |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                         Originated Time                       |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |      Attribute Length         |
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//       |                    BGP Attributes... (variable)
//       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

#[derive(Copy, Clone, Debug)]
pub struct RibEntry<'a, Octs> {
    peer_idx: u16,
    orig_time: u32,
    //attribute_len: u16,
    pub attributes: Parser<'a, Octs>,
}

impl<'a, Octs: Octets> RibEntry<'a, Octs> {
    pub fn parse(parser: &mut Parser<'a, Octs>)
        -> Result<Self, ParseError>
    {
        let peer_idx = parser.parse_u16_be()?;
        let orig_time = parser.parse_u32_be()?;
        let attribute_len = parser.parse_u16_be()?;
        let attributes = parser.parse_parser(attribute_len as usize)?;

        Ok( RibEntry {
            peer_idx, orig_time, attributes
        })
    }
    pub fn peer_index(&self) -> u16 {
        self.peer_idx
    }

    pub fn orig_time(&self) -> u32 {
        self.orig_time
    }
}


impl<Octs: Octets> fmt::Display for RibEntry<'_, Octs> {
    fn fmt (&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "peer idx {} orig_time {}",
               self.peer_index(),
               self.orig_time(),
        )
    }
}

//--- copied form routecore for now TODO pub export in routecore ? -----------

fn prefix_bits_to_bytes(bits: u8) -> usize {
    if bits != 0 {
        (bits as usize - 1) / 8 + 1
    } else {
        0
    }
}

fn parse_prefix<R: Octets>(parser: &mut Parser<'_, R>, prefix_bits: u8, afi: Afi)
    -> Result<Prefix, ParseError>
{
    let prefix_bytes = prefix_bits_to_bytes(prefix_bits);
    let prefix = match (afi, prefix_bytes) {
        (Afi::Ipv4, 0) => {
            Prefix::new_v4(0.into(), 0).map_err(|_| ParseError("prefix error"))?
        },
        (Afi::Ipv4, _b @ 5..) => {
            return Err(ParseError("illegal byte size for IPv4 NLRI"))
        },
        (Afi::Ipv4, _) => {
            let mut b = [0u8; 4];
            b[..prefix_bytes].copy_from_slice(parser.peek(prefix_bytes)?);
            parser.advance(prefix_bytes)?;
            Prefix::new(IpAddr::from(b), prefix_bits).map_err(|_e|
                    ParseError("prefix parsing failed")
            )?
        }
        (Afi::Ipv6, 0) => {
            Prefix::new_v6(0.into(), 0).map_err(|_| ParseError("prefix error"))?
        },
        (Afi::Ipv6, _b @ 17..) => {
            return Err(ParseError("illegal byte size for IPv6 NLRI"))
        },
        (Afi::Ipv6, _) => {
            let mut b = [0u8; 16];
            b[..prefix_bytes].copy_from_slice(parser.peek(prefix_bytes)?);
            parser.advance(prefix_bytes)?;
            Prefix::new(IpAddr::from(b), prefix_bits).map_err(|_e|
                    ParseError("prefix parsing failed")
            )?
        },
        (_, _) => {
            panic!("unimplemented")
        }
    };
    Ok(prefix)
}

//----------- Peer Index Table -----------------------------------------------

#[derive(Clone)]
pub struct PeerIndex {
    peers: Vec<PeerEntry>
}

impl PeerIndex {
    pub fn empty() -> Self {
        PeerIndex { peers: Vec::new() }
    }

    pub fn reserve(&mut self, n: usize) {
        self.peers.reserve(n);
    }

    pub fn with_capacity(n: usize) -> Self {
        PeerIndex { peers: Vec::with_capacity(n) }
    }

    pub fn push(&mut self, p: PeerEntry) {
        self.peers.push(p);
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.len() == 0
    }

    pub fn get<Octs: Octets>(&self, rib_entry: &RibEntry<Octs>)
        -> Option<&PeerEntry>
    {
        self.peers.get(usize::from(rib_entry.peer_index()))
    }
}

impl<I: SliceIndex<[PeerEntry]>> Index<I> for PeerIndex {
    type Output = I::Output;
    fn index(&self, i: I) -> &Self::Output {
        &self.peers[i]
    }
}

//------------ Convenience stuff / public API ---------------------------------

pub struct MrtFile<'a> {
    raw: &'a [u8],
}
impl<'a> MrtFile<'a> {
    pub fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    pub fn rib_entries(&'a self) -> Result<RibEntryIterator<'a, &'a [u8]>, ParseError> {
        let mut parser = Parser::from_ref(&self.raw);
        let peer_index = Self::extract_peer_index_table(&mut parser)?;
        Ok(
            RibEntryIterator::new(
                peer_index,
                parser
            )
        )
    }

    pub fn pi(&self) -> PeerIndex {
        let mut parser = Parser::from_ref(&self.raw);
        Self::extract_peer_index_table(&mut parser).unwrap()
    }

    pub fn rib_entries_mt<Octs: 'a + Octets>(&'a self)
        -> impl ParallelIterator<Item = <SingleEntryIterator<'a, Octs> as Iterator>::Item> + 'a
    where
        Vec<u8>: OctetsFrom<Octs::Range<'a>>
    {
        let mut parser = Parser::from_ref(&self.raw);
        let peer_index = Self::extract_peer_index_table(&mut parser).unwrap();

        let tables = TableDumpIterator::new(peer_index, parser);
        let iter = tables.par_bridge().map(|(_fam, reh)|{
            SingleEntryIterator::new(reh)
        }).flat_map_iter(|e| e.into_iter());
        iter
    }

    pub fn tables(&'a self) -> Result<TableDumpIterator<'a, &'a [u8]>, ParseError> {
        let mut parser = Parser::from_ref(&self.raw);
        let peer_index = Self::extract_peer_index_table(&mut parser)?;
        Ok(TableDumpIterator::new(peer_index, parser))
    }

    fn extract_peer_index_table(parser: &mut Parser<'_, &[u8]>) -> Result<PeerIndex, ParseError> {
        let mut m = CommonHeader::parse(parser)?;
        let mut peer_index = PeerIndex::empty();

        match m.subtype() {
            MessageSubType::TableDumpv2SubType(tdv2) => {
                match tdv2 {
                    TableDumpv2SubType::PeerIndexTable => {
                        assert!(peer_index.is_empty());
                        let mut pit = PeerIndexTable::parse(&mut m.message)?;
                        peer_index.reserve(pit.peer_count().into());
                        let mut pes = pit.entries();
                        while pes.remaining() > 0 {
                            let pe = PeerEntry::parse(&mut pes).unwrap();
                            //println!("peer entry {pe:?}");
                            peer_index.push(pe);
                        }
                        assert_eq!(peer_index.len(), pit.peer_count().into());
                        //println!("peer table with {} entries", peer_index.len());
                        Ok(peer_index)
                    },
                    _ => {
                        Err(ParseError("expected PeerIndexTable"))
                    }
                }
            }
            _ => { Err(ParseError("expected TableDumpv2SubType")) }
        }
    }
}

//------------ TableDumpIterator ----------------------------------------------

pub struct TableDumpIterator<'a, Octs> {
    pub peer_index: PeerIndex,
    parser: Parser<'a, Octs>,
}

impl<'a, Octs> TableDumpIterator<'a, Octs> {
    pub fn new(peer_index: PeerIndex, parser: Parser<'a, Octs>) -> Self {
        Self { peer_index, parser }
    }
}

impl<'a, Octs: Octets> Iterator for TableDumpIterator<'a, Octs>
where
    Vec<u8>: OctetsFrom<Octs::Range<'a>>
{
    // u16 for the 'ingress_id', for now
    type Item = (AfiSafiType, RibEntryHeader<'a, Octs>);

    fn next(&mut self) -> Option<Self::Item> {
        
        if self.parser.remaining() == 0 {
            return None;
        }

        let mut m = CommonHeader::parse(&mut self.parser).unwrap();
        if let MessageSubType::TableDumpv2SubType(tdv2) = m.subtype() {
            match tdv2 {
                TableDumpv2SubType::RibIpv4Unicast => {
                    let reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv4).unwrap();
                    Some((AfiSafiType::Ipv4Unicast, reh))
                }
                TableDumpv2SubType::RibIpv6Unicast => {
                    let reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv6).unwrap();
                    Some((AfiSafiType::Ipv6Unicast, reh))
                }
                _ => todo!()
            }
        } else {
            None
        }
    }
}

pub struct SingleEntryIterator<'a, Octs> {
    //peer_index: &'a PeerIndex,
    prefix: Prefix,
    parser: Parser<'a, Octs>, // the RibEntryHeader.entries parser
}

// TODO IntoIterator on TableDumpv2SubType
impl<'a, Octs> SingleEntryIterator<'a, Octs> {
    //pub fn new(peer_index: &'a PeerIndex, reh: RibEntryHeader<'a, Octs>) -> Self {
    pub fn new(reh: RibEntryHeader<'a, Octs>) -> Self {
        Self {
            //peer_index,
            prefix: reh.prefix,
            parser: reh.entries,
        }
    }
}

impl<'a, Octs: Octets> Iterator for SingleEntryIterator<'a, Octs>
where
    Vec<u8>: OctetsFrom<Octs::Range<'a>>
{
    //type Item = (Prefix, u16, PeerEntry, Vec<u8>);
    type Item = (Prefix, u16, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.parser.remaining() == 0 {
            return None;
        }
        let re = RibEntry::parse(&mut self.parser).unwrap();
        //let peer = self.peer_index.get(&re).unwrap();
        let mut v = re.attributes;
        let mut raw_attr = vec![0; v.remaining()];
        let _ = v.parse_buf(&mut raw_attr[..]);

        //Some((self.prefix, re.peer_idx, *peer, raw_attr))
        Some((self.prefix, re.peer_idx, raw_attr))
    }
}



//------------ RibEntryIterator -----------------------------------------------

pub struct RibEntryIterator<'a, Octs> {
    peer_index: PeerIndex,
    parser: Parser<'a, Octs>,
    current_table: Option<RibEntryHeader<'a, Octs>>,
    current_afisafi: Option<AfiSafiType>,
}
impl<'a, Octs> RibEntryIterator<'a, Octs> {
    fn new(peer_index: PeerIndex, parser: Parser<'a, Octs>) -> Self {
        Self {
            peer_index,
            parser, 
            current_table: None,
            current_afisafi: None,
        }
    }
}


impl<'a, Octs: Octets> Iterator for RibEntryIterator<'a, Octs>
where
    Vec<u8>: OctetsFrom<Octs::Range<'a>>
{
    //type Item = (AfiSafiType, u16, PeerEntry, Prefix, PaMap);
    type Item = (AfiSafiType, u16, PeerEntry, Prefix, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item>
    {
        if self.current_table.is_none() {
            if self.parser.remaining() == 0 {
                return None;
            }

            let mut m = CommonHeader::parse(&mut self.parser).unwrap();

            if let MessageSubType::TableDumpv2SubType(tdv2) = m.subtype() {
                match tdv2 {
                    TableDumpv2SubType::RibIpv4Unicast => {
                        let reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv4).unwrap();
                        self.current_table = Some(reh);
                        self.current_afisafi = Some(AfiSafiType::Ipv4Unicast);
                    }
                    TableDumpv2SubType::RibIpv6Unicast => {
                        let reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv6).unwrap();
                        self.current_table = Some(reh);
                        self.current_afisafi = Some(AfiSafiType::Ipv6Unicast);
                    }
                    _ => todo!()
                }
            }
        }

        let mut table = self.current_table.take().unwrap();
        let re = RibEntry::parse(&mut table.entries).unwrap();
        let peer = self.peer_index.get(&re).unwrap();
        // XXX here we probably need a PduParseInfo::mrt()
        let prefix = table.prefix;

        let mut v = re.attributes;
        let mut raw_attr = vec![0; v.remaining()];
        let _ = v.parse_buf(&mut raw_attr[..]);

        //let mut raw_attr = vec![];
        //let mem_info = allocation_counter::measure(|| {
        //    raw_attr = vec![0; v.remaining()];
        //    let _ = v.parse_buf(&mut raw_attr[..]);
        //});


        /*
        let pas = PathAttributes::new(re.attributes, PduParseInfo::modern());
        let mut pa_map = PaMap::empty();

        let mem_info = allocation_counter::measure(|| {
            for pa in pas {
                match pa {
                    Ok(pa) => {
                        pa_map.attributes_mut().insert(
                            pa.type_code(), pa.to_owned().unwrap()
                        );
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        break;
                        //return None;
                    }
                
                }
            }
        });

        #[cfg(test)]
        eprintln!(
            "{} {} {:.1} {}",
            re.attributes.remaining(),
            mem_info.bytes_current,
            mem_info.bytes_current as f64 / re.attributes.remaining() as f64,
            mem_info.count_current,

        ); //TODO allocation_mem_bytes);
        */




        if table.entries.remaining() != 0 {
            self.current_table = Some(table);
        } 

        //Some((*self.current_afisafi.as_ref().unwrap(), re.peer_idx, *peer, prefix, pa_map))
        Some((*self.current_afisafi.as_ref().unwrap(), re.peer_idx, *peer, prefix, raw_attr))
    }
}


//----------- Errors ---------------------------------------------------------

#[derive(Debug)]
pub struct ParseError(&'static str);
impl std::error::Error for ParseError { }
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "can't parse: {}", self.0)
    }
}

impl From<octseq::ShortInput> for ParseError {
    fn from(_: octseq::ShortInput) -> ParseError {
        ParseError("short input")
    }
}


//------------ Tests ----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use memmap2::Mmap;
    use std::fs::File;
    use rayon::iter::ParallelBridge;
    use rayon::iter::ParallelIterator;

    use routecore::bgp::{aspath::AsPath, path_attributes::{PathAttributeType, PathAttributes}};


    fn get_fh() -> Mmap {
        let filename = "../test-data/latest-bview.mrt";
        //let filename = "latest-update.mrt";
        //let filename = "rib.20230515.0800.mrt";
        //let filename = "route-collector.ams.pch.net-mrt-bgp-updates-2023-05-15-16-28";
        let file = File::open(filename).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap()  };
        println!("{}: {}MiB", filename, mmap.len() >> 20);
        mmap
    }

    // LH: so this is much slower than the original RibEntryIterator !
    #[test]
    fn par_bridge() {
        let fh = &get_fh()[..];
        let mrt_file = MrtFile::new(fh);
        let rib_entries = mrt_file.rib_entries().unwrap();
        
        println!();
        rib_entries.par_bridge().for_each(|e| {
            let (_, _, _, _, pa_map) = e;
            assert!(!pa_map.is_empty());
        });
    }

    #[test]
    fn tables_iterator() {
        let fh = &get_fh()[..];
        let mrt_file = MrtFile::new(fh);
        let tables = mrt_file.tables().unwrap();
        for (_fam, reh) in tables {
            let iter = SingleEntryIterator::new(reh);
            for (idx, (_prefix, _id, raw_attr)) in iter.enumerate() {
                assert!(!raw_attr.is_empty());
                print!("{idx}\r");
            }
        }
    }

    #[test]
    fn iter_mt() {
        let fh = &get_fh()[..];
        let mrt_file = MrtFile::new(fh);
        eprintln!("{}", mrt_file.rib_entries_mt::<&[u8]>().count());
    }
    
    #[test]
    fn par_bridge_tables_iterator() {
        let fh = &get_fh()[..];
        let mrt_file = MrtFile::new(fh);
        let tables = mrt_file.tables().unwrap();
        let count = tables.par_bridge().map(|(_fam, reh)| {
            SingleEntryIterator::new(reh)
        }).fold(|| 0_usize, |sum, iter| sum + iter.count()).sum::<usize>();
        eprintln!("count: {count}");
    }

    #[test]
    fn iterators_count() {
        let fh = &get_fh()[..];
        let mrt_file = MrtFile::new(fh);
        let rib_entries = mrt_file.rib_entries().unwrap();
        let tables = mrt_file.tables().unwrap();
        let single_entries_count = tables.map(|(_fam, reh)| {
            let iter = SingleEntryIterator::new(reh);
            iter
        }).fold(0, |acc, iter| acc + iter.count());

        eprintln!("{}", single_entries_count);
        assert_eq!(rib_entries.count(), single_entries_count);
    }

    #[test]
    fn it_works() {
        let fh = &get_fh()[..];

        let mut p = Parser::from_ref(&fh);
        let mut peer_index = PeerIndex::empty();

        // FIXME we need a special sessionconfig/pdu_parse_info because in MRT
        // the (I think) MP_REACH is slightly different than in actual BGP
        //let sc = SessionConfig::modern();

        while let Ok(ref mut m) = CommonHeader::parse(&mut p) {
            match m.subtype() {
                MessageSubType::TableDumpv2SubType(tdv2) => {
                    match tdv2 {
                        TableDumpv2SubType::PeerIndexTable => {
                            // XXX for now, we expect only a single
                            // PeerIndexTable per file
                            assert!(peer_index.is_empty());
                            let mut pit = PeerIndexTable::parse(&mut m.message).unwrap();
                            peer_index.reserve(pit.peer_count().into());
                            let mut pes = pit.entries();
                            while pes.remaining() > 0 {
                                let pe = PeerEntry::parse(&mut pes).unwrap();
                                //println!("peer entry {pe:?}");
                                peer_index.push(pe);
                            }
                            assert_eq!(peer_index.len(), pit.peer_count().into());
                            println!("peer table with {} entries", peer_index.len());
                        }
                        TableDumpv2SubType::RibIpv4Unicast => {
                            let mut reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv4).unwrap();
                            //println!("{}", reh);
                            let mut entries = reh.entries();
                            while entries.remaining() > 0 {
                                let mut re = RibEntry::parse(&mut entries).unwrap();
                                let peer = peer_index.get(&re);
                                //println!("\t{} {:?}", re, peer);

                                //println!("attr: {:?}", re.attributes);
                                //println!("attr: {:?}", re.attributes.parse_octets(re.attributes.remaining()).unwrap());
                                //let pas = match PathAttributes::parse(&mut re.attributes, sc) {
                                //let pas = match PathAttributes::new(re.attributes, sc) {
                                //    Ok(pas) => pas,
                                //    Err(e) => { eprintln!("error while parsing RibIpv4Unicast: {}", e); break; }
                                //};
                                let pas = PathAttributes::new(re.attributes, PduParseInfo::modern());
                                if let Some(aspath) = pas.get(PathAttributeType::AsPath) {
                                //if let Some(aspath) = pas.find(|pa| pa.type_code() == PathAttributeType::AsPath) {
                                    let asp = unsafe {AsPath::new_unchecked(aspath.as_ref(), true) };
                                    //println!("\t{asp}");
                                }
                                //for pa in pas.iter() {
                                //    println!("{:?}", pa.type_code());
                                //}
                            }
                        }
                        TableDumpv2SubType::RibIpv6Unicast => {
                            let mut reh = RibEntryHeader::parse(&mut m.message, Afi::Ipv6).unwrap();
                            println!("{}", reh.prefix);
                            //println!("{}", reh);
                            let mut entries = reh.entries();
                            while entries.remaining() > 0 {
                                let mut re = RibEntry::parse(&mut entries).unwrap();
                                //println!("\t{}", re);
                                //let pas = match PathAttributes::parse(&mut re.attributes, sc) {
                                //    Ok(pas) => pas,
                                //    Err(e) => { eprintln!("error while parsing RibIpv6Unicast: {}", e); break; }
                                //};

                                let pas = PathAttributes::new(re.attributes, PduParseInfo::modern());
                                if let Some(aspath) = pas.get(PathAttributeType::AsPath) {
                                //if let Some(aspath) = pas.iter().find(|pa| pa.type_code() == PathAttributeType::AsPath) {
                                    let asp = unsafe {AsPath::new_unchecked(aspath.as_ref(), true) };
                                    //println!("\t{asp}");
                                }
                                //for pa in pas.iter() {
                                //    println!("{:?}", pa.type_code());
                                //}
                            }
                        }
                        n => {
                            eprintln!("processed {}/{}", p.pos() >> 20, p.len() >> 20);
                            todo!("TODO: {n}")
                        }
                    }
                }
                MessageSubType::Bgp4MpSubType(_bgp4mp) => {
                    //match bgp4mp {
                    //    Bgp4MpSubType::Message | Bgp4MpSubType::MessageAs4 => { }
                    //    _ => { println!("got a {bgp4mp:?}"); }
                    //}
                }
            }
        }
        println!("done");
    }
}
