use std::fs::File;
use std::fmt;

use memmap2::Mmap;
use mrtin::MrtFile;
use rotonda_store::prelude::multi::{MultiThreadedStore, RouteStatus};
use rotonda_store::PublicRecord;
use routecore::bgp::path_attributes::PaMap;


#[derive(Clone, Debug)]
//struct MyPaMap(PaMap);
struct MyPaMap(Vec<u8>);
impl std::fmt::Display for MyPaMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
        
    }
}

impl rotonda_store::Meta for MyPaMap {
    type Orderable<'a> = u32;

    type TBI = u32;

    fn as_orderable(&self, _tbi: Self::TBI) -> Self::Orderable<'_> {
        todo!()
    }
}

fn main() {
        let args: Vec<String> = std::env::args().collect();

        if args.len() < 2 {
            eprintln!("specify filename.mrt to process");
            std::process::exit(1);
        }

        let mrtfile = args.get(1).unwrap();

        let store = MultiThreadedStore::<MyPaMap>::new().unwrap();

        let file = File::open(mrtfile).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap()  };
        println!("{}: {}MiB", mrtfile, mmap.len() >> 20);

        let mrt_file = MrtFile::new(&mmap[..]);
        let rib_entries = mrt_file.rib_entries().unwrap();
        
        for e in rib_entries {
            let (_, peer_idx, _, prefix, pamap) = e;
            let mui = peer_idx.into();
            let ltime = 0;
            let route_status = RouteStatus::Active;
            let val = MyPaMap(pamap);

            let record = PublicRecord::new(
                mui, ltime, route_status, val
            );
            if let Err(e) = store.insert(
                &prefix,
                record,
                None
            ) {
                eprintln!("{e}");
                panic!();
            }
        }
}
