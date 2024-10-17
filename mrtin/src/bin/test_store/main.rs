use std::fs::File;
use std::fmt;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use inetnum::addr::Prefix;
use memmap2::Mmap;
use mrtin::MrtFile;
use rayon::prelude::*;
use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use rotonda_store::custom_alloc::UpsertReport;
use rotonda_store::prelude::multi::PrefixStoreError;
use rotonda_store::prelude::multi::{MultiThreadedStore, RouteStatus};
use rotonda_store::PublicRecord;
use routecore::bgp::path_attributes::PaMap;

use rand::seq::SliceRandom;

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

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Enable concurrent route inserts
    #[arg(short, long, default_value_t = false)]
    mt: bool,

    /// Prime store by sequentially inserting prefixes first
    #[arg(short, long, default_value_t = false)]
    prime: bool,

    /// Enable concurrent priming inserts
    #[arg(long, default_value_t = false)]
    mt_prime: bool,

    
    /// Shuffle prefixes before priming the store. Enables priming.
    #[arg(short, long, default_value_t = false)]
    shuffle: bool,

    /// Use the same store for all MRT_FILES
    #[arg(long, default_value_t = false)]
    single_store: bool,

    /// MRT files to process.
    #[arg(required = true)]
    mrt_files: Vec<PathBuf>,

    /// Don't insert in store, only parse MRT_FILES
    #[arg(long, default_value_t = false)]
    parse_only: bool,

}

fn insert<T: rotonda_store::Meta>(
    store: &MultiThreadedStore<T>,
    prefix: &Prefix,
    mui: u32,
    ltime: u64,
    route_status: RouteStatus,
    value: T
) -> Result<UpsertReport, PrefixStoreError> {
    let record = PublicRecord::new(
        mui, ltime, route_status, value
    );
    store.insert(
        prefix,
        record,
        None
    ).inspect_err(|e| eprintln!("{e}"))
}

fn main() {
        let args = Cli::parse();

        let mut store = MultiThreadedStore::<MyPaMap>::new().unwrap();
        let t_total = Instant::now();
        let mut routes_total: usize = 0;
        let mut mib_total: usize = 0;
        for mrtfile in &args.mrt_files {

            if !args.single_store {
                store = MultiThreadedStore::<MyPaMap>::new().unwrap();
            }

            let file = File::open(mrtfile).unwrap();
            let mmap = unsafe { Mmap::map(&file).unwrap()  };
            println!("{}: {}MiB", mrtfile.to_string_lossy(), mmap.len() >> 20);
            mib_total += mmap.len() >> 20;


            let t_0 = Instant::now();
            let t_prev = t_0;

            let mrt_file = MrtFile::new(&mmap[..]);
            let tables = mrt_file.tables().unwrap();

            if !args.parse_only && (args.shuffle || args.prime) {
                let mut prefixes = mrt_file.tables().unwrap().par_bridge().map(|(_fam, reh)| {
                    let iter = mrtin::SingleEntryIterator::new(reh);
                    iter.map(|(prefix, _, _)| prefix)
                }).flatten_iter().collect::<Vec<_>>();

                eprintln!(
                    "collected {} prefixes to prime store, took {}ms",
                    prefixes.len(),
                    t_prev.elapsed().as_millis(),
                );

                if args.shuffle {
                    let t_s = Instant::now();
                    eprint!("shuffling before priming... ");
                    prefixes.shuffle(&mut rand::thread_rng());
                    eprintln!("done! took {}ms", t_s.elapsed().as_millis());
                }

                let t_prev = Instant::now();

                if args.mt {
                    prefixes.par_iter().for_each(|p|{
                        let _ = insert(&store, p, 0, 0, RouteStatus::InActive, MyPaMap(vec![]));
                    });
                } else {
                    for p in &prefixes {
                        let _ = insert(&store, p, 0, 0, RouteStatus::InActive, MyPaMap(vec![]));
                    }
                }
                eprintln!(

                    "primed store with {} prefixes, took {}ms",
                    prefixes.len(),
                    t_prev.elapsed().as_millis(),
                );
            }

            let t_prev = Instant::now();

            let mut num_routes = 0;
            if args.mt {
                num_routes = tables.par_bridge().map(|(_fam, reh)|  {
                    let iter = mrtin::SingleEntryIterator::new( reh);
                
                    let mut cnt = 0;
                    for e in iter {
                        cnt += 1;
                        let (prefix, peer_idx, pamap) = e;
                        let mui = peer_idx.into();
                        let ltime = 0;
                        let val = MyPaMap(pamap);

                        if !args.parse_only {
                            let _ = insert(&store, &prefix, mui, ltime, RouteStatus::Active, val);
                        }
                    }
                    cnt
                }).fold(|| 0, |sum, e| sum + e).sum::<usize>();
            } else {
                // single threaded 
                let rib_entries = mrt_file.rib_entries().unwrap();
                
                for e in rib_entries {
                    num_routes += 1;
                    let (_, peer_idx, _, prefix, pamap) = e;
                    let mui = peer_idx.into();
                    let ltime = 0;
                    let val = MyPaMap(pamap);

                    if !args.parse_only {
                        let _ = insert(&store, &prefix, mui, ltime, RouteStatus::Active, val);
                    }
                }

            }
            if !args.parse_only {
                eprintln!(
                    "inserted {} routes, took {}ms, total for this file {}ms",
                    num_routes,
                    t_prev.elapsed().as_millis(),
                    t_0.elapsed().as_millis(),
                );
            } else {
                eprintln!(
                    "parsed {}, no insertions,  total for this file {}ms",
                    num_routes,
                    t_0.elapsed().as_millis(),
                );

            }
            eprintln!("--------");

            routes_total += num_routes;
        }
        eprintln!(
            "Processed {} routes in {} files in {:.2}s",
            routes_total,
            args.mrt_files.len(),
            t_total.elapsed().as_millis() as f64 / 1000.0
        );
        eprintln!(
            "{:.0} routes per second\n\
            {:.0} MiB per second",
            routes_total as f64 / t_total.elapsed().as_secs() as f64,
            mib_total as f64 / t_total.elapsed().as_secs() as f64
        );
}
