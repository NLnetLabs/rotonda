use std::env;

fn main() {
    println!(
        "cargo:warning=Directories '/etc' and '/examples' can be found in '{:?}'",
        env::var_os("CARGO_MANIFEST_DIR").unwrap()
    );
}
