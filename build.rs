use std::env;

fn main() {
    println!(
        "cargo:warning=Directory '/etc' can be found in {:?}",
        env::var_os("CARGO_MANIFEST_DIR").unwrap()
    );
}
