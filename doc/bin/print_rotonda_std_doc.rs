use std::path::Path;

use rotonda::roto_runtime::create_runtime;

fn main() {
    let runtime = create_runtime().unwrap();
    let _ = runtime.print_documentation(Path::new("/tmp/roto_runtime_docs"));
}
