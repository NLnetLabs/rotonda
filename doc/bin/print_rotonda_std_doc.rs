use rotonda::common::roto_runtime::rotonda_roto_runtime;

fn main() {
    let runtime = rotonda_roto_runtime().unwrap();
    runtime.print_documentation();
}
