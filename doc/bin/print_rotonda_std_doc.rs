use rotonda::roto_runtime::create_runtime;

fn main() {
    let runtime = create_runtime().unwrap();
    runtime.print_documentation();
}
