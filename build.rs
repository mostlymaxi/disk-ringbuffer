use std::env;
use std::path::PathBuf;

fn main() {
    let bindings = bindgen::Builder::default()
        .header("src/page.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path =
        PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set!"));

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    cc::Build::new()
        .warnings(true)
        .extra_warnings(true)
        .file("src/page.c")
        .compile("page");
}
