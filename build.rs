// cleanup the build to not include a ton of junk (maybe?)

fn main() {
    let bindings = bindgen::Builder::default()
        .header("src/page.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file("src/bindings.rs")
        .expect("Couldn't write bindings!");

    cc::Build::new().file("src/page.c").compile("page");
}
