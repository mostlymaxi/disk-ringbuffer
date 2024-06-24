fn main() {
    println!("cargo::rerun-if-changed=src/page.c");
    cc::Build::new().file("src/page.c").compile("page");
}
