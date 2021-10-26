fn main() {
    let protos = glob::glob("protos/*.proto").unwrap();
    let paths: Vec<_> = protos.into_iter().map(|item| item.unwrap()).collect();
    prost_build::compile_protos(&paths, &["protos"]).unwrap();
}
