fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["src/kv.proto"], &["src/"])?;
    Ok(())
}
