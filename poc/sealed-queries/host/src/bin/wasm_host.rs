//! POC 4: a query as a WebAssembly module, loaded at run time. The module imports nothing, so
//! it can only compute: no files, no network, no clock; fuel bounds its CPU and a limit its
//! memory. What it hides: nothing a disassembler (or `strings`) does not show.
//!
//! wasm_host query.wasm name=path=col:type,... ...
use anyhow::{anyhow, Result};
use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

fn main() -> Result<()> {
    let args = sealed::args();
    let input = sealed::module_input(&args[1..])?;
    let engine = Engine::new(Config::new().consume_fuel(true))?;
    let module = Module::from_file(&engine, &args[0])?;
    for i in module.imports() {
        println!("module imports {}::{}: refused", i.module(), i.name());
    }
    let limits = StoreLimitsBuilder::new().memory_size(1 << 30).build();
    let mut store: Store<StoreLimits> = Store::new(&engine, limits);
    store.limiter(|l| l);
    store.set_fuel(50_000_000_000)?;
    // no imports given: a module that needs any fails here
    let instance = Instance::new(&mut store, &module, &[])?;
    let memory = instance.get_memory(&mut store, "memory").ok_or(anyhow!("no memory"))?;
    let alloc = instance.get_typed_func::<u32, u32>(&mut store, "alloc")?;
    let run = instance.get_typed_func::<(u32, u32), u32>(&mut store, "run")?;
    let out_len = instance.get_typed_func::<(), u32>(&mut store, "out_len")?;
    let at = alloc.call(&mut store, input.len() as u32)?;
    memory.write(&mut store, at as usize, &input)?;
    let out = run.call(&mut store, (at, input.len() as u32))?;
    let mut buf = vec![0; out_len.call(&mut store, ())? as usize];
    memory.read(&store, out as usize, &mut buf)?;
    print!("{}", String::from_utf8_lossy(&buf));
    eprintln!("fuel used: {}", 50_000_000_000 - store.get_fuel()?);
    Ok(())
}
