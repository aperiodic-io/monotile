//! POC 5: the same query module as a native shared library, loaded at run time. As fast as the
//! engine, and as able as this process: it can read any file and open any socket the host can.
//! Running an opaque one means sandboxing the process yourself (seccomp, no network, a jail).
//!
//! so_host libguest.so name=path=col:type,... ...
use anyhow::Result;

fn main() -> Result<()> {
    let args = sealed::args();
    let input = sealed::module_input(&args[1..])?;
    // SAFETY: the module is trusted to follow the ABI (guest/src/lib.rs); nothing else here can
    // be: a native module is code this process runs as itself.
    unsafe {
        let lib = libloading::Library::new(&args[0])?;
        let alloc: libloading::Symbol<unsafe extern "C" fn(usize) -> *mut u8> = lib.get(b"alloc")?;
        let run: libloading::Symbol<unsafe extern "C" fn(*mut u8, usize) -> *mut u8> = lib.get(b"run")?;
        let out_len: libloading::Symbol<unsafe extern "C" fn() -> usize> = lib.get(b"out_len")?;
        let at = alloc(input.len());
        std::ptr::copy_nonoverlapping(input.as_ptr(), at, input.len());
        let out = run(at, input.len());
        print!("{}", String::from_utf8_lossy(std::slice::from_raw_parts(out, out_len())));
    }
    Ok(())
}
