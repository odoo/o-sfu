//! Matches the server allocator unless DHAT requires system allocation hooks.
//!
//! System realloc copies made unrelated room setup changes dominate policy
//! instruction counts. All benchmark executables use this allocator declaration.

#[cfg(all(not(target_env = "msvc"), not(feature = "dhat")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
