#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{Engine, Error, Result};
use slog::Drain;
use std::env::current_dir;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::process::exit;

fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("version" => "0.1"));

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .arg(
            Arg::with_name("addr")
                .value_name("IP-ADDR")
                .required(false)
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::with_name("engine")
                .value_name("ENGINE-NAME")
                .possible_values(&["kvs", "sled"])
                .required(false)
                .default_value("kvs")
                .takes_value(true),
        )
        .get_matches();

    let addr = matches.value_of("addr").unwrap();
    let engine = matches.value_of("engine").unwrap();

    if engine != "kvs" && engine != "sled" {
        panic!("Invalid engine: {}", engine);
    }

    info!(logger, "IP-ADDR: {}", addr);
    info!(logger, "ENGINE-NAME: {}", engine);

    let mut buf = [0u8; 1024];
    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming() {
        let n = stream?.read(&mut buf)?;
        if n > 0 {
            let s = String::from_utf8_lossy(&buf[0..n]);
            info!(logger, "MESSAGE: {}", s);
        } else {
            break;
        }
    }

    Ok(())
}
