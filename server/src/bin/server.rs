#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{Engine, Error, Result, CommandRequest, CommandResponse};
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
        match stream {
            Ok(mut stream) => {
                match stream.peer_addr() {
                    Ok(peer_addr) => info!(logger, "{} connected!", peer_addr),
                    Err(e) => {
                        error!(logger, "{}", e);
                        continue;
                    }
                }

                if let Ok(request) = bincode::deserialize_from::<&TcpStream, CommandRequest>(&stream) {
                    info!(logger, "REQUEST: {:?}", request);
                    let response = CommandResponse::Message("Thanks!".to_owned());
                    if let Err(e) = bincode::serialize_into(&stream, &response) {
                        error!(logger, "{}", e);
                    }
                } else {
                    warn!(logger, "Bad request");
                }
            }
            Err(e) => {
                error!(logger, "Could not connect: {:?}", e);
            }
        }
    }

    Ok(())
}
