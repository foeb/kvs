#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode;
use clap::{App, AppSettings, Arg};
use ctrlc;
use kvs::{CommandRequest, CommandResponse, Engine, Error, Result};
use server::{KvStore, SledEngine};
use sled::Db;
use slog::Drain;
use std::boxed::Box;
use std::env::current_dir;
use std::net::{TcpListener, TcpStream};
use std::process::exit;

fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("addr")
                .long("addr")
                .takes_value(true)
                .value_name("IP-ADDR")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .takes_value(true)
                .value_name("ENGINE-NAME")
                .possible_values(&["kvs", "sled"])
                .default_value("kvs"),
        )
        .get_matches();

    let addr = matches.value_of("addr").unwrap();
    let engine = matches.value_of("engine").unwrap();

    info!(logger, "IP-ADDR: {}", addr);
    info!(logger, "ENGINE-NAME: {}", engine);

    let mut engine: Box<dyn kvs::Engine> = if engine == "kvs" {
        Box::new(KvStore::open(current_dir()?.as_path())?)
    } else if engine == "sled" {
        Box::new(SledEngine {
            db: Db::open(current_dir()?.as_path())?,
        })
    } else {
        panic!("Invalid engine: {}", engine);
    };

    ctrlc::set_handler(move || {
        println!("");
        println!("Goodbye!");
        exit(0)
    })
    .expect("Error setting ctrl-c handler");

    let listener = TcpListener::bind(addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match stream.peer_addr() {
                    Ok(peer_addr) => info!(logger, "{} connected!", peer_addr),
                    Err(e) => {
                        error!(logger, "{}", e);
                        continue;
                    }
                }

                if let Ok(request) =
                    bincode::deserialize_from::<&TcpStream, CommandRequest>(&stream)
                {
                    info!(logger, "REQUEST: {:?}", request);

                    let response = match request {
                        CommandRequest::Get { key } => engine.get(key).map(|x| {
                            CommandResponse::Message(format!(
                                "{}",
                                x.unwrap_or("Key not found".to_owned())
                            ))
                        }),
                        CommandRequest::Set { key, value } => if let Some(value) = value {
                            engine.set(key, value)
                        } else {
                            engine.remove(key)
                        }
                        .map(|_| CommandResponse::Message("".to_owned())),
                    }
                    .unwrap_or_else(|e| match e {
                        Error::KeyNotFound => CommandResponse::KeyNotFound,
                        _ => CommandResponse::Message(format!("Error: {}", e)),
                    });

                    info!(logger, "RESPONSE: {:?}", &response);

                    if let Err(e) = bincode::serialize_into(&stream, &response) {
                        error!(logger, "{}", e);
                    }
                } else {
                    warn!(logger, "Bad request");
                }
            }
            Err(e) => {
                error!(logger, "Could not connect: {:?}", e);
                exit(1);
            }
        }
    }

    Ok(())
}
