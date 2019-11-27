use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{CommandRequest, CommandResponse, Result};
use std::net::TcpStream;
use std::process;

fn main() -> Result<()> {
    let addr_arg = Arg::with_name("addr")
        .long("addr")
        .takes_value(true)
        .value_name("IP-ADDR")
        .default_value("127.0.0.1:4000");
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("key").required(true))
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("key").required(true))
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true))
                .arg(&addr_arg),
        )
        .get_matches();

    let (command, maybe_args) = matches.subcommand();
    let args = maybe_args.unwrap();
    let addr = args.value_of("addr").unwrap();

    let mut stream = TcpStream::connect(addr)?;

    let request = match command {
        "get" => {
            let key = args.value_of("key").unwrap();
            CommandRequest::Get {
                key: key.to_owned(),
            }
        }
        "set" => {
            let key = args.value_of("key").unwrap();
            let value = args.value_of("value").unwrap();
            CommandRequest::Set {
                key: key.to_owned(),
                value: Some(value.to_owned()),
            }
        }
        "rm" => {
            let key = args.value_of("key").unwrap();
            CommandRequest::Set {
                key: key.to_owned(),
                value: None,
            }
        }
        _ => unreachable!(),
    };

    bincode::serialize_into(&mut stream, &request)?;
    let response = bincode::deserialize_from::<&TcpStream, CommandResponse>(&stream)?;
    match response {
        CommandResponse::Message(message) => {
            if message != "" {
                println!("{}", message)
            }
        }
        CommandResponse::KeyNotFound => {
            eprintln!("Key not found");
            process::exit(1)
        }
    }

    Ok(())
}
