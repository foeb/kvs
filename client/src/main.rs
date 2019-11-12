#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use clap::{App, AppSettings, Arg, SubCommand};
use slog::Drain;

fn main() {
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
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("key").required(true)))
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("key").required(true)))
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("key")
            .required(true))
                .arg(Arg::with_name("value").required(true)),
        )
        .get_matches();

    let (command, maybe_args) = matches.subcommand();
    let args = maybe_args.unwrap();
    let addr = matches.value_of("addr").unwrap();

    info!(logger, "IP-ADDR: {}", addr);

    match command {
        "get" => {
            let key = args.value_of("key").unwrap();
            info!(logger, "COMMAND: get {}", key);
        },
        "set" => {
            let key = args.value_of("key").unwrap();
            let value = args.value_of("value").unwrap();
            info!(logger, "COMMAND: set {} {}", key, value);
        },
        "rm" => {
            let key = args.value_of("key").unwrap();
            info!(logger, "COMMAND: rm {}", key);
        },
        _ => unreachable!(),
    }
}
