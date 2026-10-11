use crate::{constants::MILLISECONDS, replica::Replica};
use log::{LevelFilter, info};
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

pub mod checksum;
pub mod constants;
pub mod message;
pub mod prepare;
pub mod replica;
pub mod snowflake;
pub mod view;

fn main() {
    TermLogger::init(
        LevelFilter::Trace,
        Config::default(),
        TerminalMode::Stdout,
        ColorChoice::Auto,
    )
    .unwrap();
    info!("Logger initialized!");
    let mut replica = Replica::init();
    info!("New replica is in town. Oh yeah!");

    loop {
        replica.tick().unwrap();
        replica.run_for_ms(MILLISECONDS).unwrap();
    }
}
