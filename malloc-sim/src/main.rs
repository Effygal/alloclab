// Copyright 2026 Effy Wang
// SPDX-License-Identifier: Apache-2.0

mod config;
mod sim;

use std::io::{self, Write};

use config::Config;
use sim::Simulator;

fn main() {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };

    let mut sim = Simulator::new(config);
    if let Err(err) = sim.run() {
        eprintln!("{err}");
        std::process::exit(1);
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();
    sim.write_report(&mut out)
        .expect("failed to write simulator report");
    out.flush().expect("failed to flush simulator report");
}
