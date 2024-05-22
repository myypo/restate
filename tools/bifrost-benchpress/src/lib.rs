// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use restate_types::config::CommonOptionCliOverride;

use self::append_latency::AppendLatencyOpts;
use self::read_to_write::WriteToReadOpts;

pub mod append_latency;
pub mod read_to_write;
pub mod util;

#[derive(Debug, Clone, clap::Parser)]
#[command(author, version, about)]
pub struct Arguments {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE"
    )]
    pub config_file: Option<PathBuf>,

    #[arg(long)]
    pub no_prometheus_stats: bool,

    #[arg(long)]
    pub no_rocksdb_stats: bool,

    #[arg(long)]
    pub retain_test_dir: bool,

    #[clap(flatten)]
    pub opts_overrides: CommonOptionCliOverride,

    /// Choose the benchmark to run
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, clap::Parser)]
pub enum Command {
    /// Measures the write-to-read latency for a single log
    WriteToRead(WriteToReadOpts),
    /// Measures the append latency for a single log
    AppendLatency(AppendLatencyOpts),
}
