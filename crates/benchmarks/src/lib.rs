#![allow(clippy::async_yields_async)]
// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for benchmarking the Restate runtime
use std::time::Duration;

use futures_util::{future, TryFutureExt};
use hyper::header::CONTENT_TYPE;
use hyper::{Body, Uri};
use pprof::flamegraph::Options;
use restate_server::config_loader::ConfigLoaderBuilder;
use restate_types::arc_util::Updateable;
use restate_types::config::{
    CommonOptionCliOverride, CommonOptionsBuilder, Configuration, ConfigurationBuilder,
    WorkerOptionsBuilder,
};
use tokio::runtime::Runtime;

use restate_core::{TaskCenter, TaskCenterBuilder, TaskKind};
use restate_node::Node;
use restate_types::retries::RetryPolicy;

pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

pub fn discover_deployment(current_thread_rt: &Runtime, address: Uri) {
    let discovery_payload = serde_json::json!({"uri": address.to_string()}).to_string();
    let discovery_result = current_thread_rt.block_on(async {
        RetryPolicy::fixed_delay(Duration::from_millis(200), 50)
            .retry(|| {
                hyper::Client::new()
                    .request(
                        hyper::Request::post("http://localhost:9070/deployments")
                            .header(CONTENT_TYPE, "application/json")
                            .body(Body::from(discovery_payload.clone()))
                            .expect("building discovery request should not fail"),
                    )
                    .map_err(anyhow::Error::from)
                    .and_then(|response| {
                        if response.status().is_success() {
                            future::ready(Ok(response))
                        } else {
                            future::ready(Err(anyhow::anyhow!("Discovery was unsuccessful.")))
                        }
                    })
            })
            .await
    });

    assert!(discovery_result
        .expect("Discovery must be successful")
        .status()
        .is_success(),);
}

pub fn spawn_restate(
    mut updateable_config: impl Updateable<Configuration> + Send + 'static,
    old_config: restate_node::config::Configuration,
) -> TaskCenter {
    let common = &updateable_config.load().common;
    let tc = TaskCenterBuilder::default()
        .options(common.clone())
        .build()
        .expect("task_center builds");
    let cloned_tc = tc.clone();
    tc.spawn(TaskKind::TestRunner, "benchmark", None, async move {
        let config = updateable_config.load();
        let node = Node::new(&old_config, config).expect("Restate node must build");
        cloned_tc
            .run_in_scope("startup", None, node.start(updateable_config))
            .await
    })
    .unwrap();

    tc
}

pub fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        // Ignore different thread origins to merge traces. This seems not needed on Linux.
        options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    }
    options
}

pub fn restate_old_configuration() -> restate_node::config::Configuration {
    let meta_options = restate_node::MetaOptionsBuilder::default()
        .schema_storage_path(tempfile::tempdir().expect("tempdir failed").into_path())
        .build()
        .expect("building meta options should work");

    let rocksdb_options = restate_node::RocksdbOptionsBuilder::default()
        .path(tempfile::tempdir().expect("tempdir failed").into_path())
        .build()
        .expect("building rocksdb options should work");

    let worker_options = restate_node::WorkerOptionsBuilder::default()
        .partitions(10)
        .storage_rocksdb(rocksdb_options)
        .build()
        .expect("building worker options should work");

    let admin_options = restate_node::AdminOptionsBuilder::default()
        .meta(meta_options)
        .build()
        .expect("building admin options should work");

    let node_options = restate_node::NodeOptionsBuilder::default()
        .worker(worker_options)
        .admin(admin_options)
        .build()
        .expect("building the configuration should work");

    let config = restate_node::config::ConfigurationBuilder::default()
        .node(node_options)
        .build()
        .expect("building the configuration should work");

    restate_node::config::Configuration::load_with_default(
        config,
        None,
        CommonOptionCliOverride::default(),
    )
    .expect("configuration loading should not fail")
}

pub fn restate_configuration() -> Configuration {
    let common_options = CommonOptionsBuilder::default()
        .base_dir(tempfile::tempdir().expect("tempdir failed").into_path())
        .build()
        .expect("building common options should work");

    let worker_options = WorkerOptionsBuilder::default()
        .bootstrap_num_partitions(10)
        .build()
        .expect("building worker options should work");

    let config = ConfigurationBuilder::default()
        .common(common_options)
        .worker(worker_options)
        .build()
        .expect("building the configuration should work");

    ConfigLoaderBuilder::default()
        .load_env(true)
        .custom_default(config)
        .build()
        .expect("builder should build")
        .load_once()
        .expect("configuration loading should not fail")
}

pub struct BenchmarkSettings {
    pub num_requests: u32,
    pub num_parallel_requests: usize,
    pub sample_size: usize,
}

pub fn parse_benchmark_settings() -> BenchmarkSettings {
    let num_requests = std::env::var("BENCHMARK_REQUESTS")
        .ok()
        .and_then(|requests| requests.parse().ok())
        .unwrap_or(4000);
    let num_parallel_requests = std::env::var("BENCHMARK_PARALLEL_REQUESTS")
        .ok()
        .and_then(|parallel_requests| parallel_requests.parse().ok())
        .unwrap_or(1000);
    let sample_size = std::env::var("BENCHMARK_SAMPLE_SIZE")
        .ok()
        .and_then(|parallel_requests| parallel_requests.parse().ok())
        .unwrap_or(20);

    BenchmarkSettings {
        num_requests,
        num_parallel_requests,
        sample_size,
    }
}
