// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("node_ctl_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.node", "::restate_types::protobuf::node")
        .extern_path(".restate.common", "::restate_types::protobuf::common")
        .compile_protos(
            &["./protobuf/node_ctl_svc.proto"],
            &["protobuf", "../types/protobuf"],
        )?;

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("core_node_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.node", "::restate_types::protobuf::node")
        .compile_protos(
            &["./protobuf/core_node_svc.proto"],
            &["protobuf", "../types/protobuf"],
        )?;

    Ok(())
}
