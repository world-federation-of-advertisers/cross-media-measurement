// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.deploy.gcloud.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.deploy.common.server.postgres.PostgresDuchyDataServer
import org.wfanet.measurement.storage.forwarded.ForwardedStorageFromFlags
import picocli.CommandLine

/** Implementation of [PostgresDuchyDataServer] using Fake Storage Service. */
class ForwardedStoragePostgresDuchyDataServer : PostgresDuchyDataServer() {

  @CommandLine.Mixin private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  override fun run() {
    run(ForwardedStorageFromFlags(forwardedStorageFlags, serverFlags.tlsFlags).storageClient)
  }
}

fun main(args: Array<String>) = commandLineMain(ForwardedStoragePostgresDuchyDataServer(), args)
