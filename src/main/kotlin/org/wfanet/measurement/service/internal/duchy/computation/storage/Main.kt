// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.internal.duchy.computation.storage

import kotlin.properties.Delegates
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

private class ComputationStorageServiceFlags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "ComputationStorageServer"
  )
  var nameForLogging by Delegates.notNull<String>()
    private set
}

@CommandLine.Command(
  name = "gcp_computation_storage_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin computationStorageServiceFlags: ComputationStorageServiceFlags
) {
  CommonServer(
    computationStorageServiceFlags.nameForLogging,
    computationStorageServiceFlags.port,
    ComputationStorageServiceImpl()
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
