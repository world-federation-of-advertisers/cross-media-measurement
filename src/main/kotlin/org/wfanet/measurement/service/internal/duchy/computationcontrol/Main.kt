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

package org.wfanet.measurement.service.internal.duchy.computationcontrol

import kotlin.properties.Delegates
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.duchy.gcp.newCascadingLegionsSketchAggregationGcpComputationManager
import org.wfanet.measurement.db.gcp.GoogleCloudStorageFromFlags
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import picocli.CommandLine

private class ComputationControlServiceFlags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "WorkerServer"
  )
  lateinit var nameForLogging: String
    private set
}

@ExperimentalCoroutinesApi
@CommandLine.Command(
  name = "gcp_worker_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin computationControlServiceFlags: ComputationControlServiceFlags,
  @CommandLine.Mixin spannerFlags: SpannerFromFlags.Flags,
  @CommandLine.Mixin cloudStorageFlags: GoogleCloudStorageFromFlags.Flags
) {
  // TODO: Expand flags and configuration to work on other cloud environments when available.
  val spannerFromFlags = SpannerFromFlags(spannerFlags)
  val cloudStorageFromFlags = GoogleCloudStorageFromFlags(cloudStorageFlags)

  val computationManager = newCascadingLegionsSketchAggregationGcpComputationManager(
    duchyName = computationControlServiceFlags.nameForLogging,
    // TODO: Pass public keys of all duchies to the computation manager
    duchyPublicKeys = mapOf(),
    databaseClient = spannerFromFlags.databaseClient,
    googleCloudStorageOptions = cloudStorageFromFlags.cloudStorageOptions,
    storageBucket = cloudStorageFromFlags.bucket
  )

  CommonServer(
    computationControlServiceFlags.nameForLogging,
    computationControlServiceFlags.port,
    ComputationControlServiceImpl(computationManager)
  ) .start() .blockUntilShutdown()
}

@ExperimentalCoroutinesApi
fun main(args: Array<String>) = commandLineMain(::run, args)
