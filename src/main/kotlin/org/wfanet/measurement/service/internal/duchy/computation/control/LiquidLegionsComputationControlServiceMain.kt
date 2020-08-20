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

package org.wfanet.measurement.service.internal.duchy.computation.control

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlin.properties.Delegates
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.duchy.computation.gcp.newLiquidLegionsSketchAggregationGcpComputationStorageClients
import org.wfanet.measurement.db.gcp.GoogleCloudStorageFromFlags
import picocli.CommandLine

private class ComputationControlServiceFlags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port: Int by Delegates.notNull()
    private set

  @set:CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "WorkerServer"
  )
  var nameForLogging: String by Delegates.notNull()
  private set

  @set:CommandLine.Option(
    names = ["--computation-storage-service-target"],
    required = true
  )
  var computationStorageServiceTarget: String by Delegates.notNull()
    private set
}

@CommandLine.Command(
  name = "gcp_worker_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin computationControlServiceFlags: ComputationControlServiceFlags,
  @CommandLine.Mixin cloudStorageFlags: GoogleCloudStorageFromFlags.Flags
) {
  // TODO: Expand flags and configuration to work on other cloud environments when available.
  val cloudStorageFromFlags = GoogleCloudStorageFromFlags(cloudStorageFlags)

  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(computationControlServiceFlags.computationStorageServiceTarget)
      .usePlaintext()
      .build()

  val storageClients = newLiquidLegionsSketchAggregationGcpComputationStorageClients(
    duchyName = computationControlServiceFlags.nameForLogging,
    // TODO: Pass public keys of all duchies to the computation manager
    duchyPublicKeys = mapOf(),
    googleCloudStorageOptions = cloudStorageFromFlags.cloudStorageOptions,
    storageBucket = cloudStorageFromFlags.bucket,
    computationStorageServiceChannel = channel
  )

  CommonServer(
    computationControlServiceFlags.nameForLogging,
    computationControlServiceFlags.port,
    LiquidLegionsComputationControlServiceImpl(storageClients)
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
