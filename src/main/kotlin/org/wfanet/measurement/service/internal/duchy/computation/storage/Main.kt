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

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.db.duchy.ProtocolStageEnumHelper
import org.wfanet.measurement.db.duchy.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.db.duchy.SingleProtocolDatabase
import org.wfanet.measurement.db.duchy.gcp.ComputationMutations
import org.wfanet.measurement.db.duchy.gcp.GcpSpannerComputationsDb
import org.wfanet.measurement.db.duchy.gcp.GcpSpannerReadOnlyComputationsRelationalDb
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import picocli.CommandLine
import kotlin.properties.Delegates

class GcpSingleProtocolDatabase(
  private val reader: GcpSpannerReadOnlyComputationsRelationalDb,
  private val writer: GcpSpannerComputationsDb<ComputationStage, ComputationStageDetails>,
  private val protocolStageEnumHelper: ProtocolStageEnumHelper<ComputationStage>,
  override val computationType: ComputationType
) : SingleProtocolDatabase,
  ReadOnlyComputationsRelationalDb by reader,
  ComputationsRelationalDb<ComputationStage> by writer,
  ProtocolStageEnumHelper<ComputationStage> by protocolStageEnumHelper

/** Creates a new Liquid Legions based spanner database client. */
fun newLiquidLegionsProtocolGapDatabaseClient(
  spanner: SpannerFromFlags,
  duchyOrder: DuchyOrder,
  duchyName: String
): GcpSingleProtocolDatabase =
  GcpSingleProtocolDatabase(
    reader = GcpSpannerReadOnlyComputationsRelationalDb(
      databaseClient = spanner.databaseClient,
      computationStagesHelper = LiquidLegionsSketchAggregationProtocol.ComputationStages
    ),
    writer = GcpSpannerComputationsDb(
      databaseClient = spanner.databaseClient,
      duchyName = duchyName,
      duchyOrder = duchyOrder,
      computationMutations = ComputationMutations(
        LiquidLegionsSketchAggregationProtocol.ComputationStages,
        LiquidLegionsSketchAggregationProtocol.ComputationStages.Details(listOf())
      )
    ),
    protocolStageEnumHelper = LiquidLegionsSketchAggregationProtocol.ComputationStages,
    computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
  )

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

  @set:CommandLine.Option(
    names = ["--duchy-name"],
    description = ["Name of the duchy where the server is running."],
    required = true
  )
  var duchyName by Delegates.notNull<String>()
    private set
}

@CommandLine.Command(
  name = "gcp_computation_storage_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin computationStorageServiceFlags: ComputationStorageServiceFlags,
  @CommandLine.Mixin spannerFlags: SpannerFromFlags.Flags
) {
  // Currently this server only accommodates the Liquid Legions protocol running on a GCP
  // instance. For a new cloud platform there would need to be an instance of
  // [SingleProtocolDatabase] which can interact with the database offerings of that cloud.
  // For a new protocol there would need to be implementations of [ProtocolStageEnumHelper]
  // for the new computation protocol.
  val gcpDatabaseClient = newLiquidLegionsProtocolGapDatabaseClient(
    spanner = SpannerFromFlags(spannerFlags),
    // TODO: Define other duchies via command line flags.
    duchyOrder = DuchyOrder(setOf()),
    duchyName = computationStorageServiceFlags.duchyName
  )
  CommonServer(
    computationStorageServiceFlags.nameForLogging,
    computationStorageServiceFlags.port,
    ComputationStorageServiceImpl(gcpDatabaseClient)
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
