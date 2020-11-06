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

package org.wfanet.measurement.duchy.deploy.gcloud.server

import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.duchy.deploy.common.server.ComputationsServer
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.ComputationMutations
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerComputationsDb
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.GcpSpannerReadOnlyComputationsRelationalDb
import org.wfanet.measurement.duchy.toDuchyOrder
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import picocli.CommandLine

/**
 * Implementation of [ComputationsServer] using Google Cloud Spanner and the
 * [Liquid Legions sketch aggregation][ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1]
 * protocol.
 */
@CommandLine.Command(
  name = "SpannerLiquidLegionsComputationsServer",
  description = ["Server daemon for ${ComputationsServer.SERVICE_NAME} service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class SpannerLiquidLegionsComputationsServer : ComputationsServer() {
  @CommandLine.Mixin
  private lateinit var spannerFlags: SpannerFlags

  private val latestDuchyPublicKeys: DuchyPublicKeys.Entry
    get() = duchyPublicKeys.latest
  private val otherDuchyNames: List<String>
    get() = latestDuchyPublicKeys.keys.filter { it != flags.duchy.duchyName }

  override val computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
  override val stageEnumHelper = LiquidLegionsSketchAggregationProtocol.ComputationStages
  override val stageDetails
    get() = LiquidLegionsSketchAggregationProtocol.ComputationStages.Details(otherDuchyNames)

  override fun run() = runBlocking {
    spannerFlags.usingSpanner { spanner ->
      val databaseClient = spanner.databaseClient
      run(
        GcpSpannerReadOnlyComputationsRelationalDb(databaseClient, stageEnumHelper),
        GcpSpannerComputationsDb(
          databaseClient = databaseClient,
          duchyName = flags.duchy.duchyName,
          duchyOrder = latestDuchyPublicKeys.toDuchyOrder(),
          computationMutations = ComputationMutations(stageEnumHelper, stageDetails)
        )
      )
    }
  }
}

fun main(args: Array<String>) =
  commandLineMain(SpannerLiquidLegionsComputationsServer(), args)
