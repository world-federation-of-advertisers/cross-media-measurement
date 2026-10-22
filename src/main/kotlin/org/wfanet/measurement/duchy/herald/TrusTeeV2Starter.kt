// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.herald

import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.key
import org.wfanet.measurement.duchy.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.utils.toRequisitionEntries
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.PopulationAttributeValue
import org.wfanet.measurement.internal.duchy.PopulationSpec
import org.wfanet.measurement.internal.duchy.PopulationSpecKt
import org.wfanet.measurement.internal.duchy.Query
import org.wfanet.measurement.internal.duchy.VectorSet
import org.wfanet.measurement.internal.duchy.VidFilter
import org.wfanet.measurement.internal.duchy.VidFilterKt
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.TrusTeeSetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.populationAttributeValue
import org.wfanet.measurement.internal.duchy.populationSpec
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeKt.ComputationDetailsKt.resultMinimumThresholds
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2Kt
import org.wfanet.measurement.internal.duchy.query
import org.wfanet.measurement.internal.duchy.vectorSet
import org.wfanet.measurement.internal.duchy.vidFilter
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.PopulationAttributeValue as SystemPopulationAttributeValue
import org.wfanet.measurement.system.v1alpha.PopulationSpec as SystemPopulationSpec
import org.wfanet.measurement.system.v1alpha.Query as SystemQuery
import org.wfanet.measurement.system.v1alpha.VectorSet as SystemVectorSet
import org.wfanet.measurement.system.v1alpha.VidFilter as SystemVidFilter

object TrusTeeV2Starter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  /** Create a TrusTEE multimeasurement Computation. */
  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    protocolSetupConfig: TrusTeeSetupConfig,
    blobStorageBucket: String,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val role = protocolSetupConfig.role
    require(role == RoleInComputation.AGGREGATOR) { "Invalid role for TrusTEE v2: $role" }

    val apiVersion = Version.fromString(systemComputation.publicApiVersion)
    require(apiVersion == Version.V2_ALPHA) { "Unsupported API version $apiVersion" }
    val measurementSpec = MeasurementSpec.parseFrom(systemComputation.measurementSpec)
    require(measurementSpec.hasMulti()) {
      "Unsupported measurement type for TrusTEE v2: ${measurementSpec.measurementTypeCase}"
    }
    require(systemComputation.mpcProtocolConfig.hasTrusTeeV2()) {
      "Missing TrusTeeV2 in the duchy protocol config."
    }
    require(systemComputation.hasMultimeasurement()) { "Missing multimeasurement" }
    val multimeasurement = systemComputation.multimeasurement

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$duchyId/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      trusTeeV2 =
        TrusTeeV2Kt.computationDetails {
          this.role = role
          queries += multimeasurement.queriesList.map { it.toInternal() }
          populationSpec = multimeasurement.populationSpec.toInternal()
          eventDescriptor =
            TrusTeeV2Kt.eventDescriptor {
              descriptorSet = multimeasurement.eventDescriptor.descriptorSet
              eventMessageType = multimeasurement.eventDescriptor.eventMessageType
            }
          val trusTeeV2Config = systemComputation.mpcProtocolConfig.trusTeeV2
          if (trusTeeV2Config.hasResultMinimumThresholds()) {
            resultMinimumThresholds = resultMinimumThresholds {
              minImpressions = trusTeeV2Config.resultMinimumThresholds.minImpressions
              minUsers = trusTeeV2Config.resultMinimumThresholds.minUsers
            }
          }
        }
    }

    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.TRUS_TEE_V2
        globalComputationId = globalId
        computationStage = Stage.INITIALIZED.toProtocolStage()
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
  }

  /** Start the Computation. */
  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasTrusTeeV2()) { "TrusTEE v2 computation required." }

    val stage = token.computationStage.trusTeeV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          stage = Stage.COMPUTING.toProtocolStage(),
        )
        logger.log(Level.INFO) { "[id=${token.globalComputationId}] Computation starts." }
      }
      // Stage that should not be reached.
      Stage.INITIALIZED ->
        logger.log(Level.WARNING) {
          "[id=${token.globalComputationId}]: Computation is not ready to start. stage=$stage."
        }
      // Log and skip for future Stages.
      Stage.COMPUTING,
      Stage.COMPLETE -> {
        logger.log(Level.WARNING) {
          "[id=${token.globalComputationId}]: Computation has started. Skip start request. stage=$stage."
        }
      }
      Stage.STAGE_UNSPECIFIED,
      Stage.UNRECOGNIZED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage $stage")
      }
    }
  }

  private fun SystemQuery.toInternal(): Query {
    val source = this
    return query {
      id = source.id
      summands = source.summands.toInternal()
      if (source.hasVidFilter()) {
        vidFilter = source.vidFilter.toInternal()
      }
      groupingPaths += source.groupingPathsList
      maxFrequency = source.maxFrequency
    }
  }

  private fun SystemVectorSet.toInternal(): VectorSet {
    val source = this
    return vectorSet { baseVectors += source.baseVectorsList }
  }

  private fun SystemVidFilter.toInternal(): VidFilter {
    val source = this
    return vidFilter {
      for (clause in source.reachClausesList) {
        reachClauses +=
          VidFilterKt.reachClause {
            reachedSets += clause.reachedSetsList.map { it.toInternal() }
            if (clause.hasUnreachedSet()) {
              unreachedSet = clause.unreachedSet.toInternal()
            }
          }
      }
      for ((path, value) in source.attributesMap) {
        attributes[path] = value.toInternal()
      }
    }
  }

  private fun SystemPopulationAttributeValue.toInternal(): PopulationAttributeValue {
    val source = this
    return populationAttributeValue {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (source.selectorCase) {
        SystemPopulationAttributeValue.SelectorCase.ENUM_VALUE -> enumValue = source.enumValue
        SystemPopulationAttributeValue.SelectorCase.SELECTOR_NOT_SET ->
          error("PopulationAttributeValue selector not set")
      }
    }
  }

  private fun SystemPopulationSpec.toInternal(): PopulationSpec {
    val source = this
    return populationSpec {
      for (subpopulation in source.subpopulationsList) {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            attributes += subpopulation.attributesList
            for (range in subpopulation.vidRangesList) {
              vidRanges +=
                PopulationSpecKt.vidRange {
                  startVid = range.startVid
                  endVidInclusive = range.endVidInclusive
                }
            }
          }
      }
    }
  }
}
