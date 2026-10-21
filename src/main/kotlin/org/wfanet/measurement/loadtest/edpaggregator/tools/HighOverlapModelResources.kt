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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.hash.Hashing
import io.grpc.Channel
import io.grpc.StatusRuntimeException
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequestKt
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpc
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpc
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate

/**
 * Ensures the high overlap [Population] exists and that the high overlap [ModelLine] has a
 * [ModelRelease] and rollout pointing at it.
 *
 * The ModelLine itself is not created here: it is provisioned once per environment by an operator
 * and looked up by resource name. Because creating a ModelLine requires a Population up front, that
 * line is bootstrapped against whatever Population is convenient; [provision] then attaches the
 * correct one. Since the PDP resolves the most recent rollout on a line, the release created here
 * supersedes the bootstrap.
 *
 * @param populationSpec the high overlap synthetic population spec
 * @param populationDataProvider resource name of the PDP that owns the Population
 * @param modelLineName resource name of the high overlap ModelLine
 * @param earliestEventDate first date the dataset has events for, which the ModelLine must already
 *   be active on
 * @param pdpChannel channel to the Kingdom public API authenticated as [populationDataProvider]
 * @param mpChannel channel to the Kingdom public API authenticated as the ModelLine's ModelProvider
 */
class HighOverlapModelResources(
  private val populationSpec: PopulationSpec,
  private val populationDataProvider: String,
  private val modelLineName: String,
  private val earliestEventDate: LocalDate,
  private val pdpChannel: Channel,
  private val mpChannel: Channel,
) {

  @Blocking
  fun provision() {
    val highOverlapPopulation = ensurePopulation()
    logger.info("high overlap Population: ${highOverlapPopulation.name}")

    val modelLine =
      try {
        ModelLinesGrpc.newBlockingStub(mpChannel)
          .getModelLine(getModelLineRequest { name = modelLineName })
      } catch (e: StatusRuntimeException) {
        throw IllegalStateException(
          "ModelLine '$modelLineName' not found. Provision it with the ModelRepository tool first.",
          e,
        )
      }
    val activeStart = modelLine.activeStartTime.toInstant()

    // A line active after the first event date silently drops the events before it.
    check(activeStart <= earliestEventDate.atStartOfDay(ZoneOffset.UTC).toInstant()) {
      "ModelLine $modelLineName is active from $activeStart, after the earliest high overlap " +
        "event date $earliestEventDate"
    }

    // The VID labeling dispatcher selects lines purely by active window, so one reaching back to
    // the low overlap data set would also be dispatched for its upload and never finish.
    val minActiveStart = LOW_OVERLAP_LAST_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC)
    check(activeStart >= minActiveStart.toInstant()) {
      "ModelLine $modelLineName is active from $activeStart, which overlaps the low overlap " +
        "data set ending $LOW_OVERLAP_LAST_DATE. It must start at or after $minActiveStart."
    }

    ensureModelRelease(highOverlapPopulation, modelLine)
  }

  /**
   * Returns the [Population] for [populationSpec], creating it if absent.
   *
   * The request ID is a hash of the serialized spec, so repeated runs reuse the same Population and
   * a changed spec yields a new one.
   */
  @Blocking
  private fun ensurePopulation(): Population {
    // Protobuf serialization is not guaranteed deterministic across builds. A spurious duplicate is
    // harmless, since the newest rollout wins regardless.
    @OptIn(ExperimentalStdlibApi::class) // For `HexFormat`.
    val requestId =
      Hashing.murmur3_128()
        .hashBytes(populationSpec.toByteArray())
        .asBytes()
        .toHexString(HexFormat.Default)
    return try {
      PopulationsGrpc.newBlockingStub(pdpChannel)
        .createPopulation(
          createPopulationRequest {
            parent = populationDataProvider
            population = population {
              populationSpec = this@HighOverlapModelResources.populationSpec
              description = POPULATION_DESCRIPTION
            }
            this.requestId = requestId
          }
        )
    } catch (e: StatusRuntimeException) {
      throw IllegalStateException("Unable to create Population for $populationDataProvider", e)
    }
  }

  /** Ensures a [ModelRelease] exists for [population] and is rolled out on [modelLine]. */
  @Blocking
  private fun ensureModelRelease(population: Population, modelLine: ModelLine) {
    val modelSuiteKey = checkNotNull(ModelLineKey.fromName(modelLine.name)).parentKey
    val modelProviderName = "modelProviders/${modelSuiteKey.modelProviderId}"
    val modelReleasesStub = ModelReleasesGrpc.newBlockingStub(mpChannel)

    val existing: List<ModelRelease> =
      try {
        modelReleasesStub
          .listModelReleases(
            listModelReleasesRequest {
              parent = "$modelProviderName/modelSuites/-"
              filter = ListModelReleasesRequestKt.filter { populationIn += population.name }
            }
          )
          .modelReleasesList
      } catch (e: StatusRuntimeException) {
        throw IllegalStateException("Unable to list ModelReleases for ${population.name}", e)
      }
    if (existing.isNotEmpty()) {
      // A previous run may have died before rolling the release out.
      val release = existing.first()
      val rollouts =
        try {
          ModelRolloutsGrpc.newBlockingStub(mpChannel)
            .listModelRollouts(
              listModelRolloutsRequest {
                parent = modelLine.name
                filter = ListModelRolloutsRequestKt.filter { modelReleaseIn += release.name }
              }
            )
            .modelRolloutsList
        } catch (e: StatusRuntimeException) {
          throw IllegalStateException("Unable to list ModelRollouts for ${modelLine.name}", e)
        }
      if (rollouts.isEmpty()) {
        createRollout(modelLine, release.name)
        logger.info("Rolled out pre-existing ${release.name} on ${modelLine.name}")
      } else {
        logger.info("Found ${release.name} rolled out on ${modelLine.name}")
      }
      return
    }

    val release: ModelRelease =
      try {
        modelReleasesStub.createModelRelease(
          createModelReleaseRequest {
            parent = modelSuiteKey.toName()
            modelRelease = modelRelease { this.population = population.name }
          }
        )
      } catch (e: StatusRuntimeException) {
        throw IllegalStateException("Unable to create ModelRelease for ${population.name}", e)
      }
    createRollout(modelLine, release.name)
    logger.info("Created ${release.name} and rolled it out on ${modelLine.name}")
  }

  /**
   * Rolls [modelReleaseName] out on [modelLine], dated at the line's active start.
   *
   * Rollouts on a line therefore share a date, and the PDP breaks the tie on create time, so the
   * most recently created rollout is the live one.
   */
  @Blocking
  private fun createRollout(modelLine: ModelLine, modelReleaseName: String) {
    try {
      ModelRolloutsGrpc.newBlockingStub(mpChannel)
        .createModelRollout(
          createModelRolloutRequest {
            parent = modelLine.name
            modelRollout = modelRollout {
              instantRolloutDate =
                modelLine.activeStartTime
                  .toInstant()
                  .atZone(ZoneOffset.UTC)
                  .toLocalDate()
                  .toProtoDate()
              modelRelease = modelReleaseName
            }
          }
        )
    } catch (e: StatusRuntimeException) {
      throw IllegalStateException("Unable to roll out $modelReleaseName on ${modelLine.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val POPULATION_DESCRIPTION = "high overlap synthetic population"

    /** Last date of the low overlap data set, which the high overlap line must stay clear of. */
    private val LOW_OVERLAP_LAST_DATE: LocalDate = LocalDate.of(2021, 3, 21)
  }
}
