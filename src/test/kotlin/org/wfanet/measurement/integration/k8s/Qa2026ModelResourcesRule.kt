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

package org.wfanet.measurement.integration.k8s

import com.google.common.hash.Hashing
import io.grpc.ManagedChannel
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
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
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate

/**
 * JUnit [TestRule] that ensures the QA 2026 [Population] exists and that the QA 2026 [ModelLine]
 * has a [ModelRelease] and rollout pointing at it.
 *
 * The ModelLine itself is **not** created here — nothing in this repository creates a ModelLine, so
 * it is provisioned once per environment by an operator and looked up by resource name. Because
 * `model-lines create` requires a Population up front, that line is bootstrapped against whatever
 * Population is convenient; this rule then attaches the correct one. Since the PDP resolves the
 * *most recent* rollout on a line, the release created here supersedes the bootstrap.
 *
 * This is the same self-provisioning pattern `SyntheticGeneratorCorrectnessTest` uses: a suite
 * accumulates ModelReleases as populations change, and a line accumulates rollouts, with the newest
 * live. It uses only public v2alpha calls; nothing in the production path is special-cased.
 *
 * The Population is keyed by a hash of the serialized spec, so re-runs reuse it and a changed spec
 * provisions a new one automatically.
 *
 * The rule is a **no-op** unless a QA 2026 model line is configured, leaving dev and head runs
 * untouched until their `QA2026_MODEL_LINE` is set.
 *
 * @property populationSpecProvider yields the QA 2026 synthetic population spec
 * @property populationDataProvider resource name of the PDP that owns the Population
 * @property modelLineName resource name of the QA 2026 ModelLine, or empty to disable
 * @property earliestEventDateProvider yields the first date the dataset has events for, which the
 *   ModelLine must already be active on
 * @property kingdomPublicApiTarget Kingdom public API target
 * @property kingdomPublicApiCertHost expected DNS-ID in the Kingdom's TLS certificate
 */
class Qa2026ModelResourcesRule(
  private val populationSpecProvider: () -> PopulationSpec,
  private val populationDataProvider: String,
  private val modelLineName: String,
  private val earliestEventDateProvider: () -> LocalDate,
  private val kingdomPublicApiTarget: String,
  private val kingdomPublicApiCertHost: String?,
) : TestRule {

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        if (modelLineName.isEmpty()) {
          logger.info("No QA 2026 model line configured; skipping QA 2026 model resources.")
        } else if (populationDataProvider.isEmpty()) {
          logger.warning(
            "No population_data_provider configured; skipping QA 2026 model resources."
          )
        } else {
          provision()
        }
        base.evaluate()
      }
    }
  }

  @Blocking
  private fun provision() {
    val pdpChannel: ManagedChannel = buildChannel(PDP_CERT_FILE, PDP_KEY_FILE)
    val mpChannel: ManagedChannel = buildChannel(MP_CERT_FILE, MP_KEY_FILE)
    try {
      val qa2026Population = ensurePopulation(pdpChannel)
      logger.info("QA 2026 Population: ${qa2026Population.name}")

      val modelLine =
        try {
          ModelLinesGrpc.newBlockingStub(mpChannel)
            .getModelLine(getModelLineRequest { name = modelLineName })
        } catch (e: StatusRuntimeException) {
          throw Exception(
            "QA2026_MODEL_LINE '$modelLineName' not found. Provision it with the ModelRepository " +
              "tool before enabling the QA 2026 dataset in this environment.",
            e,
          )
        }
      val activeStart = modelLine.activeStartTime.toInstant()

      // A line active after the first event date silently drops the events before it, so fail
      // rather than report on a subset.
      val earliestEventDate = earliestEventDateProvider()
      check(activeStart <= earliestEventDate.atStartOfDay(ZoneOffset.UTC).toInstant()) {
        "ModelLine $modelLineName is active from $activeStart, after the earliest QA 2026 event " +
          "date $earliestEventDate"
      }

      // The VID labeling dispatcher selects lines purely by active window, so one reaching back to
      // the 2021 fixture would also be dispatched for its upload and never finish.
      val minActiveStart = FIXTURE_2021_LAST_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC)
      check(activeStart >= minActiveStart.toInstant()) {
        "ModelLine $modelLineName is active from $activeStart, which overlaps the 2021 fixture " +
          "ending $FIXTURE_2021_LAST_DATE. It must start at or after $minActiveStart."
      }

      ensureModelRelease(mpChannel, qa2026Population, modelLine)
    } finally {
      pdpChannel.shutdown()
      mpChannel.shutdown()
    }
  }

  /**
   * Returns the [Population] for the QA 2026 spec, creating it if absent.
   *
   * The request ID is a hash of the serialized spec, so repeated runs reuse the same Population and
   * a changed spec yields a new one. Protobuf serialization is not guaranteed deterministic across
   * builds, which is sufficient here: a spurious duplicate is harmless, since the newest rollout
   * wins regardless.
   */
  @Blocking
  private fun ensurePopulation(pdpChannel: ManagedChannel): Population {
    val spec = populationSpecProvider()
    @OptIn(ExperimentalStdlibApi::class) // For `HexFormat`.
    val requestId =
      Hashing.murmur3_128().hashBytes(spec.toByteArray()).asBytes().toHexString(HexFormat.Default)
    return PopulationsGrpc.newBlockingStub(pdpChannel)
      .createPopulation(
        createPopulationRequest {
          parent = populationDataProvider
          population = population {
            populationSpec = spec
            description = POPULATION_DESCRIPTION
          }
          this.requestId = requestId
        }
      )
  }

  /**
   * Ensures a [ModelRelease] exists for [population] and is rolled out on [modelLine].
   *
   * Mirrors `SyntheticGeneratorCorrectnessTest.ensureModelRelease`. Rollouts on a line share the
   * line's active start date, so the PDP breaks the tie on create time and this rollout supersedes
   * the line's bootstrap.
   */
  @Blocking
  private fun ensureModelRelease(
    mpChannel: ManagedChannel,
    population: Population,
    modelLine: ModelLine,
  ) {
    val modelSuiteKey = checkNotNull(ModelLineKey.fromName(modelLine.name)).parentKey
    val modelSuiteName = modelSuiteKey.toName()
    val modelProviderName = "modelProviders/${modelSuiteKey.modelProviderId}"
    val modelReleasesStub = ModelReleasesGrpc.newBlockingStub(mpChannel)

    val existing: List<ModelRelease> =
      modelReleasesStub
        .listModelReleases(
          listModelReleasesRequest {
            parent = "$modelProviderName/modelSuites/-"
            filter = ListModelReleasesRequestKt.filter { populationIn += population.name }
          }
        )
        .modelReleasesList
    if (existing.isNotEmpty()) {
      // A release exists, but a previous run may have died before rolling it out, which would
      // silently leave the line resolving to whatever Population it was bootstrapped against.
      val release = existing.first()
      val rollouts =
        ModelRolloutsGrpc.newBlockingStub(mpChannel)
          .listModelRollouts(
            listModelRolloutsRequest {
              parent = modelLine.name
              filter = ListModelRolloutsRequestKt.filter { modelReleaseIn += release.name }
            }
          )
          .modelRolloutsList
      if (rollouts.isEmpty()) {
        createRollout(mpChannel, modelLine, release.name)
        logger.info("Rolled out pre-existing ${release.name} on ${modelLine.name}")
      } else {
        logger.info("Found ${release.name} rolled out on ${modelLine.name}")
      }
      return
    }

    val release: ModelRelease =
      modelReleasesStub.createModelRelease(
        createModelReleaseRequest {
          parent = modelSuiteName
          modelRelease = modelRelease { this.population = population.name }
        }
      )
    createRollout(mpChannel, modelLine, release.name)
    logger.info("Created ${release.name} and rolled it out on ${modelLine.name}")
  }

  /**
   * Rolls [modelReleaseName] out on [modelLine], dated at the line's active start.
   *
   * Rollouts on a line therefore share a date, and the PDP breaks the tie on create time — so the
   * most recently created rollout is the live one.
   */
  @Blocking
  private fun createRollout(
    mpChannel: ManagedChannel,
    modelLine: ModelLine,
    modelReleaseName: String,
  ) {
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
            this.modelRelease = modelReleaseName
          }
        }
      )
  }

  private fun buildChannel(certFileName: String, privateKeyFileName: String): ManagedChannel {
    val secretFiles: Path =
      checkNotNull(getRuntimePath(WORKSPACE_PATH.resolve(SECRET_FILES_PATH))) {
        "secretfiles runtime path not found"
      }
    val signingCerts =
      SigningCerts.fromPemFiles(
        secretFiles.resolve(certFileName).toFile(),
        secretFiles.resolve(privateKeyFileName).toFile(),
        secretFiles.resolve(KINGDOM_TRUSTED_CERTS_FILE).toFile(),
      )
    return buildMutualTlsChannel(kingdomPublicApiTarget, signingCerts, kingdomPublicApiCertHost)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")
    private const val KINGDOM_TRUSTED_CERTS_FILE = "kingdom_root.pem"
    private const val PDP_CERT_FILE = "pdp1_tls.pem"
    private const val PDP_KEY_FILE = "pdp1_tls.key"
    private const val MP_CERT_FILE = "mp1_tls.pem"
    private const val MP_KEY_FILE = "mp1_tls.key"

    private const val POPULATION_DESCRIPTION = "QA 2026 synthetic population"

    /** Last date of the 2021 fixture, which the QA 2026 line must stay clear of. */
    private val FIXTURE_2021_LAST_DATE: LocalDate = LocalDate.of(2021, 3, 21)
  }
}
