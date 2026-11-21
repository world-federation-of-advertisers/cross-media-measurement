// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.common.truth.Truth.assertThat
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.population
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VersionedCommon
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VersionedEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.versionedCommon
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey

abstract class AbstractMeasurementConsumerSimulatorTest {

  protected val kingdomApiThrottler = RecordingThrottler()

  protected val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }

  protected val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()

  protected val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }

  protected val measurementsServiceMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { createMeasurement(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<CreateMeasurementRequest>(0)
        request.measurement.copy { name = "$MC_NAME/measurements/m1" }
      }
  }

  protected val certificatesServiceMock: CertificatesCoroutineImplBase = mockService()
  protected val modelLinesServiceMock: ModelLinesCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersServiceMock)
    addService(eventGroupsServiceMock)
    addService(dataProvidersServiceMock)
    addService(measurementsServiceMock)
    addService(certificatesServiceMock)
    addService(modelLinesServiceMock)
  }

  protected class ShortCircuitException : RuntimeException("short-circuit")

  protected abstract fun createSimulator(
    eventGroupRefIds: List<String>
  ): MeasurementConsumerSimulator

  protected abstract fun createPopulationSimulator(): MeasurementConsumerSimulator

  protected fun stubEventGroups(refIds: List<String>) {
    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups +=
              refIds.map { refId ->
                eventGroup {
                  name = "$DATA_PROVIDER_NAME/eventGroups/$refId"
                  measurementConsumer = MC_NAME
                  eventGroupReferenceId = refId
                  dataAvailabilityInterval = DATA_INTERVAL
                }
              }
          }
        )
    }
  }

  protected fun decryptAndParseRequisitionSpec(
    encryptedRequisitionSpec: EncryptedMessage
  ): RequisitionSpec {
    val signedRequisitionSpec = decryptRequisitionSpec(encryptedRequisitionSpec, EDP_PRIVATE_KEY)
    return RequisitionSpec.parseFrom(signedRequisitionSpec.data)
  }

  @Test
  fun `testDirectReachAndFrequency throttles every Kingdom API request`() {
    stubEventGroups(listOf("sim-eg-ref-1"))

    val simulator = createSimulator(listOf("sim-eg-ref-1"))
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    assertThat(kingdomApiThrottler.invocationCount).isEqualTo(4)
  }

  @Test
  fun `buildRequisitionInfo accumulates all event groups from same data provider`() {
    val refIds = listOf("sim-eg-ref-1", "sim-eg-ref-2", "sim-eg-ref-3")
    stubEventGroups(refIds)

    val simulator = createSimulator(refIds)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }

    assertThat(captor.firstValue.measurement.dataProvidersList).hasSize(1)
    val entry = captor.firstValue.measurement.dataProvidersList[0]
    val requisitionSpec = decryptAndParseRequisitionSpec(entry.value.encryptedRequisitionSpec)
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(3)
    assertThat(requisitionSpec.events.eventGroupsList.map { it.key })
      .containsExactly(
        "$DATA_PROVIDER_NAME/eventGroups/sim-eg-ref-1",
        "$DATA_PROVIDER_NAME/eventGroups/sim-eg-ref-2",
        "$DATA_PROVIDER_NAME/eventGroups/sim-eg-ref-3",
      )
  }

  @Test
  fun `buildRequisitionInfo produces single entry for single event group`() {
    val refIds = listOf("sim-eg-ref-1")
    stubEventGroups(refIds)

    val simulator = createSimulator(refIds)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(1)
  }

  @Test
  fun `buildRequisitionInfo applies eventGroupFilter to further filter event groups`() {
    val refIds = listOf("sim-eg-wanted", "sim-eg-unwanted")
    stubEventGroups(refIds)

    val simulator = createSimulator(refIds)
    assertFailsWith<ShortCircuitException> {
      runBlocking {
        simulator.testDirectReachAndFrequency(
          "run1",
          1,
          eventGroupFilter = { it.eventGroupReferenceId == "sim-eg-wanted" },
        )
      }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(1)
    assertThat(requisitionSpec.events.eventGroupsList[0].key)
      .isEqualTo("$DATA_PROVIDER_NAME/eventGroups/sim-eg-wanted")
  }

  private fun stubPopulationResult(repositoryVersion: Int?) {
    modelLinesServiceMock.stub {
      onBlocking { getModelLine(any()) }
        .thenReturn(
          modelLine {
            name = POPULATION_MODEL_LINE
            if (repositoryVersion != null) eventTemplateVersion = repositoryVersion
          }
        )
    }
    certificatesServiceMock.stub {
      onBlocking { getCertificate(any()) }
        .thenReturn(
          certificate {
            name = DATA_PROVIDER.certificate
            x509Der = SECRETS_DIR.resolve("mc_cs_cert.der").readByteString()
          }
        )
    }
    val encryptedResult =
      encryptResult(
        signResult(result { population = population { value = 100 } }, MC_SIGNING_KEY),
        MC_ENCRYPTION_PUBLIC_KEY,
      )
    measurementsServiceMock.stub {
      onBlocking { getMeasurement(any()) }
        .thenReturn(
          measurement {
            name = "$MC_NAME/measurements/m1"
            state = Measurement.State.SUCCEEDED
            results += resultOutput {
              certificate = DATA_PROVIDER.certificate
              this.encryptedResult = encryptedResult
            }
          }
        )
    }
  }

  private suspend fun testVersionedPopulation(
    configuredVersion: Int?,
    specification: PopulationSpec,
    filter: String,
  ) {
    createPopulationSimulator()
      .testPopulation(
        "population-version-regression",
        PopulationData(DATA_PROVIDER_NAME, specification),
        POPULATION_MODEL_LINE,
        filter,
        VersionedEvent.getDescriptor(),
        configuredVersion,
      )
  }

  @Test
  fun `population expectation preserves historical version across backfill and config removal`() =
    runBlocking {
      for ((repositoryVersion, configuredVersion) in listOf(null to 1, 1 to 1, 1 to null)) {
        clearInvocations(modelLinesServiceMock)
        val priorThrottlerCalls = kingdomApiThrottler.invocationCount
        stubPopulationResult(repositoryVersion)
        testVersionedPopulation(
          configuredVersion,
          HISTORICAL_POPULATION,
          "versioned_common.retired == 2",
        )
        verifyBlocking(modelLinesServiceMock) {
          getModelLine(getModelLineRequest { name = POPULATION_MODEL_LINE })
        }
        assertThat(kingdomApiThrottler.invocationCount).isEqualTo(priorThrottlerCalls + 6)
      }
    }

  @Test
  fun `population expectation uses newer model-line version for added field`() = runBlocking {
    stubPopulationResult(3)
    val specification = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 100
        }
        attributes +=
          versionedCommon {
              original = VersionedCommon.Segment.SEGMENT_A
              later = VersionedCommon.Segment.SEGMENT_B
            }
            .pack()
      }
    }
    testVersionedPopulation(null, specification, "versioned_common.later == 2")
  }

  @Test
  fun `population expectation rejects conflicting repository and configured versions`() =
    runBlocking {
      stubPopulationResult(3)
      val error =
        assertFailsWith<IllegalArgumentException> {
          testVersionedPopulation(1, HISTORICAL_POPULATION, "versioned_common.retired == 2")
        }
      assertThat(error.message).contains("does not match")
    }

  @Test
  fun `population expectation rejects missing repository and configured versions`() = runBlocking {
    stubPopulationResult(null)
    val error =
      assertFailsWith<IllegalArgumentException> {
        testVersionedPopulation(null, HISTORICAL_POPULATION, "versioned_common.retired == 2")
      }
    assertThat(error.message).contains("missing")
  }

  @Test
  fun `population expectation does not fall back on repository read failure`() = runBlocking {
    stubPopulationResult(null)
    modelLinesServiceMock.stub {
      onBlocking { getModelLine(any()) }.thenThrow(Status.UNAVAILABLE.asRuntimeException())
    }
    val error =
      assertFailsWith<StatusException> {
        testVersionedPopulation(1, HISTORICAL_POPULATION, "versioned_common.retired == 2")
      }
    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  protected class RecordingThrottler : Throttler {
    private val invocationCounter = AtomicInteger()

    val invocationCount: Int
      get() = invocationCounter.get()

    override suspend fun <T> onReady(block: suspend () -> T): T {
      invocationCounter.incrementAndGet()
      return block()
    }
  }

  companion object {
    private val SECRETS_DIR =
      checkNotNull(
          getRuntimePath(
            Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
          )
        )
        .toFile()

    private const val POPULATION_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private val HISTORICAL_POPULATION = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 100
        }
        attributes +=
          versionedCommon {
              original = VersionedCommon.Segment.SEGMENT_A
              retired = VersionedCommon.Segment.SEGMENT_B
            }
            .pack()
      }
    }
    private val MC_ROOT = readCertificate(SECRETS_DIR.resolve("mc_root.pem"))
    val TRUSTED_CERTIFICATES = mapOf(requireNotNull(MC_ROOT.authorityKeyIdentifier) to MC_ROOT)

    const val MC_NAME = "measurementConsumers/mc1"
    private const val MC_CERT_NAME = "$MC_NAME/certificates/cert1"
    const val DATA_PROVIDER_NAME = "dataProviders/dp1"

    val MC_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("mc_cs_cert.der"),
        SECRETS_DIR.resolve("mc_cs_private.der"),
      )

    private val MC_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRETS_DIR.resolve("mc_enc_public.tink").readByteString()
    }

    private val EDP_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRETS_DIR.resolve("edp1_enc_public.tink").readByteString()
    }

    val EDP_PRIVATE_KEY = loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink"))

    val MC_DATA =
      MeasurementConsumerData(
        name = MC_NAME,
        signingKey = MC_SIGNING_KEY,
        encryptionKey = loadPrivateKey(SECRETS_DIR.resolve("mc_enc_private.tink")),
        apiAuthenticationKey = "fake-api-key",
      )

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    val MEASUREMENT_CONSUMER = measurementConsumer {
      name = MC_NAME
      certificate = MC_CERT_NAME
      publicKey = signEncryptionPublicKey(MC_ENCRYPTION_PUBLIC_KEY, MC_SIGNING_KEY)
    }

    val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = "$DATA_PROVIDER_NAME/certificates/cert1"
      publicKey = signEncryptionPublicKey(EDP_ENCRYPTION_PUBLIC_KEY, MC_SIGNING_KEY)
    }

    private val DATA_START = LocalDate.of(2021, 3, 15)
    private val DATA_END = LocalDate.of(2021, 3, 22)
    val DATA_INTERVAL: Interval = interval {
      startTime = DATA_START.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
      endTime = DATA_END.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
    }
  }
}
