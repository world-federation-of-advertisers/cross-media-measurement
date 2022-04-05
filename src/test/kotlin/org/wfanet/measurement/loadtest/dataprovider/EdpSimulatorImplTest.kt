// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchConfig.ValueSpec.Aggregator
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"
private const val MC_NAME = "mc"
private val EVENT_TEMPLATES_TO_FILTERS_MAP =
  mapOf("$TEMPLATE_PREFIX.TestVideoTemplate" to "age.value == 1")
private const val EDP_DISPLAY_NAME = "edp1"
private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private const val EDP_NAME = "dataProviders/someDataProvider"

private const val LLV2_DECAY_RATE = 12.0
private const val LLV2_MAX_SIZE = 100_000L
private const val MAX_FREQUENCY = 10

private val SKETCH_CONFIG =
  SketchConfig.newBuilder()
    .apply {
      addIndexesBuilder().apply {
        name = "Index"
        distributionBuilder.exponentialBuilder.apply {
          rate = LLV2_DECAY_RATE
          numValues = LLV2_MAX_SIZE
        }
      }
      addValuesBuilder().apply {
        name = "SamplingIndicator"
        aggregator = Aggregator.UNIQUE
        distributionBuilder.uniformBuilder.apply {
          numValues = 10_000_000 // 10M
        }
      }
      addValuesBuilder().apply {
        name = "Frequency"
        aggregator = Aggregator.SUM
        distributionBuilder.oracleBuilder.apply { key = "frequency" }
      }
    }
    .build()
private val LIQUID_LEGIONS_V2_PROTOCOL_CONFIG = liquidLegionsV2 {
  sketchParams = liquidLegionsSketchParams {
    decayRate = LLV2_DECAY_RATE
    maxSize = LLV2_MAX_SIZE
  }
  maximumFrequency = MAX_FREQUENCY
}

@RunWith(JUnit4::class)
class EdpSimulatorImplTest {
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService()
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {}
  private val requisitionFulfillmentServiceMock: RequisitionFulfillmentCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(certificatesServiceMock)
    addService(eventGroupsServiceMock)
    addService(requisitionsServiceMock)
    addService(requisitionFulfillmentServiceMock)
  }

  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var edpSimulator: EdpSimulator

  fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
    return loadSigningKey(
      SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
      SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile()
    )
  }
  fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
    return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
  }

  fun loadEncryptionPublicKey(fileName: String): TinkPublicKeyHandle {
    return loadPublicKey(SECRET_FILES_PATH.resolve(fileName).toFile())
  }



  @Test
  fun `filter events and generate sketch successfully`() = runBlocking {
    edpSimulator =
      EdpSimulator(
        EdpData(
          EDP_NAME,
          EDP_DISPLAY_NAME,
          loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
          loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
        ),
        MC_NAME,
        certificatesStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        sketchStore,
        RandomEventQuery(SketchGenerationParams(reach = 10, universeSize = 10_000)),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        EVENT_TEMPLATES_TO_FILTERS_MAP.keys.toList()
      )

    val result: AnySketch =
      SketchProtos.toAnySketch(
        edpSimulator.generateSketch(
          SKETCH_CONFIG,
          eventFilter {
            expression = "age.value == 1"
          },
          0.1.toFloat(),
          0.2.toFloat()
        )
      )
    println(result.toList())

    // assertThat(frontendSimulator.getExpectedResult("foo", LIQUID_LEGIONS_V2_PROTOCOL_CONFIG))
    //   .isEqualTo(
    //     Measurement.Result.newBuilder()
    //       .apply {
    //         reachBuilder.value = 9
    //         frequencyBuilder.apply {
    //           putRelativeFrequencyDistribution(1, 2.0 / 3) // 1,2,6,7
    //           putRelativeFrequencyDistribution(2, 1.0 / 3) // 4,5
    //         }
    //       }
    //       .build()
    //   )
    println("I WORKED!!!!")
  }

  companion object {

    @JvmField @ClassRule val temporaryFolder: TemporaryFolder = TemporaryFolder()

    lateinit var sketchStore: SketchStore
      private set

    @JvmStatic
    @BeforeClass
    fun createSketchesToStore() =
      runBlocking<Unit> { sketchStore = SketchStore(FileSystemStorageClient(temporaryFolder.root)) }
  }
}
