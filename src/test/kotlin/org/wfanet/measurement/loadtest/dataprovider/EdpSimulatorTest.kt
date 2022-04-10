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

import com.google.common.truth.Correspondence
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
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
import org.wfanet.anysketch.AnySketch.Register
import org.wfanet.anysketch.SketchConfig.ValueSpec.Aggregator
import org.wfanet.anysketch.SketchConfigKt.indexSpec
import org.wfanet.anysketch.SketchConfigKt.valueSpec
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.distribution
import org.wfanet.anysketch.exponentialDistribution
import org.wfanet.anysketch.oracleDistribution
import org.wfanet.anysketch.sketchConfig
import org.wfanet.anysketch.uniformDistribution
import org.wfanet.estimation.VidSampler
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.Gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt.gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
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
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.loadtest.config.EventFilters.VID_SAMPLER_HASH_FUNCTION
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"
private const val MC_NAME = "mc"
private val EVENT_TEMPLATES =
  listOf("$TEMPLATE_PREFIX.TestVideoTemplate", "$TEMPLATE_PREFIX.TestBannerTemplate")
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

private val SKETCH_CONFIG = sketchConfig {
  indexes += indexSpec {
    name = "Index"
    distribution = distribution {
      exponential = exponentialDistribution {
        rate = LLV2_DECAY_RATE
        numValues = LLV2_MAX_SIZE
      }
    }
  }
  values += valueSpec {
    name = "SamplingIndicator"
    aggregator = Aggregator.UNIQUE
    distribution = distribution {
      uniform = uniformDistribution {
        numValues = 10_000_000 // 10M
      }
    }
  }
  values += valueSpec {
    name = "Frequency"
    aggregator = Aggregator.SUM
    distribution = distribution { oracle = oracleDistribution { key = "frequency" } }
  }
}

class FilterTestEventQuery(val events: Map<Int, TestEvent>) : EventQuery() {

  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
    val program =
      EventFilters.compileProgram(
        eventFilter.expression,
        testEvent {},
      )
    return sequence {
      for (vid in events.keys.toList()) {
        if (EventFilters.matches(events.get(vid) as Message, program)) {
          yield(vid.toLong())
        }
      }
    }
  }
}

@RunWith(JUnit4::class)
class EdpSimulatorTest {
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

  private fun getExpectedResult(
    matchingVids: List<Int>,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float
  ): AnySketch {
    val vidSampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)
    val expectedResult: AnySketch = SketchProtos.toAnySketch(SKETCH_CONFIG)

    matchingVids.forEach {
      if (vidSampler.vidIsInSamplingBucket(
          it.toLong(),
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth
        )
      ) {
        expectedResult.insert(it.toLong(), mapOf("frequency" to 1L))
      }
    }
    return expectedResult
  }

  private fun getEvents(
    videoAd: TestVideoTemplate,
    bannerAd: TestBannerTemplate,
    vidRange: IntRange
  ): Map<Int, TestEvent> {
    return vidRange
      .map {
        it to
          testEvent {
            this.videoAd = videoAd
            this.bannerAd = bannerAd
          }
      }
      .toMap()
  }

  @Test
  fun `filters events and generate sketch successfully`() = runBlocking {
    val videoTemplateMatchingVids = (1..10)
    val bannerTemplateMatchingVids = (11..20)
    val nonMatchingVids = (21..40)
    val matchingVids = videoTemplateMatchingVids + bannerTemplateMatchingVids

    val matchingTestVideoTemplate = testVideoTemplate {
      age = ageRange { value = AgeRange.Value.AGE_18_TO_24 }
    }
    val matchingTestBannerTemplate = testBannerTemplate {
      gender = gender { value = Gender.Value.GENDER_FEMALE }
    }
    val nonMatchingTestVideoTemplate = testVideoTemplate {
      age = ageRange { value = AgeRange.Value.AGE_RANGE_UNSPECIFIED }
    }
    val nonMatchingTestBannerTemplate = testBannerTemplate {
      gender = gender { value = Gender.Value.GENDER_MALE }
    }

    val videoTemplateMatchingEvents =
      getEvents(matchingTestVideoTemplate, nonMatchingTestBannerTemplate, videoTemplateMatchingVids)

    val bannerTemplateMatchingEvents =
      getEvents(
        nonMatchingTestVideoTemplate,
        matchingTestBannerTemplate,
        bannerTemplateMatchingVids
      )

    val nonMatchingEvents =
      getEvents(nonMatchingTestVideoTemplate, nonMatchingTestBannerTemplate, nonMatchingVids)

    val matchingEvents = videoTemplateMatchingEvents + bannerTemplateMatchingEvents
    val allEvents = matchingEvents + nonMatchingEvents

    val vidSamplingIntervalStart = 0.1f
    val vidSamplingIntervalWidth = 0.2f

    val edpSimulator =
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
        FilterTestEventQuery(allEvents),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        EVENT_TEMPLATES
      )

    val result: AnySketch =
      SketchProtos.toAnySketch(
        edpSimulator.generateSketch(
          SKETCH_CONFIG,
          eventFilter { expression = "video_ad.age.value == 1 || banner_ad.gender.value == 2" },
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth
        )
      )

    assertAnySketchEquals(
      result,
      getExpectedResult(matchingVids, vidSamplingIntervalStart, vidSamplingIntervalWidth)
    )
  }

  companion object {

    @JvmField @ClassRule val temporaryFolder: TemporaryFolder = TemporaryFolder()
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

    private val EQUIVALENCE: Correspondence<Register?, Register?> =
      Correspondence.from(EdpSimulatorTest::registersEquivalent, "is equivalent to")

    fun registersEquivalent(result: Register?, expected: Register?): Boolean {
      if (result == null || expected == null) {
        return result == expected
      }
      return result.getIndex() == expected.getIndex() &&
        result.getValues().containsAll(expected.getValues()) &&
        expected.getValues().containsAll(result.getValues())
    }

    private fun assertAnySketchEquals(sketch: AnySketch, other: AnySketch) {
      assertThat(sketch).comparingElementsUsing(EQUIVALENCE).containsExactlyElementsIn(other)
    }

    lateinit var sketchStore: SketchStore
      private set

    @JvmStatic
    @BeforeClass
    fun initSketchStore() =
      runBlocking<Unit> { sketchStore = SketchStore(FileSystemStorageClient(temporaryFolder.root)) }
  }
}
