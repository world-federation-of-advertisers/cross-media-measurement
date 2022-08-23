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
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.AnySketch.Register
import org.wfanet.anysketch.Sketch
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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt.gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange as privacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup as PrivacyLandscapeAge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Charge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender as PrivacyLandscapeGender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestInMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.loadtest.config.EventFilters.VID_SAMPLER_HASH_FUNCTION
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"
private const val MC_NAME = "mc"
private val EVENT_TEMPLATES =
  listOf(
    "$TEMPLATE_PREFIX.TestVideoTemplate",
    "$TEMPLATE_PREFIX.TestBannerTemplate",
    "$TEMPLATE_PREFIX.TestPrivacyBudgetTemplate"
  )
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
private val MEASUREMENT_CONSUMER_CERTIFICATE_DER =
  SECRET_FILES_PATH.resolve("mc_cs_cert.der").toFile().readByteString()
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private val EXTERNAL_MEASUREMENT_CONSUMER_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private val MEASUREMENT_CONSUMER_CERTIFICATE = certificate {
  name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
  x509Der = MEASUREMENT_CONSUMER_CERTIFICATE_DER
}
private val MEASUREMENT_PUBLIC_KEY =
  SECRET_FILES_PATH.resolve("${MC_NAME}_enc_public.tink").toFile().readByteString()
private val CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY = elGamalPublicKey {
  ellipticCurveId = 415
  generator = HexString("036B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296").bytes
  element = HexString("0277BF406C5AA4376413E480E0AB8B0EFCA999D362204E6D1686E0BE567811604D").bytes
}

private val REQUISITION_ONE_SPEC = requisitionSpec {
  eventGroups += eventGroupEntry {
    key = "eventGroup/name"
    value =
      RequisitionSpecKt.EventGroupEntryKt.value {
        collectionInterval = timeInterval {
          startTime =
            LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        }
        filter = eventFilter {
          expression = "privacy_budget.age.value == 1 || banner_ad.gender.value == 2"
        }
      }
  }
  measurementPublicKey = MEASUREMENT_PUBLIC_KEY
  nonce = Random.Default.nextLong()
}
private const val DUCHIES_MAP_KEY = "1"
private const val DUCHY_NAME = "worker1"

@RunWith(JUnit4::class)
class EdpSimulatorTest {
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService {
    onBlocking { getCertificate(any()) }.thenReturn(MEASUREMENT_CONSUMER_CERTIFICATE)
  }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION_ONE })
      .thenReturn(listRequisitionsResponse {})
  }
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
      if (
        vidSampler.vidIsInSamplingBucket(
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
    bannerAd: TestBannerTemplate,
    privacyBudget: TestPrivacyBudgetTemplate,
    vidRange: IntRange,
  ): Map<Int, List<TestEvent>> {
    return vidRange
      .map {
        it to
          listOf(
            testEvent {
              this.bannerAd = bannerAd
              this.privacyBudget = privacyBudget
            }
          )
      }
      .toMap()
  }

  @Test
  fun `filters events, charges privacy budget and generates sketch successfully`() {
    runBlocking {
      val videoTemplateMatchingVids = (1..10)
      val bannerTemplateMatchingVids = (11..20)
      val nonMatchingVids = (21..40)
      val matchingVids = videoTemplateMatchingVids + bannerTemplateMatchingVids

      val matchingTestPrivacyTemplate = testPrivacyBudgetTemplate {
        age = privacyAgeRange { value = PrivacyAgeRange.Value.AGE_35_TO_54 }
      }
      val matchingTestBannerTemplate = testBannerTemplate {
        gender = gender { value = Gender.Value.GENDER_FEMALE }
      }

      val nonMatchingTestPrivacyTemplate = testPrivacyBudgetTemplate {
        age = privacyAgeRange { value = PrivacyAgeRange.Value.AGE_18_TO_24 }
      }
      val nonMatchingTestBannerTemplate = testBannerTemplate {
        gender = gender { value = Gender.Value.GENDER_MALE }
      }

      val privacyTemplateMatchingEvents =
        getEvents(
          nonMatchingTestBannerTemplate,
          matchingTestPrivacyTemplate,
          videoTemplateMatchingVids
        )

      val bannerTemplateMatchingEvents =
        getEvents(
          matchingTestBannerTemplate,
          nonMatchingTestPrivacyTemplate,
          bannerTemplateMatchingVids
        )

      val nonMatchingEvents =
        getEvents(nonMatchingTestBannerTemplate, nonMatchingTestPrivacyTemplate, nonMatchingVids)

      val matchingEvents = privacyTemplateMatchingEvents + bannerTemplateMatchingEvents
      val allEvents = matchingEvents + nonMatchingEvents

      val backingStore = TestInMemoryBackingStore()
      val privacyBudgetManager =
        PrivacyBudgetManager(
          PrivacyBucketFilter(TestPrivacyBucketMapper()),
          backingStore,
          10.0f,
          0.02f
        )

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
          FilterEventQuery(allEvents),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          EVENT_TEMPLATES,
          privacyBudgetManager
        )
      edpSimulator.createEventGroup()
      edpSimulator.executeRequisitionFulfillingWorkflow()
      val storedSketch = sketchStore.get(REQUISITION_ONE)?.read()?.flatten()
      assertThat(storedSketch).isNotNull()

      val anySketchResult = SketchProtos.toAnySketch(Sketch.parseFrom(storedSketch))

      val vidSamplingIntervalStart = 0.0f
      val vidSamplingIntervalWidth = PRIVACY_BUCKET_VID_SAMPLE_WIDTH

      assertAnySketchEquals(
        anySketchResult,
        getExpectedResult(matchingVids, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      )

      val balanceLedger: Map<PrivacyBucketGroup, MutableMap<Charge, PrivacyBudgetBalanceEntry>> =
        backingStore.getBalancesMap()

      // All the Buckets are only charged once, so all entries should have a repetition count of 1.
      balanceLedger.values
        .flatMap { it.values }
        .forEach { assertThat(it.repetitionCount).isEqualTo(1) }

      // The list of all the charged privacy bucket groups should be correct based on the filter.
      assertThat(balanceLedger.keys)
        .containsExactly(
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.MALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now(),
            LocalDate.now(),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.RANGE_35_54,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.MALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LocalDate.now().minusDays(1),
            LocalDate.now().minusDays(1),
            PrivacyLandscapeAge.ABOVE_54,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          )
        )
    }
  }
  @Test
  fun `calculate direct reach and frequency correctly`() {
    runBlocking {
      val vidList = mutableListOf(1L, 1L, 1L, 2L, 2L, 3L, 4L, 5L)
      val (reachValue, frequencyMap) = EdpSimulator.calculateDirectReachAndFrequency(vidList)
      // 5 unique people(1, 2, 3, 4, 5) being reached
      val expectedReachValue = 5
      // 1 reach -> 0.6(3/5), 2 reach -> 0.2(1/5), 3 reach -> 0.2(1/5)
      val expectedFrequencyMap = mapOf(1L to 0.6, 2L to 0.2, 3L to 0.2)

      assertThat(reachValue).isEqualTo(expectedReachValue)
      frequencyMap.forEach { (frequency, percentage) ->
        assertThat(percentage).isEqualTo(expectedFrequencyMap[frequency])
      }
    }
  }

  companion object {
    private val MC_SIGNING_KEY =
      loadSigningKey("${MC_NAME}_cs_cert.der", "${MC_NAME}_cs_private.der")
    private val DUCHY_SIGNING_KEY =
      loadSigningKey("${DUCHY_NAME}_cs_cert.der", "${DUCHY_NAME}_cs_private.der")

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MEASUREMENT_PUBLIC_KEY
      reachAndFrequency = reachAndFrequency {}
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      }
      nonceHashes += hashSha256(REQUISITION_ONE_SPEC.nonce)
    }

    private val ENCRYPTED_REQUISITION_ONE_SPEC =
      encryptRequisitionSpec(
        signedRequisitionSpec = signRequisitionSpec(REQUISITION_ONE_SPEC, MC_SIGNING_KEY),
        measurementPublicKey =
          loadEncryptionPublicKey("${EDP_DISPLAY_NAME}_enc_public.tink").toEncryptionPublicKey()
      )

    private val REQUISITION_ONE = requisition {
      name = "requisition_one"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate =
        MeasurementConsumerCertificateKey(
            externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
            externalIdToApiId(8L)
          )
          .toName()
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_ONE_SPEC
      protocolConfig = protocolConfig {
        liquidLegionsV2 =
          ProtocolConfigKt.liquidLegionsV2 {
            sketchParams = liquidLegionsSketchParams {
              decayRate = LLV2_DECAY_RATE
              maxSize = LLV2_MAX_SIZE
              samplingIndicatorSize = 10_000_000
            }
            ellipticCurveId = 415
            maximumFrequency = 12
          }
      }
      duchies += duchyEntry {
        key = DUCHIES_MAP_KEY
        value = value {
          duchyCertificate = externalIdToApiId(6L)
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey =
              signElgamalPublicKey(CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY, DUCHY_SIGNING_KEY)
          }
        }
      }
    }

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
