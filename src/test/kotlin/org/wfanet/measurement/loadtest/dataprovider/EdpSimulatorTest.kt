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
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
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
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
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
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.PersonKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
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
  listOf("$TEMPLATE_PREFIX.Video", "$TEMPLATE_PREFIX.BannerAd", "$TEMPLATE_PREFIX.Person")
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
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private val MEASUREMENT_CONSUMER_CERTIFICATE = certificate {
  name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
  x509Der = MEASUREMENT_CONSUMER_CERTIFICATE_DER
}
private val MEASUREMENT_PUBLIC_KEY =
  encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRET_FILES_PATH.resolve("${MC_NAME}_enc_public.tink").toFile().readByteString()
    }
    .toByteString()

private val CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY = elGamalPublicKey {
  ellipticCurveId = 415
  generator = HexString("036B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296").bytes
  element = HexString("0277BF406C5AA4376413E480E0AB8B0EFCA999D362204E6D1686E0BE567811604D").bytes
}

private val LAST_EVENT_DATE = LocalDate.now()
private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

private val REQUISITION_ONE_SPEC = requisitionSpec {
  eventGroups += eventGroupEntry {
    key = "eventGroup/name"
    value =
      RequisitionSpecKt.EventGroupEntryKt.value {
        collectionInterval = timeInterval {
          startTime = TIME_RANGE.start.toProtoTime()
          endTime = TIME_RANGE.endExclusive.toProtoTime()
        }
        filter = eventFilter {
          expression = "person.age_group.value == 1 && person.gender.value == 2"
        }
      }
  }
  measurementPublicKey = MEASUREMENT_PUBLIC_KEY
  nonce = Random.Default.nextLong()
}
private const val DUCHY_ID = "worker1"

@RunWith(JUnit4::class)
class EdpSimulatorTest {
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService {
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }))
      }
      .thenReturn(MEASUREMENT_CONSUMER_CERTIFICATE)
    onBlocking { getCertificate(eq(getCertificateRequest { name = DUCHY_CERTIFICATE.name })) }
      .thenReturn(DUCHY_CERTIFICATE)
  }
  private val measurementConsumersServiceMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }
        .thenReturn(
          measurementConsumer {
            publicKey = signEncryptionPublicKey(MC_PUBLIC_KEY, MC_SIGNING_KEY)
            certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
          }
        )
    }
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()
  private val eventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenReturn(
          eventGroupMetadataDescriptor {
            name = "dataProviders/foo/eventGroupMetadataDescriptors/bar"
          }
        )
    }
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION_ONE })
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }
  private val requisitionFulfillmentServiceMock: RequisitionFulfillmentCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersServiceMock)
    addService(certificatesServiceMock)
    addService(eventGroupsServiceMock)
    addService(eventGroupMetadataDescriptorsServiceMock)
    addService(requisitionsServiceMock)
    addService(requisitionFulfillmentServiceMock)
  }

  private val measurementConsumersStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }

  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupMetadataDescriptorsStub by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  private val backingStore = TestInMemoryBackingStore()
  private val privacyBudgetManager =
    PrivacyBudgetManager(PrivacyBucketFilter(TestPrivacyBucketMapper()), backingStore, 10.0f, 0.02f)

  private fun getExpectedResult(
    matchingVids: Iterable<Long>,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float
  ): AnySketch {
    val vidSampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)
    val expectedResult: AnySketch = SketchProtos.toAnySketch(SKETCH_CONFIG)

    matchingVids.forEach {
      if (
        vidSampler.vidIsInSamplingBucket(it, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      ) {
        expectedResult.insert(it, mapOf("frequency" to 1L))
      }
    }
    return expectedResult
  }

  private fun generateEvents(
    vidRange: LongRange,
    date: LocalDate,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender
  ): Map<Long, List<TestEvent>> {
    return vidRange.associateWith { vid ->
      listOf(
        testEvent {
          time = date.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          person = person {
            this.vid = vid
            this.ageGroup = PersonKt.ageGroupField { value = ageGroup }
            this.gender = PersonKt.genderField { value = gender }
          }
        }
      )
    }
  }

  @Test
  fun `filters events, charges privacy budget and generates sketch successfully`() {
    runBlocking {
      val matchingEvents =
        generateEvents(
          1L..10L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.FEMALE
        )
      val nonMatchingEvents =
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE
        ) +
          generateEvents(
            16L..20L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_55_PLUS,
            Person.Gender.FEMALE
          ) +
          generateEvents(
            21L..25L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_18_TO_34,
            Person.Gender.MALE
          ) +
          generateEvents(
            26L..30L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_35_TO_54,
            Person.Gender.MALE
          )

      val allEvents = matchingEvents + nonMatchingEvents

      val edpSimulator =
        EdpSimulator(
          EDP_DATA,
          MC_NAME,
          measurementConsumersStub,
          certificatesStub,
          eventGroupsStub,
          eventGroupMetadataDescriptorsStub,
          requisitionsStub,
          requisitionFulfillmentStub,
          sketchStore,
          InMemoryEventQuery(allEvents),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          EVENT_TEMPLATES,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES
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
        getExpectedResult((1L..10L), vidSamplingIntervalStart, vidSamplingIntervalWidth)
      )

      val balanceLedger: Map<PrivacyBucketGroup, MutableMap<Charge, PrivacyBudgetBalanceEntry>> =
        backingStore.getBalancesMap()

      // Verify that each bucket is only charged once.
      for (bucketBalances in balanceLedger.values) {
        assertThat(bucketBalances).hasSize(1)
        for (balanceEntry in bucketBalances.values) {
          assertThat(balanceEntry.repetitionCount).isEqualTo(1)
        }
      }

      // The list of all the charged privacy bucket groups should be correct based on the filter.
      assertThat(balanceLedger.keys)
        .containsExactly(
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          ),
        )
    }
  }

  @Test
  fun `refuses requisition when DuchyEntry verification fails`() {
    val throttlerMock = mock<Throttler>()
    val eventQueryMock = mock<EventQuery>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        sketchStore,
        eventQueryMock,
        throttlerMock,
        listOf(),
        privacyBudgetManager,
        TRUSTED_CERTIFICATES
      )
    val requisition =
      REQUISITION_ONE.copy {
        duchies[0] =
          duchies[0].copy {
            value =
              value.copy {
                liquidLegionsV2 =
                  liquidLegionsV2.copy {
                    elGamalPublicKey =
                      elGamalPublicKey.copy { signature = "garbage".toByteStringUtf8() }
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { requisitionsServiceMock.listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = requisition.name
          refusal = refusal { justification = Refusal.Justification.CONSENT_SIGNAL_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message).contains(DUCHY_NAME)
    verifyBlocking(requisitionFulfillmentServiceMock, never()) { fulfillRequisition(any()) }
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `fulfill direct reach and frequency requisition correctly with sampling rate equal to 1`() {
    val throttlerMock = mock<Throttler>()
    val eventQuery = RandomEventQuery(SketchGenerationParams(reach = 1000, universeSize = 10000))
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        sketchStore,
        eventQuery,
        throttlerMock,
        listOf(),
        privacyBudgetManager,
        TRUSTED_CERTIFICATES
      )

    val vidSamplingIntervalWidth = 1f
    val newMeasurementSpec =
      MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = vidSamplingIntervalWidth }
      }
    val requisition =
      REQUISITION_ONE.copy {
        name = "requisition_direct"
        protocolConfig =
          protocolConfig.copy {
            protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
          }
        measurementSpec = signMeasurementSpec(newMeasurementSpec, MC_SIGNING_KEY)
      }

    requisitionsServiceMock.stub {
      onBlocking { requisitionsServiceMock.listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val fulfillDirectRequisitionRequest: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition
      )

    val signedResult = decryptResult(fulfillDirectRequisitionRequest.encryptedData, MC_PRIVATE_KEY)
    val reachAndFrequencyResult = Measurement.Result.parseFrom(signedResult.data)

    val expectedReachValue = 948L
    val expectedFrequencyMap =
      mapOf(
        1L to 0.947389665261748,
        2L to 0.04805005905234108,
        3L to 0.0038138458821366963,
        4L to 9.558853281715655E-5
      )

    assertThat(reachAndFrequencyResult.reach.value).isEqualTo(expectedReachValue)
    reachAndFrequencyResult.frequency.relativeFrequencyDistributionMap.forEach {
      (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedFrequencyMap[frequency])
    }

    verifyBlocking(requisitionFulfillmentServiceMock, never()) { fulfillRequisition(any()) }
    verifyBlocking(requisitionsServiceMock, times(1)) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `fulfill direct reach and frequency requisition correctly with sampling rate smaller than 1`() {
    val throttlerMock = mock<Throttler>()
    val eventQuery = RandomEventQuery(SketchGenerationParams(reach = 1000, universeSize = 10000))
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        sketchStore,
        eventQuery,
        throttlerMock,
        listOf(),
        privacyBudgetManager,
        TRUSTED_CERTIFICATES
      )

    val vidSamplingIntervalWidth = 0.1f
    val newMeasurementSpec =
      MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = vidSamplingIntervalWidth }
      }
    val requisition =
      REQUISITION_ONE.copy {
        name = "requisition_direct"
        protocolConfig =
          protocolConfig.copy {
            protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
          }
        measurementSpec = signMeasurementSpec(newMeasurementSpec, MC_SIGNING_KEY)
      }

    requisitionsServiceMock.stub {
      onBlocking { requisitionsServiceMock.listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val fulfillDirectRequisitionRequest: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition
      )

    val signedResult = decryptResult(fulfillDirectRequisitionRequest.encryptedData, MC_PRIVATE_KEY)
    val reachAndFrequencyResult = Measurement.Result.parseFrom(signedResult.data)

    // vidList is first filtered by vidSampler and noised reach is scaled by sampling rate which is
    // vidSamplingInterval.
    val expectedReachValue = 1010L
    val expectedFrequencyMap =
      mapOf(
        1L to 0.9614979640529286,
        2L to 0.01568143177129103,
      )

    assertThat(reachAndFrequencyResult.reach.value).isEqualTo(expectedReachValue)
    reachAndFrequencyResult.frequency.relativeFrequencyDistributionMap.forEach {
      (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedFrequencyMap[frequency])
    }

    verifyBlocking(requisitionFulfillmentServiceMock, never()) { fulfillRequisition(any()) }
    verifyBlocking(requisitionsServiceMock, times(1)) { fulfillDirectRequisition(any()) }
  }

  companion object {
    private val MC_SIGNING_KEY =
      loadSigningKey("${MC_NAME}_cs_cert.der", "${MC_NAME}_cs_private.der")
    private val DUCHY_SIGNING_KEY =
      loadSigningKey("${DUCHY_ID}_cs_cert.der", "${DUCHY_ID}_cs_private.der")

    private val DUCHY_NAME = DuchyKey(DUCHY_ID).toName()
    private val DUCHY_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_SIGNING_KEY.certificate.encoded.toByteString()
    }

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = org.wfanet.measurement.loadtest.dataprovider.MEASUREMENT_PUBLIC_KEY
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams { epsilon = 1.0 }
        frequencyPrivacyParams = differentialPrivacyParams { epsilon = 1.0 }
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      }
      nonceHashes += hashSha256(REQUISITION_ONE_SPEC.nonce)
    }

    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val MEASUREMENT_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val ENCRYPTED_REQUISITION_ONE_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_ONE_SPEC, MC_SIGNING_KEY),
        MEASUREMENT_PUBLIC_KEY
      )

    private val EDP_DATA =
      EdpData(
        EDP_NAME,
        EDP_DISPLAY_NAME,
        loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
        loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
      )

    private val REQUISITION_ONE = requisition {
      name = "requisition_one"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_ONE_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
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
      }
      duchies += duchyEntry {
        key = DUCHY_NAME
        value = value {
          duchyCertificate = DUCHY_CERTIFICATE.name
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey =
              signElgamalPublicKey(CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY, DUCHY_SIGNING_KEY)
          }
        }
      }
    }

    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      readCertificateCollection(SECRET_FILES_PATH.resolve("edp_trusted_certs.pem").toFile())
        .associateBy { requireNotNull(it.authorityKeyIdentifier) }

    @JvmField @ClassRule val temporaryFolder: TemporaryFolder = TemporaryFolder()

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String
    ): SigningKeyHandle {
      return loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile()
      )
    }

    private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
      return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
    }

    private val EQUIVALENCE: Correspondence<Register?, Register?> =
      Correspondence.from(EdpSimulatorTest::registersEquivalent, "is equivalent to")

    private fun registersEquivalent(result: Register?, expected: Register?): Boolean {
      if (result == null || expected == null) {
        return result == expected
      }
      return result.index == expected.index &&
        result.values.containsAll(expected.values) &&
        expected.values.containsAll(result.values)
    }

    private fun assertAnySketchEquals(sketch: AnySketch, other: AnySketch) {
      assertThat(sketch).comparingElementsUsing(EQUIVALENCE).containsExactlyElementsIn(other)
    }

    private lateinit var sketchStore: SketchStore

    @JvmStatic
    @BeforeClass
    fun initSketchStore() {
      sketchStore = SketchStore(FileSystemStorageClient(temporaryFolder.root))
    }
  }
}
