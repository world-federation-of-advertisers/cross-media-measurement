/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import io.grpc.Status
import java.lang.UnsupportedOperationException
import java.security.cert.X509Certificate
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.config.EventGroupMetadata

@RunWith(JUnit4::class)
class AbstractRequisitionGrouperTest {
  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { refuseRequisition(any()) }.thenReturn(REQUISITION)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  abstract val requistionGrouper: RequisitionGrouper

  @Test
  fun `refuses Requisition when EventGroup not found`() {
    requisitionGrouper.mapRequisition { REQUISTION }
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
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
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message)
      .contains(REQUISITION_SPEC.events.eventGroupsList.first().key)
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  companion object {
    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      "dataProviders/foo/eventGroupMetadataDescriptors/bar"

    private val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val DUCHY_ONE_SIGNING_KEY =
      loadSigningKey("${DUCHY_ONE_ID}_cs_cert.der", "${DUCHY_ONE_ID}_cs_private.der")
    private val DUCHY_TWO_SIGNING_KEY =
      loadSigningKey("${DUCHY_TWO_ID}_cs_cert.der", "${DUCHY_TWO_ID}_cs_private.der")

    private val DUCHY_ONE_NAME = DuchyKey(DUCHY_ONE_ID).toName()
    private val DUCHY_TWO_NAME = DuchyKey(DUCHY_TWO_ID).toName()
    private val DUCHY_ONE_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_ONE_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_ONE_SIGNING_KEY.certificate.encoded.toByteString()
    }
    private val DUCHY_TWO_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_TWO_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_TWO_SIGNING_KEY.certificate.encoded.toByteString()
    }

    private val DUCHY1_ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val EDP_SIGNING_KEY =
      loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
    private val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
    private val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(9L))

    private val DATA_PROVIDER_CERTIFICATE = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val DATA_PROVIDER_RESULT_CERTIFICATE = certificate {
      name = DATA_PROVIDER_RESULT_CERTIFICATE_KEY.toName()
      x509Der = EDP_RESULT_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_RESULT_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val EDP_DATA =
      DataProviderData(
        EDP_NAME,
        EDP_DISPLAY_NAME,
        loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
        EDP_RESULT_SIGNING_KEY,
        DATA_PROVIDER_RESULT_CERTIFICATE_KEY,
      )

    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups += eventGroupEntry {
            key = EVENT_GROUP_NAME
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = TIME_RANGE.start.toProtoTime()
                  endTime = TIME_RANGE.endExclusive.toProtoTime()
                }
                filter = eventFilter {
                  expression =
                    "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                      "person.gender == ${Person.Gender.FEMALE_VALUE}"
                }
              }
          }
        }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.Default.nextLong()
    }

    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

    private val REACH_ONLY_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        clearReachAndFrequency()
        reach = reach { privacyParams = OUTPUT_DP_PARAMS }
      }
    private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      impression = impression {
        privacyParams = differentialPrivacyParams {
          epsilon = 10.0
          delta = 1E-12
        }
        maximumFrequencyPerUser = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

    private val LIQUID_LEGIONS_SKETCH_PARAMS = liquidLegionsSketchParams {
      decayRate = LLV2_DECAY_RATE
      maxSize = LLV2_MAX_SIZE
      samplingIndicatorSize = 10_000_000
    }

    private val REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            liquidLegionsV2 =
              ProtocolConfigKt.liquidLegionsV2 {
                noiseMechanism = NOISE_MECHANISM
                sketchParams = LIQUID_LEGIONS_SKETCH_PARAMS
                ellipticCurveId = 415
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += duchyEntry {
        key = DUCHY_ONE_NAME
        value = value {
          duchyCertificate = DUCHY_ONE_CERTIFICATE.name
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey =
              signElgamalPublicKey(CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
          }
        }
      }
    }

    private val DUCHY_ENTRY_ONE = duchyEntry {
      key = DUCHY_ONE_NAME
      value = value {
        duchyCertificate = DUCHY_ONE_CERTIFICATE.name
        honestMajorityShareShuffle = honestMajorityShareShuffle {
          publicKey = signEncryptionPublicKey(DUCHY1_ENCRYPTION_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
        }
      }
    }

    private val DUCHY_ENTRY_TWO = duchyEntry {
      key = DUCHY_TWO_NAME
      value = value { duchyCertificate = DUCHY_TWO_CERTIFICATE.name }
    }

    /** TODO(@kungfucraig): Replace this with the object in HmmsRequisitions.kt */
    private val HMSS_REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            honestMajorityShareShuffle =
              ProtocolConfigKt.honestMajorityShareShuffle {
                ringModulus = RING_MODULUS
                noiseMechanism = NOISE_MECHANISM
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += DUCHY_ENTRY_ONE
      duchies += DUCHY_ENTRY_TWO
    }

    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      readCertificateCollection(SECRET_FILES_PATH.resolve("edp_trusted_certs.pem").toFile())
        .associateBy { requireNotNull(it.authorityKeyIdentifier) }

    private val TEST_EVENT_TEMPLATES = EdpSimulator.buildEventTemplates(TestEvent.getDescriptor())
    private val TEST_METADATA = EventGroupMetadata.testMetadata(1)

    private val SYNTHETIC_DATA_SPEC =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL.first().copy {
        dateSpecs.forEachIndexed { index, dateSpec ->
          dateSpecs[index] =
            dateSpec.copy {
              dateRange =
                SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
                  start = FIRST_EVENT_DATE.toProtoDate()
                  endExclusive = (LAST_EVENT_DATE.plusDays(1)).toProtoDate()
                }
            }
        }
      }
    private val syntheticGeneratorEventQuery =
      object :
        SyntheticGeneratorEventQuery(
          SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
          TestEvent.getDescriptor(),
        ) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return SYNTHETIC_DATA_SPEC
        }
      }

    private val POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1000L
        }
      }
    }
    private val HMSS_VID_INDEX_MAP = InMemoryVidIndexMap.build(POPULATION_SPEC)

    /** Dummy [Throttler] for satisfying signatures without being used. */
    private val dummyThrottler =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T {
          throw UnsupportedOperationException("Should not be called")
        }
      }

    /** [SketchEncrypter] that does not encrypt, just returning the plaintext. */
    private val fakeSketchEncrypter =
      object : SketchEncrypter {
        override fun encrypt(
          sketch: Sketch,
          ellipticCurveId: Int,
          encryptionKey: ElGamalPublicKey,
          maximumValue: Int,
        ): ByteString {
          return sketch.toByteString()
        }

        override fun encrypt(
          sketch: Sketch,
          ellipticCurveId: Int,
          encryptionKey: ElGamalPublicKey,
        ): ByteString {
          return sketch.toByteString()
        }
      }

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
      return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
    }
  }
}
