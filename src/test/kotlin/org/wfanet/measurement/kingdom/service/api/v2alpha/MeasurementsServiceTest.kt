// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2.alpha.ListMeasurementsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.listMeasurementsPageToken
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultPair
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.protocol
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.State as InternalState
import org.wfanet.measurement.internal.kingdom.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt as InternalProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest as internalCancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.differentialPrivacyParams as internalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest as internalGetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.liquidLegionsSketchParams as internalLiquidLegionsSketchParams
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig

private const val DEFAULT_LIMIT = 50
private const val DATA_PROVIDERS_CERTIFICATE_NAME =
  "dataProviders/AAAAAAAAAHs/certificates/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAHs"
private const val MEASUREMENT_NAME_2 = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAJs"
private const val MEASUREMENT_NAME_3 = "$MEASUREMENT_CONSUMER_NAME/measurements/AAAAAAAAAKs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAHs"
private val DATA_PROVIDERS_NAME = makeDataProvider(123L)
private val EXTERNAL_DATA_PROVIDER_IDS = listOf(ExternalId(123L), ExternalId(456L))
private val EXTERNAL_MEASUREMENT_ID =
  apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME)!!.measurementId)
private val EXTERNAL_MEASUREMENT_CONSUMER_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )
private val ENCRYPTED_DATA = ByteString.copyFromUtf8("data")
private val DUCHY_CERTIFICATE_NAME = "duchies/AAAAAAAAAHs/certificates/AAAAAAAAAHs"
private val DATA_PROVIDER_NONCE_HASH: ByteString =
  HexString("97F76220FEB39EE6F262B1F0C8D40F221285EEDE105748AE98F7DC241198D69F").bytes
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

@RunWith(JUnit4::class)
class MeasurementsServiceTest {
  private val internalMeasurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase =
    mockService {
      onBlocking { createMeasurement(any()) }.thenReturn(INTERNAL_MEASUREMENT)
      onBlocking { getMeasurement(any()) }.thenReturn(INTERNAL_MEASUREMENT)
      onBlocking { streamMeasurements(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MEASUREMENT,
            INTERNAL_MEASUREMENT.copy {
              externalMeasurementId =
                apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_2)!!.measurementId)
            },
            INTERNAL_MEASUREMENT.copy {
              externalMeasurementId =
                apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_3)!!.measurementId)
            }
          )
        )
      onBlocking { cancelMeasurement(any()) }.thenReturn(INTERNAL_MEASUREMENT)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalMeasurementsMock) }

  private lateinit var service: MeasurementsService

  @Before
  fun initService() {
    service =
      MeasurementsService(
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `getMeasurement returns measurement`() {
    val request = getMeasurementRequest { name = MEASUREMENT_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.getMeasurement(request) }
      }

    val expected = MEASUREMENT

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::getMeasurement
      )
      .isEqualTo(
        internalGetMeasurementRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `getMeasurement throws INVALID_ARGUMENT when resource name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getMeasurement(GetMeasurementRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getMeasurement throws PERMISSION_DENIED when mc caller doesn't match`() {
    val request = getMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.getMeasurement(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getMeasurement throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = getMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking { service.getMeasurement(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getMeasurement throws UNAUTHENTICATED when mc principal not found`() {
    val request = getMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getMeasurement(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMeasurement with RF type and single EDP returns Measurement with direct protocol`() {
    val measurement =
      MEASUREMENT.copy {
        val firstDataProvider = dataProviders.first()
        dataProviders.clear()
        dataProviders += firstDataProvider

        measurementSpec =
          measurementSpec.copy {
            data =
              MEASUREMENT_SPEC.copy {
                  val firstNonceHash = nonceHashes.first()
                  nonceHashes.clear()
                  nonceHashes += firstNonceHash
                }
                .toByteString()
          }

        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols += protocol { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        dataProviders.remove(EXTERNAL_DATA_PROVIDER_IDS.last().value)
        details =
          details.copy {
            measurementSpec = measurement.measurementSpec.data
            clearProtocolConfig()
            clearDuchyProtocolConfig()
          }
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val request = createMeasurementRequest {
      this.measurement =
        measurement.copy {
          clearName()
          clearProtocolConfig()
        }
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        internalMeasurement.copy {
          clearUpdateTime()
          clearExternalMeasurementId()
          details = details.copy { clearFailure() }
          results.clear()
        }
      )

    assertThat(result)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        ProtocolConfig.getDescriptor().findFieldByNumber(ProtocolConfig.NAME_FIELD_NUMBER)
      )
      .isEqualTo(measurement)
  }

  @Test
  fun `createMeasurement with impression type returns measurement with resource name set`() {
    runBlocking {
      whenever(internalMeasurementsMock.createMeasurement(any()))
        .thenReturn(
          INTERNAL_MEASUREMENT.copy {
            details =
              details.copy {
                clearProtocolConfig()
                measurementSpec =
                  MEASUREMENT_SPEC.copy {
                      clearReachAndFrequency()
                      impression = impression {
                        privacyParams = differentialPrivacyParams {
                          epsilon = 1.0
                          delta = 0.0
                        }
                        maximumFrequencyPerUser = 1
                      }
                    }
                    .toByteString()
              }
          }
        )
    }

    val request = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
          measurementSpec = signedData {
            data =
              MEASUREMENT_SPEC.copy {
                  clearReachAndFrequency()
                  impression = impression {
                    privacyParams = differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 0.0
                    }
                    maximumFrequencyPerUser = 1
                  }
                }
                .toByteString()
            signature = UPDATE_TIME.toByteString()
          }
        }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    val expected =
      MEASUREMENT.copy {
        measurementSpec = signedData {
          data =
            MEASUREMENT_SPEC.copy {
                clearReachAndFrequency()
                impression = impression {
                  privacyParams = differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 0.0
                  }
                  maximumFrequencyPerUser = 1
                }
              }
              .toByteString()
          signature = UPDATE_TIME.toByteString()
        }
        protocolConfig = protocolConfig {
          name = "protocolConfigs/Direct"
          measurementType = ProtocolConfig.MeasurementType.IMPRESSION
          protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
        }
      }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        INTERNAL_MEASUREMENT.copy {
          clearUpdateTime()
          clearExternalMeasurementId()
          details =
            details.copy {
              clearFailure()
              clearProtocolConfig()
              clearDuchyProtocolConfig()
              measurementSpec = request.measurement.measurementSpec.data
            }
          results.clear()
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `createMeasurement with duration type returns measurement with resource name set`() {
    runBlocking {
      whenever(internalMeasurementsMock.createMeasurement(any()))
        .thenReturn(
          INTERNAL_MEASUREMENT.copy {
            details =
              details.copy {
                clearProtocolConfig()
                measurementSpec =
                  MEASUREMENT_SPEC.copy {
                      clearReachAndFrequency()
                      duration = duration {
                        privacyParams = differentialPrivacyParams {
                          epsilon = 1.0
                          delta = 0.0
                        }
                        maximumWatchDurationPerUser = 1
                      }
                    }
                    .toByteString()
              }
          }
        )
    }

    val request = createMeasurementRequest {
      measurement =
        MEASUREMENT.copy {
          measurementSpec = signedData {
            data =
              MEASUREMENT_SPEC.copy {
                  clearReachAndFrequency()
                  duration = duration {
                    privacyParams = differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 0.0
                    }
                    maximumWatchDurationPerUser = 1
                  }
                }
                .toByteString()
            signature = UPDATE_TIME.toByteString()
          }
        }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    val expected =
      MEASUREMENT.copy {
        measurementSpec = signedData {
          data =
            MEASUREMENT_SPEC.copy {
                clearReachAndFrequency()
                duration = duration {
                  privacyParams = differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 0.0
                  }
                  maximumWatchDurationPerUser = 1
                }
              }
              .toByteString()
          signature = UPDATE_TIME.toByteString()
        }
        protocolConfig = protocolConfig {
          name = "protocolConfigs/Direct"
          measurementType = ProtocolConfig.MeasurementType.DURATION
          protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
        }
      }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        INTERNAL_MEASUREMENT.copy {
          clearUpdateTime()
          clearExternalMeasurementId()
          details =
            details.copy {
              clearFailure()
              clearProtocolConfig()
              clearDuchyProtocolConfig()
              measurementSpec = request.measurement.measurementSpec.data
            }
          results.clear()
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when certificate resource name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement = MEASUREMENT.copy { clearMeasurementConsumerCertificate() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws PERMISSION_DENIED when mc caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking {
            service.createMeasurement(createMeasurementRequest { measurement = MEASUREMENT })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createMeasurement throws PERMISSION_DENIED when principal without authorization is found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking {
            service.createMeasurement(createMeasurementRequest { measurement = MEASUREMENT })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createMeasurement throws UNAUTHENTICATED when mc principal not found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createMeasurement(createMeasurementRequest { measurement = MEASUREMENT })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement spec is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest { measurement = MEASUREMENT.copy { clearMeasurementSpec() } }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement public key is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data = MEASUREMENT_SPEC.copy { clearMeasurementPublicKey() }.toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF reach privacy params are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            reachAndFrequency = reachAndFrequency {
                              frequencyPrivacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                            }
                            vidSamplingInterval = vidSamplingInterval { width = 1.0F }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF frequency privacy params are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            reachAndFrequency = reachAndFrequency {
                              reachPrivacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                            }
                            vidSamplingInterval = vidSamplingInterval { width = 1.0F }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF vid sampling interval is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            clearVidSamplingInterval()
                            reachAndFrequency = reachAndFrequency {
                              reachPrivacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                              frequencyPrivacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                            }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF epsilon privacy param is 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            clearVidSamplingInterval()
                            reachAndFrequency = reachAndFrequency {
                              reachPrivacyParams = differentialPrivacyParams {
                                epsilon = 0.0
                                delta = 0.0
                              }
                              frequencyPrivacyParams = differentialPrivacyParams {
                                epsilon = 0.0
                                delta = 0.0
                              }
                            }
                            vidSamplingInterval = vidSamplingInterval { width = 1.0F }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when impression privacy params are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            impression = impression { maximumFrequencyPerUser = 1 }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when impression max freq per user is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            impression = impression {
                              privacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                            }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when duration privacy params are missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            duration = duration { maximumWatchDurationPerUser = 1 }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when duration watch time per user is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data =
                        MEASUREMENT_SPEC.copy {
                            clearReachAndFrequency()
                            duration = duration {
                              privacyParams = differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 1.0
                              }
                            }
                          }
                          .toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement type is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec = signedData {
                      data = MEASUREMENT_SPEC.copy { clearMeasurementType() }.toByteString()
                      signature = UPDATE_TIME.toByteString()
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Data Providers is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest { measurement = MEASUREMENT.copy { dataProviders.clear() } }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Data Providers Entry is missing key`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    dataProviders.clear()
                    dataProviders += dataProviderEntry { key = "" }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing cert name`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    dataProviders.clear()
                    dataProviders += dataProviderEntry { key = DATA_PROVIDERS_NAME }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing public key`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    dataProviders.clear()
                    dataProviders += dataProviderEntry {
                      key = DATA_PROVIDERS_NAME
                      value = value { dataProviderCertificate = DATA_PROVIDERS_CERTIFICATE_NAME }
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing spec`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    dataProviders.clear()
                    dataProviders += dataProviderEntry {
                      key = DATA_PROVIDERS_NAME
                      value = value {
                        dataProviderCertificate = DATA_PROVIDERS_CERTIFICATE_NAME
                        dataProviderPublicKey = signedData {
                          data = UPDATE_TIME.toByteString()
                          signature = UPDATE_TIME.toByteString()
                        }
                      }
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing nonce hash`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    dataProviders[0] =
                      dataProviders[0].copy { value = value.copy { clearNonceHash() } }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMeasurements with no page token returns response`() {
    val request = listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listMeasurements(request) }
      }

    val expected = listMeasurementsResponse {
      measurement += MEASUREMENT.copy { name = MEASUREMENT_NAME }
      measurement += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
      measurement += MEASUREMENT.copy { name = MEASUREMENT_NAME_3 }
    }

    val streamMeasurementsRequest =
      captureFirst<StreamMeasurementsRequest> {
        verify(internalMeasurementsMock).streamMeasurements(capture())
      }

    assertThat(streamMeasurementsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamMeasurementsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamMeasurementsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMeasurements with page token with filter returns response`() {
    val pageSize = 2
    val internalStates =
      listOf(
        InternalState.FAILED,
        InternalState.CANCELLED,
        InternalState.PENDING_PARTICIPANT_CONFIRMATION,
        InternalState.PENDING_COMPUTATION,
        InternalState.SUCCEEDED,
        InternalState.PENDING_REQUISITION_PARAMS,
        InternalState.PENDING_REQUISITION_FULFILLMENT
      )
    val publicStates =
      listOf(
        State.FAILED,
        State.SUCCEEDED,
        State.AWAITING_REQUISITION_FULFILLMENT,
        State.COMPUTING,
        State.CANCELLED
      )

    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = pageSize
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        states += publicStates
        lastMeasurement = previousPageEnd { externalMeasurementId = EXTERNAL_MEASUREMENT_ID }
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
      filter = filter { states += publicStates }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listMeasurements(request) }
      }

    val expected = listMeasurementsResponse {
      measurement += MEASUREMENT.copy { name = MEASUREMENT_NAME }
      measurement += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = pageSize
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        states += publicStates
        lastMeasurement = previousPageEnd {
          externalMeasurementId =
            apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_2)!!.measurementId)
        }
      }
      nextPageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    val streamMeasurementsRequest =
      captureFirst<StreamMeasurementsRequest> {
        verify(internalMeasurementsMock).streamMeasurements(capture())
      }

    assertThat(streamMeasurementsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamMeasurementsRequest {
          limit = pageSize + 1
          filter =
            StreamMeasurementsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              states += internalStates
              externalMeasurementIdAfter = EXTERNAL_MEASUREMENT_ID
              updatedAfter = Timestamp.getDefaultInstance()
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMeasurements with new page size replaces page size in page token`() {
    val pageSize = 3
    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.pageSize = pageSize
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = 1
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        lastMeasurement = previousPageEnd { externalMeasurementId = EXTERNAL_MEASUREMENT_ID }
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { service.listMeasurements(request) }
    }

    val streamMeasurementsRequest =
      captureFirst<StreamMeasurementsRequest> {
        verify(internalMeasurementsMock).streamMeasurements(capture())
      }

    assertThat(streamMeasurementsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamMeasurementsRequest {
          limit = pageSize + 1
          filter =
            StreamMeasurementsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalMeasurementIdAfter = EXTERNAL_MEASUREMENT_ID
              updatedAfter = Timestamp.getDefaultInstance()
            }
        }
      )
  }

  @Test
  fun `listMeasurements with no page size uses page size in page token`() {
    val pageSize = 1
    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = pageSize
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        lastMeasurement = previousPageEnd { externalMeasurementId = EXTERNAL_MEASUREMENT_ID }
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { service.listMeasurements(request) }
    }

    val streamMeasurementsRequest =
      captureFirst<StreamMeasurementsRequest> {
        verify(internalMeasurementsMock).streamMeasurements(capture())
      }

    assertThat(streamMeasurementsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamMeasurementsRequest {
          limit = pageSize + 1
          filter =
            StreamMeasurementsRequestKt.filter {
              externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
              externalMeasurementIdAfter = EXTERNAL_MEASUREMENT_ID
              updatedAfter = Timestamp.getDefaultInstance()
            }
        }
      )
  }

  @Test
  fun `listMeasurements throws PERMISSION_DENIED when mc caller doesn't match`() {
    val request = listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listMeasurements throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking { service.listMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listMeasurements throws UNAUTHENTICATED when mc principal not found`() {
    val request = listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listMeasurements(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMeasurements throws invalid argument when parent doesn't match parent in page token`() {
    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      val listMeasurementsPageToken = listMeasurementsPageToken {
        pageSize = DEFAULT_LIMIT
        externalMeasurementConsumerId = 654
        states += State.FAILED
        lastMeasurement = previousPageEnd { externalMeasurementId = EXTERNAL_MEASUREMENT_ID }
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
      filter = filter { states += listOf(State.FAILED) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMeasurements throws invalid argument when states don't match states in page token`() {
    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      val listMeasurementsPageToken = listMeasurementsPageToken {
        pageSize = DEFAULT_LIMIT
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        states += State.CANCELLED
        lastMeasurement = previousPageEnd { externalMeasurementId = EXTERNAL_MEASUREMENT_ID }
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
      filter = filter { states += listOf(State.FAILED) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMeasurements throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listMeasurements(ListMeasurementsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMeasurements throws INVALID_ARGUMENT when pageSize is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listMeasurements(
              listMeasurementsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `cancelMeasurement returns measurement`() {
    val request = cancelMeasurementRequest { name = MEASUREMENT_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.cancelMeasurement(request) }
      }

    val expected = MEASUREMENT

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::cancelMeasurement
      )
      .isEqualTo(
        internalCancelMeasurementRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `cancelMeasurement throws PERMISSION_DENIED when mc caller doesn't match`() {
    val request = cancelMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.cancelMeasurement(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `cancelMeasurement throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = cancelMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking { service.cancelMeasurement(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `cancelMeasurement throws UNAUTHENTICATED when mc principal not found`() {
    val request = cancelMeasurementRequest { name = MEASUREMENT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.cancelMeasurement(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `cancelMeasurement throws INVALID_ARGUMENT when resource name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.cancelMeasurement(CancelMeasurementRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when dataProviderList has duplicated keys`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement = MEASUREMENT.copy { dataProviders += dataProviders }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        INTERNAL_PROTOCOL_CONFIG.liquidLegionsV2,
        DUCHY_PROTOCOL_CONFIG.liquidLegionsV2,
        setOf("aggregator"),
        2
      )
    }

    private val INTERNAL_PROTOCOL_CONFIG = internalProtocolConfig {
      externalProtocolConfigId = "llv2"
      liquidLegionsV2 =
        InternalProtocolConfigKt.liquidLegionsV2 {
          sketchParams = internalLiquidLegionsSketchParams {
            decayRate = 1.1
            maxSize = 100
            samplingIndicatorSize = 1000
          }
          dataProviderNoise = internalDifferentialPrivacyParams {
            epsilon = 2.1
            delta = 3.3
          }
        }
    }

    private val PUBLIC_PROTOCOL_CONFIG = protocolConfig {
      name = "protocolConfigs/llv2"
      measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
      protocols +=
        ProtocolConfigKt.protocol {
          liquidLegionsV2 = liquidLegionsV2 {
            sketchParams = liquidLegionsSketchParams {
              decayRate = 1.1
              maxSize = 100
              samplingIndicatorSize = 1000
            }
            dataProviderNoise = differentialPrivacyParams {
              epsilon = 2.1
              delta = 3.3
            }
          }
        }
    }

    private val DUCHY_PROTOCOL_CONFIG = duchyProtocolConfig {
      liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
    }

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = UPDATE_TIME.toByteString()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1.0
        }
        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1.0
        }
      }
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
      nonceHashes += EXTERNAL_DATA_PROVIDER_IDS.map { it.value.toByteString() }
    }

    private val MEASUREMENT = measurement {
      name = MEASUREMENT_NAME
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signedData {
        data = MEASUREMENT_SPEC.toByteString()
        signature = UPDATE_TIME.toByteString()
      }
      dataProviders +=
        EXTERNAL_DATA_PROVIDER_IDS.map {
          val dataProviderKey = DataProviderKey(it.apiId.value)
          val certificateKey =
            DataProviderCertificateKey(dataProviderKey.dataProviderId, ExternalId(789L).apiId.value)
          dataProviderEntry {
            key = dataProviderKey.toName()
            value = value {
              dataProviderCertificate = certificateKey.toName()
              dataProviderPublicKey = signedData {
                data = UPDATE_TIME.toByteString()
                signature = UPDATE_TIME.toByteString()
              }
              encryptedRequisitionSpec = UPDATE_TIME.toByteString()
              nonceHash = DATA_PROVIDER_NONCE_HASH
            }
          }
        }
      protocolConfig = PUBLIC_PROTOCOL_CONFIG
      measurementReferenceId = "ref_id"
      failure = failure {
        reason = Failure.Reason.CERTIFICATE_REVOKED
        message = "Measurement Consumer Certificate has been revoked"
      }
      results += resultPair {
        certificate = DATA_PROVIDERS_CERTIFICATE_NAME
        encryptedResult = ENCRYPTED_DATA
      }
      results += resultPair {
        certificate = DUCHY_CERTIFICATE_NAME
        encryptedResult = ENCRYPTED_DATA
      }
    }

    private val INTERNAL_MEASUREMENT = internalMeasurement {
      externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
      externalMeasurementId = EXTERNAL_MEASUREMENT_ID
      providedMeasurementId = MEASUREMENT.measurementReferenceId
      externalMeasurementConsumerCertificateId =
        apiIdToExternalId(
          MeasurementConsumerCertificateKey.fromName(MEASUREMENT.measurementConsumerCertificate)!!
            .certificateId
        )
      updateTime = UPDATE_TIME
      dataProviders.putAll(
        MEASUREMENT.dataProvidersList.associateBy(
          { apiIdToExternalId(DataProviderKey.fromName(it.key)!!.dataProviderId) },
          {
            InternalMeasurementKt.dataProviderValue {
              externalDataProviderCertificateId =
                apiIdToExternalId(
                  DataProviderCertificateKey.fromName(it.value.dataProviderCertificate)!!
                    .certificateId
                )
              dataProviderPublicKey = it.value.dataProviderPublicKey.data
              dataProviderPublicKeySignature = it.value.dataProviderPublicKey.signature
              encryptedRequisitionSpec = it.value.encryptedRequisitionSpec
              nonceHash = it.value.nonceHash
            }
          }
        )
      )
      details =
        InternalMeasurementKt.details {
          apiVersion = Version.V2_ALPHA.string
          measurementSpec = MEASUREMENT.measurementSpec.data
          measurementSpecSignature = MEASUREMENT.measurementSpec.signature
          protocolConfig = INTERNAL_PROTOCOL_CONFIG
          duchyProtocolConfig = DUCHY_PROTOCOL_CONFIG
          failure =
            InternalMeasurementKt.failure {
              reason = InternalMeasurement.Failure.Reason.CERTIFICATE_REVOKED
              message = MEASUREMENT.failure.message
            }
        }
      results += resultInfo {
        externalAggregatorDuchyId = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!.duchyId
        externalCertificateId =
          apiIdToExternalId(DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!.certificateId)
        encryptedResult = ENCRYPTED_DATA
      }
      results += resultInfo {
        externalDataProviderId =
          apiIdToExternalId(
            DataProviderCertificateKey.fromName(DATA_PROVIDERS_CERTIFICATE_NAME)!!.dataProviderId
          )
        externalCertificateId =
          apiIdToExternalId(
            DataProviderCertificateKey.fromName(DATA_PROVIDERS_CERTIFICATE_NAME)!!.certificateId
          )
        encryptedResult = ENCRYPTED_DATA
      }
    }
  }
}
