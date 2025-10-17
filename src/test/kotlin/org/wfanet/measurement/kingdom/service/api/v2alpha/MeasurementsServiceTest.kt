/*
 * Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Durations
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
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.population
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listMeasurementsPageToken
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.reachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.BatchGetDataProvidersRequest
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement.State as InternalState
import org.wfanet.measurement.internal.kingdom.MeasurementFailure as InternalMeasurementFailure
import org.wfanet.measurement.internal.kingdom.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt as InternalProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt.direct
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCreateMeasurementsRequest as internalBatchCreateMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.batchCreateMeasurementsResponse as internalBatchCreateMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.batchGetDataProvidersResponse as internalBatchGetDataProvidersResponse
import org.wfanet.measurement.internal.kingdom.batchGetMeasurementsRequest as internalBatchGetMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.batchGetMeasurementsResponse as internalBatchGetMeasurementsResponse
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest as internalCancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementRequest as internalCreateMeasurementRequest
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderCapabilities as internalDataProviderCapabilities
import org.wfanet.measurement.internal.kingdom.differentialPrivacyParams as internalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.getMeasurementRequest as internalGetMeasurementRequest
import org.wfanet.measurement.internal.kingdom.liquidLegionsSketchParams as internalLiquidLegionsSketchParams
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.measurementDetails
import org.wfanet.measurement.internal.kingdom.measurementFailure as internalMeasurementFailure
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfig
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException

private const val DEFAULT_LIMIT = 50
private const val DATA_PROVIDERS_RESULT_CERTIFICATE_NAME =
  "dataProviders/AAAAAAAAALs/certificates/AAAAAAAAALs"
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
private val EXTERNAL_MEASUREMENT_ID_2 =
  apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_2)!!.measurementId)
private val EXTERNAL_MEASUREMENT_ID_3 =
  apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_3)!!.measurementId)
private val EXTERNAL_MEASUREMENT_CONSUMER_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )
private val ENCRYPTED_DATA = ByteString.copyFromUtf8("data")
private val ENCRYPTED_RESULT = encryptedMessage {
  ciphertext = ENCRYPTED_DATA
  typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
}
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DUCHY_CERTIFICATE_NAME = "$DUCHY_NAME/certificates/AAAAAAAAAHs"
private val DATA_PROVIDER_NONCE_HASH: ByteString =
  HexString("97F76220FEB39EE6F262B1F0C8D40F221285EEDE105748AE98F7DC241198D69F").bytes
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(100).toProtoTime()
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val CREATED_AFTER: Timestamp = Instant.ofEpochSecond(0).toProtoTime()
private val CREATED_BEFORE: Timestamp = Instant.now().toProtoTime()
private val UPDATED_AFTER: Timestamp = Instant.ofEpochSecond(0).toProtoTime()
private val UPDATED_BEFORE: Timestamp = Instant.now().toProtoTime()

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
            INTERNAL_MEASUREMENT.copy { externalMeasurementId = EXTERNAL_MEASUREMENT_ID_2 },
            INTERNAL_MEASUREMENT.copy { externalMeasurementId = EXTERNAL_MEASUREMENT_ID_3 },
          )
        )
      onBlocking { cancelMeasurement(any()) }.thenReturn(INTERNAL_MEASUREMENT)
      onBlocking { batchCreateMeasurements(any()) }
        .thenReturn(
          internalBatchCreateMeasurementsResponse {
            measurements += INTERNAL_MEASUREMENT
            measurements +=
              INTERNAL_MEASUREMENT.copy { externalMeasurementId = EXTERNAL_MEASUREMENT_ID_2 }
          }
        )
      onBlocking { batchGetMeasurements(any()) }
        .thenReturn(
          internalBatchGetMeasurementsResponse {
            measurements += INTERNAL_MEASUREMENT
            measurements +=
              INTERNAL_MEASUREMENT.copy { externalMeasurementId = EXTERNAL_MEASUREMENT_ID_2 }
          }
        )
    }
  private val internalDataProvidersMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase =
    mockService {
      onBlocking { batchGetDataProviders(any()) }
        .thenAnswer { invocation ->
          val request: BatchGetDataProvidersRequest = invocation.getArgument(0)
          val internalDataProviders: List<InternalDataProvider> =
            request.externalDataProviderIdsList.map {
              internalDataProvider { externalDataProviderId = it }
            }
          internalBatchGetDataProvidersResponse { dataProviders.addAll(internalDataProviders) }
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalMeasurementsMock)
    addService(internalDataProvidersMock)
  }

  private lateinit var service: MeasurementsService
  private lateinit var hmssEnabledService: MeasurementsService
  private lateinit var trusTeeEnabledService: MeasurementsService

  @Before
  fun initService() {
    service =
      MeasurementsService(
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        NOISE_MECHANISMS,
        reachOnlyLlV2Enabled = true,
      )

    hmssEnabledService =
      MeasurementsService(
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        NOISE_MECHANISMS,
        hmssEnabled = true,
      )

    trusTeeEnabledService =
      MeasurementsService(
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        NOISE_MECHANISMS,
        trusTeeEnabled = true,
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
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::getMeasurement,
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
  fun `createMeasurement with REACH_AND_FREQUENCY type and multiple EDPs`() {
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
      requestId = "foo"
    }
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        details = details.copy { clearFailure() }
        results.clear()
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        MEASUREMENT.copy {
          clearFailure()
          results.clear()
        }
      )
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
            }
          requestId = request.requestId
        }
      )
  }

  @Test
  fun `createMeasurement with REACH type and multiple EDPs`() {
    val measurement =
      REACH_ONLY_MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
    }
    val internalMeasurement =
      REACH_ONLY_INTERNAL_MEASUREMENT.copy {
        results.clear()
        details = details.copy { clearFailure() }
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        REACH_ONLY_MEASUREMENT.copy {
          clearFailure()
          results.clear()
        }
      )

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
            }
        }
      )
  }

  @Test
  fun `createMeasurement with REACH_AND_FREQUENCY type and single EDP specifies direct protocol`() {
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()

        dataProviders.clear()
        dataProviders += MEASUREMENT.dataProvidersList.first()

        measurementSpec =
          measurementSpec.copy {
            setMessage(
              MEASUREMENT_SPEC.copy {
                  nonceHashes.clear()
                  nonceHashes += MEASUREMENT_SPEC.nonceHashesList.first()
                }
                .pack()
            )
          }

        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement =
        measurement.copy {
          // This should be ignored.
          protocolConfig = protocolConfig {
            measurementType = ProtocolConfig.MeasurementType.IMPRESSION
          }
        }
    }
    val firstDataProviderExternalId = EXTERNAL_DATA_PROVIDER_IDS.first()
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        results.clear()
        details =
          details.copy {
            clearFailure()

            clearDuchyProtocolConfig()
            clearProtocolConfig()
            measurementSpec = measurement.measurementSpec.message.value
          }

        dataProviders.clear()
        dataProviders[firstDataProviderExternalId.value] =
          INTERNAL_MEASUREMENT.dataProvidersMap.getValue(firstDataProviderExternalId.value)
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        measurement.copy {
          protocolConfig =
            protocolConfig.copy {
              protocols.clear()
              protocols +=
                ProtocolConfigKt.protocol {
                  direct = DEFAULT_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG
                }
            }
        }
      )
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()

              details =
                details.copy {
                  protocolConfig = internalProtocolConfig {
                    direct = DEFAULT_INTERNAL_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG
                  }
                }
            }
        }
      )
  }

  @Test
  fun `createMeasurement with IMPRESSION type specifies direct protocol`() {
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()

        measurementSpec = measurementSpec.copy { setMessage(IMPRESSION_MEASUREMENT_SPEC.pack()) }

        protocolConfig =
          protocolConfig.copy {
            measurementType = ProtocolConfig.MeasurementType.IMPRESSION

            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement =
        measurement.copy {
          clearName()
          clearProtocolConfig()
        }
    }
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        results.clear()
        details =
          details.copy {
            clearFailure()

            clearDuchyProtocolConfig()
            clearProtocolConfig()
            measurementSpec = measurement.measurementSpec.message.value
          }
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        measurement.copy {
          protocolConfig =
            protocolConfig.copy {
              protocols.clear()
              protocols +=
                ProtocolConfigKt.protocol { direct = DEFAULT_DIRECT_IMPRESSION_PROTOCOL_CONFIG }
            }
        }
      )
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()

              details =
                details.copy {
                  protocolConfig = internalProtocolConfig {
                    direct = DEFAULT_INTERNAL_DIRECT_IMPRESSION_PROTOCOL_CONFIG
                  }
                }
            }
        }
      )
  }

  @Test
  fun `createMeasurement with DURATION type specifies direct protocol`() {
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()

        measurementSpec = measurementSpec.copy { setMessage(DURATION_MEASUREMENT_SPEC.pack()) }

        protocolConfig =
          protocolConfig.copy {
            measurementType = ProtocolConfig.MeasurementType.DURATION

            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement =
        measurement.copy {
          clearName()
          clearProtocolConfig()
        }
    }
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        results.clear()
        details =
          details.copy {
            clearFailure()

            clearDuchyProtocolConfig()
            clearProtocolConfig()
            measurementSpec = measurement.measurementSpec.message.value
          }
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        measurement.copy {
          protocolConfig =
            protocolConfig.copy {
              protocols.clear()
              protocols +=
                ProtocolConfigKt.protocol { direct = DEFAULT_DIRECT_WATCH_DURATION_PROTOCOL_CONFIG }
            }
        }
      )
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()

              details =
                details.copy {
                  protocolConfig = internalProtocolConfig {
                    direct = DEFAULT_INTERNAL_DIRECT_WATCH_DURATION_PROTOCOL_CONFIG
                  }
                }
            }
        }
      )
  }

  @Test
  fun `createMeasurement with POPULATION type specifies direct protocol`() {
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()

        measurementSpec =
          measurementSpec.copy {
            setMessage(
              MEASUREMENT_SPEC.copy {
                  clearReachAndFrequency()
                  population = population {}
                  modelLine = "some-model-line"
                }
                .pack()
            )
          }

        protocolConfig =
          protocolConfig.copy {
            measurementType = ProtocolConfig.MeasurementType.POPULATION

            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement =
        measurement.copy {
          clearName()
          clearProtocolConfig()
        }
    }
    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        results.clear()
        details =
          details.copy {
            clearFailure()

            clearDuchyProtocolConfig()
            clearProtocolConfig()
            measurementSpec = measurement.measurementSpec.message.value
          }
      }
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }.thenReturn(internalMeasurement)
    }

    val response: Measurement =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createMeasurement(request) }
      }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        measurement.copy {
          protocolConfig =
            protocolConfig.copy {
              protocols.clear()
              protocols +=
                ProtocolConfigKt.protocol { direct = DEFAULT_DIRECT_POPULATION_PROTOCOL_CONFIG }
            }
        }
      )
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            internalMeasurement.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()

              details =
                details.copy {
                  protocolConfig = internalProtocolConfig {
                    direct = DEFAULT_INTERNAL_DIRECT_POPULATION_PROTOCOL_CONFIG
                  }
                }
            }
        }
      )
  }

  @Test
  fun `createMeasurement with HMSS enabled and EDPs capable specifies HMSS protocol`() {
    internalDataProvidersMock.stub {
      onBlocking { batchGetDataProviders(any()) }
        .thenReturn(
          internalBatchGetDataProvidersResponse {
            for (externalDataProviderId in EXTERNAL_DATA_PROVIDER_IDS) {
              dataProviders += internalDataProvider {
                this.externalDataProviderId = externalDataProviderId.value
                details =
                  details.copy {
                    capabilities = internalDataProviderCapabilities {
                      honestMajorityShareShuffleSupported = true
                    }
                  }
              }
            }
          }
        )
    }
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
      requestId = "foo"
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { hmssEnabledService.createMeasurement(request) }
    }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            INTERNAL_MEASUREMENT.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
              results.clear()
              details =
                details.copy {
                  clearFailure()
                  protocolConfig = HMSS_INTERNAL_PROTOCOL_CONFIG
                  clearDuchyProtocolConfig()
                }
            }
          requestId = request.requestId
        }
      )
  }

  @Test
  fun `createMeasurement with HMSS enabled using wrapping VidSamplingInterval`() {
    internalDataProvidersMock.stub {
      onBlocking { batchGetDataProviders(any()) }
        .thenReturn(
          internalBatchGetDataProvidersResponse {
            for (externalDataProviderId in EXTERNAL_DATA_PROVIDER_IDS) {
              dataProviders += internalDataProvider {
                this.externalDataProviderId = externalDataProviderId.value
                details =
                  details.copy {
                    capabilities = internalDataProviderCapabilities {
                      honestMajorityShareShuffleSupported = true
                    }
                  }
              }
            }
          }
        )
    }
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
        measurementSpec = signedMessage {
          setMessage(WRAPPING_INTERVAL_MEASUREMENT_SPEC.pack())
          signature = UPDATE_TIME.toByteString()
          signatureAlgorithmOid = "2.9999"
        }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
      requestId = "foo"
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { hmssEnabledService.createMeasurement(request) }
    }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            INTERNAL_MEASUREMENT.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
              results.clear()
              details =
                details.copy {
                  clearFailure()
                  protocolConfig = HMSS_INTERNAL_PROTOCOL_CONFIG
                  clearDuchyProtocolConfig()
                  measurementSpec = WRAPPING_INTERVAL_MEASUREMENT_SPEC.pack().value
                }
            }
          requestId = request.requestId
        }
      )
  }

  @Test
  fun `createMeasurement with TrusTEE enabled and EDPs capable specifies trusTEE protocol`() {
    internalDataProvidersMock.stub {
      onBlocking { batchGetDataProviders(any()) }
        .thenReturn(
          internalBatchGetDataProvidersResponse {
            for (externalDataProviderId in EXTERNAL_DATA_PROVIDER_IDS) {
              dataProviders += internalDataProvider {
                this.externalDataProviderId = externalDataProviderId.value
                details =
                  details.copy {
                    capabilities = internalDataProviderCapabilities { trusTeeSupported = true }
                  }
              }
            }
          }
        )
    }
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
      requestId = "foo"
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { trusTeeEnabledService.createMeasurement(request) }
    }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            INTERNAL_MEASUREMENT.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
              results.clear()
              details =
                details.copy {
                  clearFailure()
                  protocolConfig = TRUS_TEE_INTERNAL_PROTOCOL_CONFIG
                  clearDuchyProtocolConfig()
                }
            }
          requestId = request.requestId
        }
      )
  }

  @Test
  fun `createMeasurement with TrusTEE enabled using wrapping VidSamplingInterval`() {
    internalDataProvidersMock.stub {
      onBlocking { batchGetDataProviders(any()) }
        .thenReturn(
          internalBatchGetDataProvidersResponse {
            for (externalDataProviderId in EXTERNAL_DATA_PROVIDER_IDS) {
              dataProviders += internalDataProvider {
                this.externalDataProviderId = externalDataProviderId.value
                details =
                  details.copy {
                    capabilities = internalDataProviderCapabilities { trusTeeSupported = true }
                  }
              }
            }
          }
        )
    }
    val measurement =
      MEASUREMENT.copy {
        clearFailure()
        results.clear()
        clearProtocolConfig()
        measurementSpec = signedMessage {
          setMessage(WRAPPING_INTERVAL_MEASUREMENT_SPEC.pack())
          signature = UPDATE_TIME.toByteString()
          signatureAlgorithmOid = "2.9999"
        }
      }
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      this.measurement = measurement
      requestId = "foo"
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { trusTeeEnabledService.createMeasurement(request) }
    }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .isEqualTo(
        internalCreateMeasurementRequest {
          this.measurement =
            INTERNAL_MEASUREMENT.copy {
              clearExternalMeasurementId()
              clearCreateTime()
              clearUpdateTime()
              results.clear()
              details =
                details.copy {
                  clearFailure()
                  protocolConfig = TRUS_TEE_INTERNAL_PROTOCOL_CONFIG
                  clearDuchyProtocolConfig()
                  measurementSpec = WRAPPING_INTERVAL_MEASUREMENT_SPEC.pack().value
                }
            }
          requestId = request.requestId
        }
      )
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when model line is missing for POPULATION measurement`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                measurement =
                  MEASUREMENT.copy {
                    measurementSpec =
                      measurementSpec.copy {
                        setMessage(
                          MEASUREMENT_SPEC.copy {
                              clearReachAndFrequency()
                              population = population {}
                            }
                            .pack()
                        )
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
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                measurement = MEASUREMENT
              }
            )
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
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                measurement = MEASUREMENT
              }
            )
          }
        }
      }

    assertWithMessage(exception.toString())
      .that(exception.status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createMeasurement throws UNAUTHENTICATED when mc principal not found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createMeasurement(createMeasurementRequest { measurement = MEASUREMENT })
        }
      }
    assertWithMessage(exception.toString())
      .that(exception.status.code)
      .isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(createMeasurementRequest { measurement = MEASUREMENT })
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("parent")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when certificate does not match parent`() {
    val measurementConsumerCertificateKey =
      MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
    val measurementConsumerCertificate =
      MeasurementConsumerCertificateKey("bogus", measurementConsumerCertificateKey.certificateId)
        .toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                measurement =
                  MEASUREMENT.copy {
                    this.measurementConsumerCertificate = measurementConsumerCertificate
                  }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("measurement_consumer_certificate")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement spec is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT.copy { clearMeasurementSpec() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("measurement_spec")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement public key is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(MEASUREMENT_SPEC.copy { clearMeasurementPublicKey() }.pack())
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("key")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF reach privacy params are missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                MEASUREMENT_SPEC.copy {
                    reachAndFrequency = reachAndFrequency.copy { clearReachPrivacyParams() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("reach")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF frequency privacy params are missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                MEASUREMENT_SPEC.copy {
                    reachAndFrequency = reachAndFrequency.copy { clearFrequencyPrivacyParams() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("frequency")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF vid sampling interval is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(MEASUREMENT_SPEC.copy { clearVidSamplingInterval() }.pack())
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("sampling")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF epsilon privacy param is 0`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                MEASUREMENT_SPEC.copy {
                    reachAndFrequency =
                      reachAndFrequency.copy {
                        reachPrivacyParams = reachPrivacyParams.copy { clearEpsilon() }
                      }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("privacy")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF spec is missing max frequency`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                MEASUREMENT_SPEC.copy {
                    reachAndFrequency = reachAndFrequency.copy { clearMaximumFrequency() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("maximum_frequency")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when RF spec max frequency is 1`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                MEASUREMENT_SPEC.copy {
                    reachAndFrequency = reachAndFrequency.copy { maximumFrequency = 1 }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("maximum_frequency")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Reach-only privacy params are missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        REACH_ONLY_MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                REACH_ONLY_MEASUREMENT_SPEC.copy { reach = reach.copy { clearPrivacyParams() } }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("privacy")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Reach-only vid sampling interval is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        REACH_ONLY_MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(REACH_ONLY_MEASUREMENT_SPEC.copy { clearVidSamplingInterval() }.pack())
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("sampling")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Reach-only epsilon privacy param is 0`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        REACH_ONLY_MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                REACH_ONLY_MEASUREMENT_SPEC.copy {
                    reach = reach.copy { privacyParams = privacyParams.copy { clearEpsilon() } }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("privacy")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when impression privacy params are missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                IMPRESSION_MEASUREMENT_SPEC.copy {
                    impression = impression.copy { clearPrivacyParams() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("privacy")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when impression max freq per user is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                IMPRESSION_MEASUREMENT_SPEC.copy {
                    impression = impression.copy { clearMaximumFrequencyPerUser() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("frequency")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when duration privacy params are missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                DURATION_MEASUREMENT_SPEC.copy { duration = duration.copy { clearPrivacyParams() } }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("privacy")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when duration watch time per user is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(
                DURATION_MEASUREMENT_SPEC.copy {
                    duration = duration.copy { clearMaximumWatchDurationPerUser() }
                  }
                  .pack()
              )
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("maximum")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when measurement type is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          measurementSpec =
            measurementSpec.copy {
              setMessage(MEASUREMENT_SPEC.copy { clearMeasurementType() }.pack())
            }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("type")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Data Providers is missing`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT.copy { dataProviders.clear() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("provider")
  }

  @Test
  fun `createMeasurement throws INVALID_ARGUMENT when Data Providers Entry is missing key`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT.copy { dataProviders[0] = dataProviders[0].copy { clearKey() } }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("provider")
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing cert name`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          dataProviders[0] =
            dataProviders[0].copy { value = value.copy { clearDataProviderCertificate() } }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("provider")
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing public key`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          dataProviders[0] =
            dataProviders[0].copy { value = value.copy { clearDataProviderPublicKey() } }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("key")
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing spec`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          dataProviders[0] =
            dataProviders[0].copy { value = value.copy { clearEncryptedRequisitionSpec() } }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("requisition")
  }

  @Test
  fun `createMeasurement throws error when Data Providers Entry value is missing nonce hash`() {
    val request = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy {
          dataProviders[0] = dataProviders[0].copy { value = value.copy { clearNonceHash() } }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createMeasurement(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("nonce")
  }

  @Test
  fun `createMeasurement throws error when LLv2 VidSamplingInterval is wrapping around 1`() =
    runBlocking {
      val request = createMeasurementRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        measurement =
          MEASUREMENT.copy {
            measurementSpec = signedMessage {
              setMessage(WRAPPING_INTERVAL_MEASUREMENT_SPEC.pack())
              signature = UPDATE_TIME.toByteString()
              signatureAlgorithmOid = "2.9999"
            }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
            runBlocking { service.createMeasurement(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().ignoringCase().contains("VidSamplingInterval")
    }

  @Test
  fun `listMeasurements with no page token returns response`() {
    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      filter = filter {
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listMeasurements(request) }
      }

    val expected = listMeasurementsResponse {
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME }
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_3 }
    }

    val streamMeasurementsRequest: StreamMeasurementsRequest = captureFirst {
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
              createdAfter = CREATED_AFTER
              createdBefore = CREATED_BEFORE
              updatedBefore = UPDATED_BEFORE
              updatedAfter = UPDATED_AFTER
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
        InternalState.PENDING_REQUISITION_FULFILLMENT,
      )
    val publicStates =
      listOf(
        State.FAILED,
        State.SUCCEEDED,
        State.AWAITING_REQUISITION_FULFILLMENT,
        State.COMPUTING,
        State.CANCELLED,
      )

    val request = listMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = pageSize
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        states += publicStates
        lastMeasurement = previousPageEnd {
          updateTime = UPDATE_TIME
          externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        }
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
      filter = filter {
        states += publicStates
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listMeasurements(request) }
      }

    val expected = listMeasurementsResponse {
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME }
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
      val listMeasurementsPageToken = listMeasurementsPageToken {
        this.pageSize = pageSize
        externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
        states += publicStates
        lastMeasurement = previousPageEnd {
          externalMeasurementId =
            apiIdToExternalId(MeasurementKey.fromName(MEASUREMENT_NAME_2)!!.measurementId)
        }
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      nextPageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    val streamMeasurementsRequest: StreamMeasurementsRequest = captureFirst {
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
              createdAfter = CREATED_AFTER
              createdBefore = CREATED_BEFORE
              updatedBefore = UPDATED_BEFORE
              updatedAfter = UPDATED_AFTER
              after =
                StreamMeasurementsRequestKt.FilterKt.after {
                  updateTime = UPDATE_TIME
                  measurement = measurementKey {
                    externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
                    externalMeasurementId = EXTERNAL_MEASUREMENT_ID
                  }
                }
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
        lastMeasurement = previousPageEnd {
          updateTime = UPDATE_TIME
          externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        }
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      filter = filter {
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { service.listMeasurements(request) }
    }

    val streamMeasurementsRequest: StreamMeasurementsRequest = captureFirst {
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
              createdAfter = CREATED_AFTER
              createdBefore = CREATED_BEFORE
              updatedBefore = UPDATED_BEFORE
              updatedAfter = UPDATED_AFTER
              after =
                StreamMeasurementsRequestKt.FilterKt.after {
                  updateTime = UPDATE_TIME
                  measurement = measurementKey {
                    externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
                    externalMeasurementId = EXTERNAL_MEASUREMENT_ID
                  }
                }
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
        lastMeasurement = previousPageEnd {
          updateTime = UPDATE_TIME
          externalMeasurementId = EXTERNAL_MEASUREMENT_ID
        }
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      filter = filter {
        createdAfter = CREATED_AFTER
        createdBefore = CREATED_BEFORE
        updatedBefore = UPDATED_BEFORE
        updatedAfter = UPDATED_AFTER
      }
      pageToken = listMeasurementsPageToken.toByteArray().base64UrlEncode()
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { service.listMeasurements(request) }
    }

    val streamMeasurementsRequest: StreamMeasurementsRequest = captureFirst {
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
              createdAfter = CREATED_AFTER
              createdBefore = CREATED_BEFORE
              updatedBefore = UPDATED_BEFORE
              updatedAfter = UPDATED_AFTER
              after =
                StreamMeasurementsRequestKt.FilterKt.after {
                  updateTime = UPDATE_TIME
                  measurement = measurementKey {
                    externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
                    externalMeasurementId = EXTERNAL_MEASUREMENT_ID
                  }
                }
            }
        }
      )
  }

  @Test
  fun `listMeasurements with no filters returns response`() {
    val request = listMeasurementsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listMeasurements(request) }
      }

    val expected = listMeasurementsResponse {
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME }
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_3 }
    }

    val streamMeasurementsRequest: StreamMeasurementsRequest = captureFirst {
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
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::cancelMeasurement,
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

  @Test
  fun `batchCreateMeasurements returns measurements`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchCreateMeasurements(request) }
      }

    val expected = batchCreateMeasurementsResponse {
      measurements += MEASUREMENT
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
    }

    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        clearExternalMeasurementId()
        clearCreateTime()
        clearUpdateTime()
        details = details.copy { clearFailure() }
        results.clear()
      }
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchCreateMeasurements,
      )
      .isEqualTo(
        internalBatchCreateMeasurementsRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          requests += internalCreateMeasurementRequest { measurement = internalMeasurement }
          requests += internalCreateMeasurementRequest { measurement = internalMeasurement }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMeasurements returns measurements when all child requests do not have parent`() {
    val createMeasurementRequest = createMeasurementRequest { measurement = MEASUREMENT }
    val createMeasurementRequest2 = createMeasurementRequest { measurement = MEASUREMENT }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchCreateMeasurements(request) }
      }

    val expected = batchCreateMeasurementsResponse {
      measurements += MEASUREMENT
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
    }

    val internalMeasurement =
      INTERNAL_MEASUREMENT.copy {
        clearExternalMeasurementId()
        clearCreateTime()
        clearUpdateTime()
        details = details.copy { clearFailure() }
        results.clear()
      }
    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchCreateMeasurements,
      )
      .isEqualTo(
        internalBatchCreateMeasurementsRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          requests += internalCreateMeasurementRequest { measurement = internalMeasurement }
          requests += internalCreateMeasurementRequest { measurement = internalMeasurement }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when 1 of 2 child requests has no parent`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest { measurement = MEASUREMENT }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when too many requests`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      for (i in 0..BATCH_LIMIT) {
        requests += createMeasurementRequest
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when child parent doesn't match`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when parent is missing`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when child parent is invalid`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = "measurementConsumers"
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateMeasurements throws PERMISSION_DENIED when mc caller doesn't match`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateMeasurements throws PERMISSION_DENIED when principal without auth found`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateMeasurements throws UNAUTHENTICATED when mc principal not found`() {
    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchCreateMeasurements(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchCreateMeasurements throws INVALID_ARGUMENT when certificate does not match parent`() {
    val measurementConsumerCertificateKey =
      MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
    val measurementConsumerCertificate =
      MeasurementConsumerCertificateKey("bogus", measurementConsumerCertificateKey.certificateId)
        .toName()

    val createMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      measurement = MEASUREMENT
    }
    val createMeasurementRequest2 = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      parent = MEASUREMENT_CONSUMER_NAME
      measurement =
        MEASUREMENT.copy { this.measurementConsumerCertificate = measurementConsumerCertificate }
    }
    val request = batchCreateMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createMeasurementRequest
      requests += createMeasurementRequest2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateMeasurements(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("measurement_consumer_certificate")
  }

  @Test
  fun `batchGetMeasurements returns measurements`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchGetMeasurements(request) }
      }

    val expected = batchGetMeasurementsResponse {
      measurements += MEASUREMENT
      measurements += MEASUREMENT.copy { name = MEASUREMENT_NAME_2 }
    }

    verifyProtoArgument(
        internalMeasurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchGetMeasurements,
      )
      .isEqualTo(
        internalBatchGetMeasurementsRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          externalMeasurementIds += EXTERNAL_MEASUREMENT_ID
          externalMeasurementIds += EXTERNAL_MEASUREMENT_ID_2
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchGetMeasurements throws INVALID_ARGUMENT when too many requests`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      for (i in 0..BATCH_LIMIT) {
        names += MEASUREMENT_NAME
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchGetMeasurements throws INVALID_ARGUMENT when parent is missing`() {
    val request = batchGetMeasurementsRequest {
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchGetMeasurements throws INVALID_ARGUMENT when parent is invalid`() {
    val request = batchGetMeasurementsRequest {
      parent = "measurementConsumers"
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchGetMeasurements throws PERMISSION_DENIED when mc caller doesn't match parent`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchGetMeasurements throws INVALID_ARGUMENT when resource name is invalid`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += MEASUREMENT_NAME
      names += "measurements"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchGetMeasurements throws INVALID_ARGUMENT when mc caller doesn't match name`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchGetMeasurements throws PERMISSION_DENIED when principal without authorization found`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_NAME) {
          runBlocking { service.batchGetMeasurements(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchGetMeasurements throws UNAUTHENTICATED when mc principal not found`() {
    val request = batchGetMeasurementsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += MEASUREMENT_NAME
      names += MEASUREMENT_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchGetMeasurements(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getMeasurement throws NOT_FOUND with measurement name when measurement not found`() {
    internalMeasurementsMock.stub {
      onBlocking { getMeasurement(any()) }
        .thenThrow(
          MeasurementNotFoundByMeasurementConsumerException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(EXTERNAL_MEASUREMENT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getMeasurement(getMeasurementRequest { name = MEASUREMENT_NAME }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("measurement", MEASUREMENT_NAME)
  }

  @Test
  fun `createMeasurement throws FAILED_PRECONDITION when certificate is invalid`() {
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }
        .thenThrow(
          CertificateIsInvalidException()
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate is invalid.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                this.measurement = MEASUREMENT
                requestId = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `createMeasurement throws FAILED_PRECONDITION with data provider name when data provider not found`() {
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }
        .thenThrow(
          DataProviderNotFoundException(EXTERNAL_DATA_PROVIDER_IDS.first())
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                this.measurement = MEASUREMENT
                requestId = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDERS_NAME)
  }

  @Test
  fun `createMeasurement throws FAILED_PRECONDITION with duchy name when duchy not found`() {
    val duchyId = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!.duchyId
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }
        .thenThrow(
          DuchyNotFoundException(duchyId)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                this.measurement = MEASUREMENT
                requestId = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("duchy", DUCHY_NAME)
  }

  @Test
  fun `createMeasurement throws NOT_FOUND with measurement consumer name when measurement consumer not found`() {
    internalMeasurementsMock.stub {
      onBlocking { createMeasurement(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.createMeasurement(
              createMeasurementRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                this.measurement = MEASUREMENT
                requestId = "foo"
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumer", MEASUREMENT_CONSUMER_NAME)
  }

  @Test
  fun `cancelMeasurement throws NOT_FOUND with measurement name when measurement not found`() {
    internalMeasurementsMock.stub {
      onBlocking { cancelMeasurement(any()) }
        .thenThrow(
          MeasurementNotFoundByMeasurementConsumerException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(EXTERNAL_MEASUREMENT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.cancelMeasurement(cancelMeasurementRequest { name = MEASUREMENT_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("measurement", MEASUREMENT_NAME)
  }

  @Test
  fun `cancelMeasurement throws FAILED_PRECONDITION with measurement state and name when measurement state illegal`() {
    internalMeasurementsMock.stub {
      onBlocking { cancelMeasurement(any()) }
        .thenThrow(
          MeasurementStateIllegalException(
              ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              ExternalId(EXTERNAL_MEASUREMENT_ID),
              InternalState.FAILED,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Measurement state illegal.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.cancelMeasurement(cancelMeasurementRequest { name = MEASUREMENT_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("measurement", MEASUREMENT_NAME)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("state", State.FAILED.toString())
  }

  @Test
  fun `batchCreateMeasurement throws FAILED_PRECONDITION with data provider name when data provider not found`() {
    internalMeasurementsMock.stub {
      onBlocking { batchCreateMeasurements(any()) }
        .thenThrow(
          DataProviderNotFoundException(EXTERNAL_DATA_PROVIDER_IDS.first())
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.batchCreateMeasurements(
              batchCreateMeasurementsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                requests += createMeasurementRequest {
                  parent = MEASUREMENT_CONSUMER_NAME
                  this.measurement = MEASUREMENT
                  requestId = "foo"
                }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDERS_NAME)
  }

  @Test
  fun `batchCreateMeasurement throws NOT_FOUND with measurement consumer name when measurment consumer not found`() {
    internalMeasurementsMock.stub {
      onBlocking { batchCreateMeasurements(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
        )
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.batchCreateMeasurements(
              batchCreateMeasurementsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                requests += createMeasurementRequest {
                  parent = MEASUREMENT_CONSUMER_NAME
                  this.measurement = MEASUREMENT
                  requestId = "foo"
                }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumer", MEASUREMENT_CONSUMER_NAME)
  }

  @Test
  fun `batchGetMeasurements throws NOT_FOUND when a measurement not found`() {
    internalMeasurementsMock.stub {
      onBlocking { batchGetMeasurements(any()) }
        .thenThrow(Status.NOT_FOUND.withDescription("Measurement not found").asRuntimeException())
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.batchGetMeasurements(
              batchGetMeasurementsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                names += MEASUREMENT_NAME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      Llv2ProtocolConfig.setForTest(
        LLV2_INTERNAL_PROTOCOL_CONFIG.liquidLegionsV2,
        LLV2_DUCHY_PROTOCOL_CONFIG.liquidLegionsV2,
        setOf("aggregator"),
        2,
      )
      RoLlv2ProtocolConfig.setForTest(
        RO_LLV2_INTERNAL_PROTOCOL_CONFIG.reachOnlyLiquidLegionsV2,
        RO_LLV2_DUCHY_PROTOCOL_CONFIG.reachOnlyLiquidLegionsV2,
        setOf("aggregator"),
        2,
      )
      HmssProtocolConfig.setForTest(
        HMSS_INTERNAL_PROTOCOL_CONFIG.honestMajorityShareShuffle,
        "worker1",
        "worker2",
        "aggregator",
      )
      TrusTeeProtocolConfig.setForTest(TRUS_TEE_INTERNAL_PROTOCOL_CONFIG.trusTee, "aggregator")
    }

    private val API_VERSION = Version.V2_ALPHA

    private val NOISE_MECHANISMS =
      listOf(
        ProtocolConfig.NoiseMechanism.NONE,
        ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE,
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )

    private val DIFFERENTIAL_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1.0
    }

    private val LLV2_INTERNAL_PROTOCOL_CONFIG = internalProtocolConfig {
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
          noiseMechanism = InternalNoiseMechanism.GEOMETRIC
        }
    }

    private val LLV2_PUBLIC_PROTOCOL_CONFIG = protocolConfig {
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
            noiseMechanism = ProtocolConfig.NoiseMechanism.GEOMETRIC
          }
        }
    }

    private val LLV2_DUCHY_PROTOCOL_CONFIG = duchyProtocolConfig {
      liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
    }

    private val RO_LLV2_INTERNAL_PROTOCOL_CONFIG = internalProtocolConfig {
      externalProtocolConfigId = "rollv2"
      reachOnlyLiquidLegionsV2 =
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
          noiseMechanism = InternalNoiseMechanism.GEOMETRIC
        }
    }

    private val RO_LLV2_PUBLIC_PROTOCOL_CONFIG = protocolConfig {
      measurementType = ProtocolConfig.MeasurementType.REACH
      protocols +=
        ProtocolConfigKt.protocol {
          reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
            sketchParams = reachOnlyLiquidLegionsSketchParams {
              decayRate = 1.1
              maxSize = 100
            }
            dataProviderNoise = differentialPrivacyParams {
              epsilon = 2.1
              delta = 3.3
            }
            noiseMechanism = ProtocolConfig.NoiseMechanism.GEOMETRIC
          }
        }
    }

    private val RO_LLV2_DUCHY_PROTOCOL_CONFIG = duchyProtocolConfig {
      reachOnlyLiquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
    }

    private val HMSS_INTERNAL_PROTOCOL_CONFIG = internalProtocolConfig {
      externalProtocolConfigId = "hmss"
      honestMajorityShareShuffle = InternalProtocolConfigKt.honestMajorityShareShuffle {}
    }

    private val TRUS_TEE_INTERNAL_PROTOCOL_CONFIG = internalProtocolConfig {
      externalProtocolConfigId = "trustee"
      trusTee = InternalProtocolConfigKt.trusTee {}
    }

    private val DATA_PROVIDER_PUBLIC_KEY = encryptionPublicKey { data = UPDATE_TIME.toByteString() }
    private val MEASUREMENT_PUBLIC_KEY = encryptionPublicKey { data = UPDATE_TIME.toByteString() }

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MEASUREMENT_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1.0
        }
        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1.0
        }
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval { width = 1.0f }
      nonceHashes += EXTERNAL_DATA_PROVIDER_IDS.map { it.value.toByteString() }
    }

    private val IMPRESSION_MEASUREMENT_SPEC =
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

    private val DURATION_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        clearReachAndFrequency()
        duration = duration {
          privacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 0.0
          }
          maximumWatchDurationPerUser = Durations.fromMinutes(5)
        }
      }

    private val WRAPPING_INTERVAL_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval {
          start = 0.5f
          width = 0.8f
        }
      }

    private val MEASUREMENT = measurement {
      name = MEASUREMENT_NAME
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signedMessage {
        setMessage(MEASUREMENT_SPEC.pack())
        signature = UPDATE_TIME.toByteString()
        signatureAlgorithmOid = "2.9999"
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
              dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
              encryptedRequisitionSpec = encryptedMessage {
                ciphertext = UPDATE_TIME.toByteString()
                typeUrl = ProtoReflection.getTypeUrl(SignedMessage.getDescriptor())
              }
              nonceHash = DATA_PROVIDER_NONCE_HASH
            }
          }
        }
      protocolConfig = LLV2_PUBLIC_PROTOCOL_CONFIG
      measurementReferenceId = "ref_id"
      failure = failure {
        reason = Failure.Reason.CERTIFICATE_REVOKED
        message = "Measurement Consumer Certificate has been revoked"
      }
      results += resultOutput {
        certificate = DATA_PROVIDERS_RESULT_CERTIFICATE_NAME
        encryptedResult = ENCRYPTED_RESULT
      }
      results += resultOutput {
        certificate = DUCHY_CERTIFICATE_NAME
        encryptedResult = ENCRYPTED_RESULT
      }
      createTime = CREATE_TIME
      updateTime = UPDATE_TIME
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
      createTime = CREATE_TIME
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
              dataProviderPublicKey = it.value.dataProviderPublicKey.value
              encryptedRequisitionSpec = it.value.encryptedRequisitionSpec.ciphertext
              nonceHash = it.value.nonceHash
            }
          },
        )
      )
      details = measurementDetails {
        apiVersion = API_VERSION.string
        measurementSpec = MEASUREMENT.measurementSpec.message.value
        measurementSpecSignature = MEASUREMENT.measurementSpec.signature
        measurementSpecSignatureAlgorithmOid = MEASUREMENT.measurementSpec.signatureAlgorithmOid
        protocolConfig = LLV2_INTERNAL_PROTOCOL_CONFIG
        duchyProtocolConfig = LLV2_DUCHY_PROTOCOL_CONFIG
        failure = internalMeasurementFailure {
          reason = InternalMeasurementFailure.Reason.CERTIFICATE_REVOKED
          message = MEASUREMENT.failure.message
        }
      }
      results += resultInfo {
        externalAggregatorDuchyId = DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!.duchyId
        externalCertificateId =
          apiIdToExternalId(DuchyCertificateKey.fromName(DUCHY_CERTIFICATE_NAME)!!.certificateId)
        encryptedResult = ENCRYPTED_DATA
        apiVersion = API_VERSION.string
      }
      results += resultInfo {
        externalDataProviderId =
          apiIdToExternalId(
            DataProviderCertificateKey.fromName(DATA_PROVIDERS_RESULT_CERTIFICATE_NAME)!!
              .dataProviderId
          )
        externalCertificateId =
          apiIdToExternalId(
            DataProviderCertificateKey.fromName(DATA_PROVIDERS_RESULT_CERTIFICATE_NAME)!!
              .certificateId
          )
        encryptedResult = ENCRYPTED_DATA
        apiVersion = API_VERSION.string
      }
    }

    private val REACH_ONLY_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        clearReachAndFrequency()
        reach = reach { privacyParams = DIFFERENTIAL_PRIVACY_PARAMS }
      }

    private val REACH_ONLY_MEASUREMENT =
      MEASUREMENT.copy {
        measurementSpec = measurementSpec.copy { setMessage(REACH_ONLY_MEASUREMENT_SPEC.pack()) }
        protocolConfig = RO_LLV2_PUBLIC_PROTOCOL_CONFIG
      }

    private val REACH_ONLY_INTERNAL_MEASUREMENT =
      INTERNAL_MEASUREMENT.copy {
        details = measurementDetails {
          apiVersion = API_VERSION.string
          measurementSpec = REACH_ONLY_MEASUREMENT.measurementSpec.message.value
          measurementSpecSignature = REACH_ONLY_MEASUREMENT.measurementSpec.signature
          measurementSpecSignatureAlgorithmOid =
            REACH_ONLY_MEASUREMENT.measurementSpec.signatureAlgorithmOid
          protocolConfig = RO_LLV2_INTERNAL_PROTOCOL_CONFIG
          duchyProtocolConfig = RO_LLV2_DUCHY_PROTOCOL_CONFIG
          failure = internalMeasurementFailure {
            reason = InternalMeasurementFailure.Reason.CERTIFICATE_REVOKED
            message = MEASUREMENT.failure.message
          }
        }
      }
    private val DEFAULT_INTERNAL_DIRECT_NOISE_MECHANISMS: List<InternalNoiseMechanism> =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )

    private val DEFAULT_INTERNAL_DIRECT_REACH_AND_FREQUENCY_PROTOCOL_CONFIG:
      InternalProtocolConfig.Direct =
      direct {
        noiseMechanisms += DEFAULT_INTERNAL_DIRECT_NOISE_MECHANISMS
        customDirectMethodology =
          InternalProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
        deterministicCountDistinct =
          InternalProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
        liquidLegionsCountDistinct =
          InternalProtocolConfig.Direct.LiquidLegionsCountDistinct.getDefaultInstance()
        deterministicDistribution =
          InternalProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
        liquidLegionsDistribution =
          InternalProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
      }

    private val DEFAULT_INTERNAL_DIRECT_IMPRESSION_PROTOCOL_CONFIG: InternalProtocolConfig.Direct =
      direct {
        noiseMechanisms += DEFAULT_INTERNAL_DIRECT_NOISE_MECHANISMS
        customDirectMethodology =
          InternalProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
        deterministicCount = InternalProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
      }

    private val DEFAULT_INTERNAL_DIRECT_WATCH_DURATION_PROTOCOL_CONFIG:
      InternalProtocolConfig.Direct =
      direct {
        noiseMechanisms += DEFAULT_INTERNAL_DIRECT_NOISE_MECHANISMS
        customDirectMethodology =
          InternalProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
        deterministicSum = InternalProtocolConfig.Direct.DeterministicSum.getDefaultInstance()
      }

    private val DEFAULT_INTERNAL_DIRECT_POPULATION_PROTOCOL_CONFIG: InternalProtocolConfig.Direct =
      direct {
        noiseMechanisms += DEFAULT_INTERNAL_DIRECT_NOISE_MECHANISMS
        deterministicCount = InternalProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
      }

    private const val BATCH_LIMIT = 50
  }
}
