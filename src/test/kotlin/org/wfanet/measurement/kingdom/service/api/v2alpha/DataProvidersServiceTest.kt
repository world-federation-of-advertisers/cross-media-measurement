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
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.copy
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.security.cert.X509Certificate
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.DataProvider as InternalDataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt as InternalDataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as InternalDataProvidersService
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersClient
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.certificateDetails
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest as internalGetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalRequest as internalReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.replaceDataAvailabilityIntervalsRequest as internalReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderCapabilitiesRequest as internalReplaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.internal.kingdom.replaceDataProviderRequiredDuchiesRequest as internalReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException

private const val DATA_PROVIDER_ID = 123L
private const val DATA_PROVIDER_ID_2 = 124L
private const val CERTIFICATE_ID = 456L

private val DATA_PROVIDER_NAME = makeDataProvider(DATA_PROVIDER_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(DATA_PROVIDER_ID_2)
private val CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
private const val EXTERNAL_DUCHY_ID = "worker1"
private val DUCHY_KEY = DuchyKey(EXTERNAL_DUCHY_ID)
private val DUCHY_NAME = DUCHY_KEY.toName()
private val DUCHY_NAMES = listOf(DUCHY_NAME)
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

@RunWith(JUnit4::class)
class DataProvidersServiceTest {
  private val internalServiceMock: InternalDataProvidersService = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
    onBlocking { replaceDataProviderRequiredDuchies(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
    onBlocking { replaceDataAvailabilityInterval(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
    onBlocking { replaceDataAvailabilityIntervals(any()) }.thenReturn(INTERNAL_DATA_PROVIDER)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: DataProvidersService

  @Before
  fun initService() {
    service = DataProvidersService(InternalDataProvidersClient(grpcTestServerRule.channel))
  }

  @Test
  fun `get with edp caller returns resource`() {
    val dataProvider =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(dataProvider).isEqualTo(DATA_PROVIDER)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(internalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get with mc caller returns resource`() {
    val dataProvider =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(dataProvider).isEqualTo(DATA_PROVIDER)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(internalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get with model provider caller returns resource`() {
    val dataProvider =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }

    assertThat(dataProvider).isEqualTo(DATA_PROVIDER)
    verifyProtoArgument(internalServiceMock, InternalDataProvidersService::getDataProvider)
      .isEqualTo(internalGetDataProviderRequest { externalDataProviderId = DATA_PROVIDER_ID })
  }

  @Test
  fun `get throws UNAUTHENTICATED when no principal found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `get throws PERMISSION_DENIED when edp caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws PERMISSION_DENIED when principal with no authorization found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getDataProvider(getDataProviderRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `get throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getDataProvider(getDataProviderRequest { name = "foo" }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies from edp succeeds`() {
    val dataProvider =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking {
          service.replaceDataProviderRequiredDuchies(
            replaceDataProviderRequiredDuchiesRequest {
              name = DATA_PROVIDER_NAME
              requiredDuchies += DUCHY_NAMES
            }
          )
        }
      }

    assertThat(dataProvider).isEqualTo(DATA_PROVIDER)
    verifyProtoArgument(
        internalServiceMock,
        InternalDataProvidersService::replaceDataProviderRequiredDuchies,
      )
      .isEqualTo(
        internalReplaceDataProviderRequiredDuchiesRequest {
          externalDataProviderId = DATA_PROVIDER_ID
          requiredExternalDuchyIds += EXTERNAL_DUCHY_ID
        }
      )
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = "foo"
                requiredDuchies += DUCHY_NAMES
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws INVALID_ARGUMENT when requiredDuchies is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = DATA_PROVIDER_NAME
                requiredDuchies += listOf("worker1")
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws UNAUTHENTICATED when no principal found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.replaceDataProviderRequiredDuchies(
            replaceDataProviderRequiredDuchiesRequest {
              name = DATA_PROVIDER_NAME
              requiredDuchies += DUCHY_NAMES
            }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws PERMISSION_DENIED when edp caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = DATA_PROVIDER_NAME
                requiredDuchies += DUCHY_NAMES
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws PERMISSION_DENIED with mc caller`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = DATA_PROVIDER_NAME
                requiredDuchies += DUCHY_NAMES
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws PERMISSION_DENIED with model provider caller`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = DATA_PROVIDER_NAME
                requiredDuchies += DUCHY_NAMES
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataAvailabilityInterval returns data provider`() {
    val newDataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 300 }
      endTime = timestamp { seconds = 500 }
    }

    runBlocking {
      whenever(internalServiceMock.replaceDataAvailabilityInterval(any()))
        .thenReturn(
          INTERNAL_DATA_PROVIDER.copy {
            details =
              INTERNAL_DATA_PROVIDER.details.copy {
                dataAvailabilityInterval = newDataAvailabilityInterval
              }
          }
        )
    }

    val dataProvider =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking {
          service.replaceDataAvailabilityInterval(
            replaceDataAvailabilityIntervalRequest {
              name = DATA_PROVIDER_NAME
              dataAvailabilityInterval = newDataAvailabilityInterval
            }
          )
        }
      }

    assertThat(dataProvider)
      .isEqualTo(DATA_PROVIDER.copy { dataAvailabilityInterval = newDataAvailabilityInterval })

    verifyProtoArgument(
        internalServiceMock,
        InternalDataProvidersService::replaceDataAvailabilityInterval,
      )
      .isEqualTo(
        internalReplaceDataAvailabilityIntervalRequest {
          externalDataProviderId = DATA_PROVIDER_ID
          dataAvailabilityInterval = newDataAvailabilityInterval
        }
      )
  }

  @Test
  fun `replaceDataAvailabilityInterval throws INVALID_ARGUMENT when name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = "foo"
                dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws INVALID_ARGUMENT when start_time missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = interval { endTime = timestamp { seconds = 500 } }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Both")
  }

  @Test
  fun `replaceDataAvailabilityInterval throws INVALID_ARGUMENT when end_time missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = interval { startTime = timestamp { seconds = 300 } }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Both")
  }

  @Test
  fun `replaceDataAvailabilityInterval throws INVALID_ARGUMENT when end_time invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 300 }
                  endTime = timestamp { seconds = 200 }
                }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("before")
  }

  @Test
  fun `replaceDataAvailabilityInterval throws UNAUTHENTICATED when no principal found`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.replaceDataAvailabilityInterval(
            replaceDataAvailabilityIntervalRequest {
              name = DATA_PROVIDER_NAME
              dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
            }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws PERMISSION_DENIED when edp caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws PERMISSION_DENIED when mc caller`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws PERMISSION_DENIED when model provider caller`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataAvailabilityIntervals returns data provider`() {
    val dataAvailabilityIntervals = DATA_PROVIDER.dataAvailabilityIntervalsList

    val response = runBlocking {
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        service.replaceDataAvailabilityIntervals(
          replaceDataAvailabilityIntervalsRequest {
            name = DATA_PROVIDER_NAME
            this.dataAvailabilityIntervals += dataAvailabilityIntervals
          }
        )
      }
    }

    verifyProtoArgument(
        internalServiceMock,
        InternalDataProvidersService::replaceDataAvailabilityIntervals,
      )
      .isEqualTo(
        internalReplaceDataAvailabilityIntervalsRequest {
          externalDataProviderId = DATA_PROVIDER_ID
          this.dataAvailabilityIntervals += INTERNAL_DATA_PROVIDER.dataAvailabilityIntervalsList
        }
      )
    assertThat(response).isEqualTo(DATA_PROVIDER)
  }

  @Test
  fun `replaceDataAvailabilityIntervals throws INVALID_ARGUMENT when ModelLine name is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          withDataProviderPrincipal(DATA_PROVIDER_NAME) {
            service.replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityIntervals +=
                  DATA_PROVIDER.dataAvailabilityIntervalsList.first().copy {
                    key = "bogus-model-line"
                  }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("ModelLine")
  }

  @Test
  fun `replaceDataAvailabilityIntervals throws INVALID_ARGUMENT when end_time not set`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          withDataProviderPrincipal(DATA_PROVIDER_NAME) {
            service.replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityIntervals +=
                  DATA_PROVIDER.dataAvailabilityIntervalsList.first().copy {
                    value = value.copy { clearEndTime() }
                  }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("end_time")
  }

  @Test
  fun `replaceDataAvailabilityIntervals throws INVALID_ARGUMENT when end_time is before start_time`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          withDataProviderPrincipal(DATA_PROVIDER_NAME) {
            service.replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityIntervals +=
                  DATA_PROVIDER.dataAvailabilityIntervalsList.first().copy {
                    value =
                      value.copy { startTime = Timestamps.add(endTime, duration { seconds = 10 }) }
                  }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("start_time")
    assertThat(exception).hasMessageThat().contains("end_time")
  }

  @Test
  fun `replaceDataAvailabilityIntervals throws PERMISSION_DENIED when principal does not match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
            service.replaceDataAvailabilityIntervals(
              replaceDataAvailabilityIntervalsRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityIntervals += DATA_PROVIDER.dataAvailabilityIntervalsList
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceDataProviderCapabilities returns updated DataProvider`() {
    val internalDataProvider =
      INTERNAL_DATA_PROVIDER.copy {
        details =
          details.copy {
            capabilities = capabilities.copy { honestMajorityShareShuffleSupported = true }
          }
      }
    internalServiceMock.stub {
      onBlocking { replaceDataProviderCapabilities(any()) }.thenReturn(internalDataProvider)
    }
    val request = replaceDataProviderCapabilitiesRequest {
      name = DATA_PROVIDER_NAME
      capabilities = DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true }
    }

    val response: DataProvider = runBlocking {
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        service.replaceDataProviderCapabilities(request)
      }
    }

    assertThat(response).isEqualTo(DATA_PROVIDER.copy { capabilities = request.capabilities })
    verifyProtoArgument(
        internalServiceMock,
        InternalDataProvidersService::replaceDataProviderCapabilities,
      )
      .isEqualTo(
        internalReplaceDataProviderCapabilitiesRequest {
          externalDataProviderId = internalDataProvider.externalDataProviderId
          capabilities = internalDataProvider.details.capabilities
        }
      )
  }

  @Test
  fun `replaceDataProviderCapabilities throws PERMISSION_DENIED for incorrect principal`() {
    val request = replaceDataProviderCapabilitiesRequest {
      name = DATA_PROVIDER_NAME
      capabilities = DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
            service.replaceDataProviderCapabilities(request)
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getDataProvider throws NOT_FOUND with data provider name when data provider not found`() {
    internalServiceMock.stub {
      onBlocking { getDataProvider(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.getDataProvider(getDataProviderRequest { name = DATA_PROVIDER_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `replaceDataProviderRequiredDuchies throws NOT_FOUND with data provider name when data provider not found`() {
    internalServiceMock.stub {
      onBlocking { replaceDataProviderRequiredDuchies(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataProviderRequiredDuchies(
              replaceDataProviderRequiredDuchiesRequest {
                name = DATA_PROVIDER_NAME
                requiredDuchies += DUCHY_NAMES
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `replaceDataAvailabilityInterval throws NOT_FOUND with data provider name when data provider not found`() {
    internalServiceMock.stub {
      onBlocking { replaceDataAvailabilityInterval(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataAvailabilityInterval(
              replaceDataAvailabilityIntervalRequest {
                name = DATA_PROVIDER_NAME
                dataAvailabilityInterval = DATA_PROVIDER.dataAvailabilityInterval
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `replaceDataProviderCapabilities throws NOT_FOUND with data provider name when data provider not found`() {
    internalServiceMock.stub {
      onBlocking { replaceDataProviderCapabilities(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.replaceDataProviderCapabilities(
              replaceDataProviderCapabilitiesRequest {
                name = DATA_PROVIDER_NAME
                capabilities =
                  DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  companion object {
    private val API_VERSION = Version.V2_ALPHA

    private val serverCertificate: X509Certificate =
      readCertificate(TestData.FIXED_SERVER_CERT_PEM_FILE)
    private val SERVER_CERTIFICATE_DER = serverCertificate.encoded.toByteString()

    private val ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(TestData.FIXED_ENCRYPTION_PUBLIC_KEYSET).toEncryptionPublicKey()
    private val SIGNED_PUBLIC_KEY = signedMessage {
      setMessage(ENCRYPTION_PUBLIC_KEY.pack())
      signature = ByteString.copyFromUtf8("Fake signature of public key")
      signatureAlgorithmOid = "2.9999"
    }

    private val INTERNAL_DATA_PROVIDER: InternalDataProvider = internalDataProvider {
      externalDataProviderId = DATA_PROVIDER_ID
      dataAvailabilityIntervals +=
        InternalDataProviderKt.dataAvailabilityMapEntry {
          key = modelLineKey {
            externalModelProviderId = 1234L
            externalModelSuiteId = 2345L
            externalModelLineId = 3456L
          }
          value = interval {
            startTime =
              LocalDate.of(2025, 1, 10).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            endTime =
              LocalDate.of(2025, 4, 10).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
          }
        }
      details = dataProviderDetails {
        apiVersion = API_VERSION.string
        publicKey = SIGNED_PUBLIC_KEY.message.value
        publicKeySignature = SIGNED_PUBLIC_KEY.signature
        publicKeySignatureAlgorithmOid = SIGNED_PUBLIC_KEY.signatureAlgorithmOid
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
      }
      certificate = internalCertificate {
        externalDataProviderId = DATA_PROVIDER_ID
        externalCertificateId = CERTIFICATE_ID
        subjectKeyIdentifier = serverCertificate.subjectKeyIdentifier!!
        notValidBefore = serverCertificate.notBefore.toInstant().toProtoTime()
        notValidAfter = serverCertificate.notAfter.toInstant().toProtoTime()
        details = certificateDetails { x509Der = SERVER_CERTIFICATE_DER }
      }
      requiredExternalDuchyIds += EXTERNAL_DUCHY_ID
    }

    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = CERTIFICATE_NAME
      certificateDer = SERVER_CERTIFICATE_DER
      publicKey = SIGNED_PUBLIC_KEY
      requiredDuchies += DUCHY_NAMES
      dataAvailabilityIntervals +=
        DataProviderKt.dataAvailabilityMapEntry {
          key = "modelProviders/AAAAAAAABNI/modelSuites/AAAAAAAACSk/modelLines/AAAAAAAADYA"
          value = INTERNAL_DATA_PROVIDER.dataAvailabilityIntervalsList.first().value
        }
      dataAvailabilityInterval = INTERNAL_DATA_PROVIDER.details.dataAvailabilityInterval
      capabilities = DataProvider.Capabilities.getDefaultInstance()
    }
  }
}

// private fun ReadableInstant.toProtoTime(): Timestamp {
//  return toInstant().toProtoTime()
// }
