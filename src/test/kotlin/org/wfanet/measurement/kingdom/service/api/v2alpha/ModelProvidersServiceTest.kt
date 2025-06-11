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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.GetModelProviderRequest
import org.wfanet.measurement.api.v2alpha.ModelProvider
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.getModelProviderRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersRequest
import org.wfanet.measurement.api.v2alpha.listModelProvidersResponse
import org.wfanet.measurement.api.v2alpha.modelProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ListModelProvidersPageTokenKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt as InternalModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.getModelProviderRequest as internalGetModelProviderRequest
import org.wfanet.measurement.internal.kingdom.listModelProvidersPageToken as internalListModelProvidersPageToken
import org.wfanet.measurement.internal.kingdom.listModelProvidersRequest as internalListModelProvidersRequest
import org.wfanet.measurement.internal.kingdom.listModelProvidersResponse as internalListModelProvidersResponse
import org.wfanet.measurement.internal.kingdom.modelProvider as internalModelProvider

@RunWith(JUnit4::class)
class ModelProvidersServiceTest {
  private val internalModelProvidersMock =
    mockService<InternalModelProvidersGrpcKt.ModelProvidersCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalModelProvidersMock) }

  private lateinit var service: ModelProvidersService

  @Before
  fun initService() {
    service =
      ModelProvidersService(
        InternalModelProvidersGrpcKt.ModelProvidersCoroutineStub(grpcTestServer.channel)
      )
  }

  @Test
  fun `getModelProvider returns ModelProvider when ModelProvider caller is found`() = runBlocking {
    val internalModelProvider = internalModelProvider {
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
    }
    internalModelProvidersMock.stub {
      onBlocking { getModelProvider(any()) } doReturn internalModelProvider
    }

    val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }
    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) { service.getModelProvider(request) }

    verifyProtoArgument(
        internalModelProvidersMock,
        InternalModelProvidersGrpcKt.ModelProvidersCoroutineImplBase::getModelProvider,
      )
      .isEqualTo(
        internalGetModelProviderRequest { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
      )
    assertThat(result).isEqualTo(MODEL_PROVIDER)
  }

  @Test
  fun `getModelProvider returns ModelProvider when DataProvider caller is found`() = runBlocking {
    val internalModelProvider = internalModelProvider {
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
    }
    internalModelProvidersMock.stub {
      onBlocking { getModelProvider(any()) } doReturn internalModelProvider
    }

    val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }
    val result = withDataProviderPrincipal(DATA_PROVIDER_NAME) { service.getModelProvider(request) }

    verifyProtoArgument(
        internalModelProvidersMock,
        InternalModelProvidersGrpcKt.ModelProvidersCoroutineImplBase::getModelProvider,
      )
      .isEqualTo(
        internalGetModelProviderRequest { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
      )
    assertThat(result).isEqualTo(MODEL_PROVIDER)
  }

  @Test
  fun `getModelProvider returns ModelProvider when MeasurementConsumer caller is found`() =
    runBlocking {
      val internalModelProvider = internalModelProvider {
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      }
      internalModelProvidersMock.stub {
        onBlocking { getModelProvider(any()) } doReturn internalModelProvider
      }

      val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          service.getModelProvider(request)
        }

      verifyProtoArgument(
          internalModelProvidersMock,
          InternalModelProvidersGrpcKt.ModelProvidersCoroutineImplBase::getModelProvider,
        )
        .isEqualTo(
          internalGetModelProviderRequest { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
        )
      assertThat(result).isEqualTo(MODEL_PROVIDER)
    }

  @Test
  fun `getModelProvider throws PERMISSION_DENIED when Principal is duchy`() = runBlocking {
    val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { service.getModelProvider(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getModelProvider throws PERMISSION_DENIED when ModelProvider caller doesn't match`() =
    runBlocking {
      val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) { service.getModelProvider(request) }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `getModelProvider throws UNAUTHENTICATED when no Principal is found`() = runBlocking {
    val request = getModelProviderRequest { name = MODEL_PROVIDER_NAME }

    val exception = assertFailsWith<StatusRuntimeException> { service.getModelProvider(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getModelProvider throws INVALID_ARGUMENT when name is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          service.getModelProvider(GetModelProviderRequest.getDefaultInstance())
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelProviders with page size and page token returns ModelProviders`() = runBlocking {
    val internalModelProvider = internalModelProvider {
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID_2
    }
    val internalListModelProvidersResponse = internalListModelProvidersResponse {
      modelProviders += internalModelProvider
      nextPageToken = internalListModelProvidersPageToken {
        after =
          ListModelProvidersPageTokenKt.after {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID_3
          }
      }
    }
    internalModelProvidersMock.stub {
      onBlocking { listModelProviders(any()) } doReturn internalListModelProvidersResponse
    }

    val internalPagetoken = internalListModelProvidersPageToken {
      after =
        ListModelProvidersPageTokenKt.after { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }
    }
    val request = listModelProvidersRequest {
      pageSize = 1
      pageToken = internalPagetoken.toByteString().base64UrlEncode()
    }
    val result = service.listModelProviders(request)

    verifyProtoArgument(
        internalModelProvidersMock,
        InternalModelProvidersGrpcKt.ModelProvidersCoroutineImplBase::listModelProviders,
      )
      .isEqualTo(
        internalListModelProvidersRequest {
          pageSize = 1
          pageToken = internalPagetoken
        }
      )
    assertThat(result)
      .isEqualTo(
        listModelProvidersResponse {
          modelProviders += internalModelProvider.toModelProvider()
          nextPageToken =
            internalListModelProvidersResponse.nextPageToken.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listModelProviders throws INVALID_ARGUMENT when page size is negative`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listModelProviders(listModelProvidersRequest { pageSize = -1 })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelProviders throws INVALID_ARGUMENT when page token is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listModelProviders(listModelProvidersRequest { pageToken = "mayhem" })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAwAAHs"
    private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
    private const val MODEL_PROVIDER_NAME_2 = "modelProviders/AAAAAAAAAJs"
    private const val MODEL_PROVIDER_NAME_3 = "modelProviders/AAAAAAAABJs"
    private val EXTERNAL_MODEL_PROVIDER_ID =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
    private val EXTERNAL_MODEL_PROVIDER_ID_2 =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME_2)!!.modelProviderId)
    private val EXTERNAL_MODEL_PROVIDER_ID_3 =
      apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME_3)!!.modelProviderId)

    private val MODEL_PROVIDER: ModelProvider = modelProvider { name = MODEL_PROVIDER_NAME }
  }
}
