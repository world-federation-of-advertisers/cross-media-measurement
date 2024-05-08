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
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.deleteApiKeyRequest
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.ApiKey as InternalApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineImplBase as InternalApiKeysCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub as InternalApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.apiKey as internalApiKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteApiKeyRequest as internalDeleteApiKeyRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ApiKeyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException

private const val ACCOUNT_NAME = "accounts/AAAAAAC8YU4"
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID_2 = 155L
private const val AUTHENTICATION_KEY = 12345672L
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID)).toName()
private val MEASUREMENT_CONSUMER_NAME_2 =
  MeasurementConsumerKey(externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID_2)).toName()
private val API_KEY_NAME = "$MEASUREMENT_CONSUMER_NAME/apiKeys/AAAAAAAAAMs"
private val API_KEY_NAME_2 = "$MEASUREMENT_CONSUMER_NAME_2/apiKeys/AAAAAAAAANs"
private val EXTERNAL_API_KEY_ID = apiIdToExternalId(ApiKeyKey.fromName(API_KEY_NAME)!!.apiKeyId)

@RunWith(JUnit4::class)
class ApiKeysServiceTest {
  private val internalApiKeysMock: InternalApiKeysCoroutineImplBase =
    mockService() {
      onBlocking { createApiKey(any()) }.thenReturn(INTERNAL_API_KEY)
      onBlocking { deleteApiKey(any()) }.thenReturn(INTERNAL_API_KEY)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalApiKeysMock) }

  private lateinit var service: ApiKeysService

  @Before
  fun initClient() {
    service = ApiKeysService(InternalApiKeysCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createApiKey returns api key`() {
    val request = createApiKeyRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      apiKey = PUBLIC_API_KEY
    }

    val result = withAccount(INTERNAL_ACCOUNT) { runBlocking { service.createApiKey(request) } }

    assertThat(result).isEqualTo(PUBLIC_API_KEY)

    verifyProtoArgument(internalApiKeysMock, InternalApiKeysCoroutineImplBase::createApiKey)
      .isEqualTo(
        INTERNAL_API_KEY.copy {
          clearExternalApiKeyId()
          clearAuthenticationKey()
        }
      )
  }

  @Test
  fun `createApiKey throws INVALID_ARGUMENT when measurement consumer name is invalid`() {
    val request = createApiKeyRequest {
      parent = "blaze"
      apiKey = PUBLIC_API_KEY
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.createApiKey(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createApiKey throws UNAUTHENTICATED when credentials are missing`() {
    val request = createApiKeyRequest {}

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createApiKey(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createApiKey throws PERMISSION_DENIED when account doesn't own measurement consumer`() {
    val request = createApiKeyRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      apiKey = PUBLIC_API_KEY
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.createApiKey(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteApiKey returns api key`() = runBlocking {
    val request = deleteApiKeyRequest { name = API_KEY_NAME }

    val result = withAccount(INTERNAL_ACCOUNT) { runBlocking { service.deleteApiKey(request) } }

    assertThat(result).isEqualTo(PUBLIC_API_KEY)

    verifyProtoArgument(internalApiKeysMock, InternalApiKeysCoroutineImplBase::deleteApiKey)
      .isEqualTo(
        internalDeleteApiKeyRequest {
          val key = ApiKeyKey.fromName(API_KEY_NAME)
          externalMeasurementConsumerId = apiIdToExternalId(key!!.measurementConsumerId)
          externalApiKeyId = apiIdToExternalId(key.apiKeyId)
        }
      )
  }

  @Test
  fun `deleteApiKey throws INVALID_ARGUMENT when name is invalid`() {
    val request = deleteApiKeyRequest { name = "asdfa" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.deleteApiKey(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteApiKey throws UNAUTHENTICATED when credentials are missing`() {
    val request = deleteApiKeyRequest {}

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.deleteApiKey(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteApiKey throws PERMISSION_DENIED when account doesn't own measurement consumer`() {
    val request = deleteApiKeyRequest { name = API_KEY_NAME_2 }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.deleteApiKey(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createApiKey throws NOT_FOUND with measurment consumer name when measurement consumer is not found`() {
    internalApiKeysMock.stub {
      onBlocking { createApiKey(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
        )
    }
    val request = createApiKeyRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      apiKey = PUBLIC_API_KEY
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.createApiKey(request) } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumer", MEASUREMENT_CONSUMER_NAME)
  }

  @Test
  fun `deleteApiKey throws NOT_FOUND with api key id when api key is not found`() {
    internalApiKeysMock.stub {
      onBlocking { deleteApiKey(any()) }
        .thenThrow(
          ApiKeyNotFoundException(ExternalId(EXTERNAL_API_KEY_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "Api Key not found.")
        )
    }
    val request = deleteApiKeyRequest { name = API_KEY_NAME }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withAccount(INTERNAL_ACCOUNT) { runBlocking { service.deleteApiKey(request) } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("externalApiKeyId", ExternalId(EXTERNAL_API_KEY_ID).apiId.value)
  }
}

private val PUBLIC_API_KEY: ApiKey = apiKey {
  name = API_KEY_NAME
  nickname = "nickname"
  description = "description"
  authenticationKey = externalIdToApiId(AUTHENTICATION_KEY)
}

private val INTERNAL_API_KEY: InternalApiKey = internalApiKey {
  val key = ApiKeyKey.fromName(API_KEY_NAME)
  externalMeasurementConsumerId = apiIdToExternalId(key!!.measurementConsumerId)
  externalApiKeyId = apiIdToExternalId(key.apiKeyId)
  nickname = "nickname"
  description = "description"
  authenticationKey = AUTHENTICATION_KEY
}

private val INTERNAL_ACCOUNT: InternalAccount = internalAccount {
  externalAccountId = apiIdToExternalId(AccountKey.fromName(ACCOUNT_NAME)!!.accountId)
  externalOwnedMeasurementConsumerIds +=
    apiIdToExternalId(ApiKeyKey.fromName(API_KEY_NAME)!!.measurementConsumerId)
}
