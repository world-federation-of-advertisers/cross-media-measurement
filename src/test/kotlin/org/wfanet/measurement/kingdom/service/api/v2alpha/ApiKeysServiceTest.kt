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
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeyKey
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.deleteApiKeyRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase as InternalAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ApiKey as InternalApiKey
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineImplBase as InternalApiKeysCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub as InternalApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.apiKey as internalApiKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.revokeApiKeyRequest

private const val ACCOUNT_NAME = "accounts/AAAAAAC8YU4"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/AAAAAAAAAJs"
private const val API_KEY_NAME = "$MEASUREMENT_CONSUMER_NAME/apiKeys/AAAAAAAAAMs"
private const val API_KEY_NAME_2 = "$MEASUREMENT_CONSUMER_NAME_2/apiKeys/AAAAAAAAANs"
private const val ID_TOKEN = "id_token"
private const val AUTHENTICATION_KEY = 12345672L

@RunWith(JUnit4::class)
class ApiKeysServiceTest {
  private val internalApiKeysMock: InternalApiKeysCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { createApiKey(any()) }.thenReturn(INTERNAL_API_KEY)
      onBlocking { revokeApiKey(any()) }.thenReturn(INTERNAL_API_KEY)
    }

  private val internalAccountsMock: InternalAccountsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { authenticateAccount(any()) }.thenReturn(INTERNAL_ACCOUNT)
    }

  @get:Rule
  val internalGrpcTestServerRule = GrpcTestServerRule {
    addService(internalApiKeysMock)
    addService(internalAccountsMock)
  }

  @get:Rule
  var publicGrpcTestServerRule = GrpcTestServerRule {
    val internalApiKeysCoroutineStub =
      InternalApiKeysCoroutineStub(internalGrpcTestServerRule.channel)
    val internalAccountsCoroutineStub =
      AccountsGrpcKt.AccountsCoroutineStub(internalGrpcTestServerRule.channel)
    val service = ApiKeysService(internalApiKeysCoroutineStub)
    addService(service.withAccountAuthenticationServerInterceptor(internalAccountsCoroutineStub))
  }

  private lateinit var client: ApiKeysCoroutineStub

  @Before
  fun initClient() {
    client = ApiKeysCoroutineStub(publicGrpcTestServerRule.channel)
  }

  @Test
  fun `createApiKey returns api key`() {
    val request = createApiKeyRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      apiKey = PUBLIC_API_KEY
    }

    val result = runBlocking { client.withIdToken(ID_TOKEN).createApiKey(request) }

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
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).createApiKey(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Measurement Consumer resource name unspecified or invalid")
  }

  @Test
  fun `createApiKey throws UNAUTHENTICATED when credentials are missing`() {
    val request = createApiKeyRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.createApiKey(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `createApiKey throws PERMISSION_DENIED when account doesn't own measurement consumer`() {
    val request = createApiKeyRequest {
      parent = MEASUREMENT_CONSUMER_NAME_2
      apiKey = PUBLIC_API_KEY
    }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).createApiKey(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Account doesn't own Measurement Consumer")
  }

  @Test
  fun `deleteApiKey returns api key`() = runBlocking {
    val request = deleteApiKeyRequest { name = API_KEY_NAME }

    val result = client.withIdToken(ID_TOKEN).deleteApiKey(request)

    assertThat(result).isEqualTo(PUBLIC_API_KEY)

    verifyProtoArgument(internalApiKeysMock, InternalApiKeysCoroutineImplBase::revokeApiKey)
      .isEqualTo(
        revokeApiKeyRequest {
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
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).deleteApiKey(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `deleteApiKey throws UNAUTHENTICATED when credentials are missing`() {
    val request = deleteApiKeyRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.deleteApiKey(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `deleteApiKey throws PERMISSION_DENIED when account doesn't own measurement consumer`() {
    val request = deleteApiKeyRequest { name = API_KEY_NAME_2 }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).deleteApiKey(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Account doesn't own Measurement Consumer")
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
