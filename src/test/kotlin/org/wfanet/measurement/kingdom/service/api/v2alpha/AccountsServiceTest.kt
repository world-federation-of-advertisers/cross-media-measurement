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
import io.grpc.StatusRuntimeException
import java.net.URI
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
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountKt
import org.wfanet.measurement.api.v2alpha.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ReplaceAccountIdentityRequestKt
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.createAccountRequest
import org.wfanet.measurement.api.v2alpha.replaceAccountIdentityRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.Account.ActivationState as InternalActivationState
import org.wfanet.measurement.internal.kingdom.AccountKt as InternalAccountKt
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.activateAccountRequest as internalActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.openIdRequestParams
import org.wfanet.measurement.internal.kingdom.replaceAccountIdentityRequest as internalReplaceAccountIdentityRequest

private const val ACTIVATION_TOKEN = 12345672L
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = 12345673L

private const val EXTERNAL_ACCOUNT_ID = 12345678L
private const val EXTERNAL_CREATOR_ACCOUNT_ID = 56781234L

private const val ACCOUNT_NAME = "accounts/AAAAAAC8YU4"
private const val CREATOR_ACCOUNT_NAME = "accounts/AAAAAANiabI"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"

private const val ID_TOKEN = "id_token"
private const val ID_TOKEN_2 = "id_token_2"
private const val ISSUER = "issuer"
private const val SUBJECT = "subject"

private const val REDIRECT_URI = "https://localhost:2048"
private const val SELF_ISSUED_ISSUER = "https://self-issued.me"

@RunWith(JUnit4::class)
class AccountsServiceTest {
  private val internalAccountsMock: AccountsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { createAccount(any()) }.thenReturn(UNACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { activateAccount(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { replaceAccountIdentity(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { authenticateAccount(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { generateOpenIdRequestParams(any()) }.thenReturn(OPEN_ID_REQUEST_PARAMS)
    }

  @get:Rule val internalGrpcTestServerRule = GrpcTestServerRule { addService(internalAccountsMock) }

  @get:Rule
  var publicGrpcTestServerRule = GrpcTestServerRule {
    val internalAccountsCoroutineStub =
      AccountsGrpcKt.AccountsCoroutineStub(internalGrpcTestServerRule.channel)
    val service = AccountsService(internalAccountsCoroutineStub, REDIRECT_URI)
    addService(service.withAccountAuthenticationServerInterceptor(internalAccountsCoroutineStub))
  }

  private lateinit var client: AccountsCoroutineStub

  @Before
  fun initClient() {
    client = AccountsCoroutineStub(publicGrpcTestServerRule.channel)
  }

  @Test
  fun `createAccount returns unactivated account`() {
    val request = createAccountRequest {
      account =
        account {
          activationParams =
            AccountKt.activationParams { ownedMeasurementConsumer = MEASUREMENT_CONSUMER_NAME }
        }
    }

    val result = runBlocking { client.withIdToken(ID_TOKEN).createAccount(request) }

    assertThat(result).isEqualTo(UNACTIVATED_ACCOUNT)

    verifyProtoArgument(internalAccountsMock, AccountsCoroutineImplBase::createAccount)
      .isEqualTo(
        internalAccount {
          externalCreatorAccountId = EXTERNAL_ACCOUNT_ID
          externalOwnedMeasurementConsumerId =
            UNACTIVATED_INTERNAL_ACCOUNT.externalOwnedMeasurementConsumerId
        }
      )
  }

  @Test
  fun `createAccount throws INVALID_ARGUMENT when owned measurement consumer name is invalid`() {
    val request = createAccountRequest {
      account =
        account {
          activationParams = AccountKt.activationParams { ownedMeasurementConsumer = "43254" }
        }
    }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).createAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Owned Measurement Consumer Resource name invalid")
  }

  @Test
  fun `createAccount throws UNAUTHENTICATED when credentials are missing`() {
    val request = createAccountRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.createAccount(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `createAccount throws UNAUTHENTICATED when credentials are invalid`() = runBlocking {
    whenever(internalAccountsMock.authenticateAccount(any()))
      .thenThrow(StatusRuntimeException(Status.UNAUTHENTICATED))

    val request = createAccountRequest {}

    val exception =
      assertFailsWith<StatusException> { client.withIdToken(ID_TOKEN).createAccount(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `activateAccount returns activated account`() {
    val request = activateAccountRequest {
      name = ACCOUNT_NAME
      activationToken = externalIdToApiId(ACTIVATION_TOKEN)
    }

    val result = runBlocking { client.withIdToken(ID_TOKEN).activateAccount(request) }

    assertThat(result).isEqualTo(ACTIVATED_ACCOUNT)

    verifyProtoArgument(internalAccountsMock, AccountsCoroutineImplBase::activateAccount)
      .isEqualTo(
        internalActivateAccountRequest {
          externalAccountId = EXTERNAL_ACCOUNT_ID
          activationToken = ACTIVATION_TOKEN
        }
      )
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when resource name is missing`() {
    val request = activateAccountRequest { activationToken = externalIdToApiId(ACTIVATION_TOKEN) }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).activateAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when activation token is missing`() {
    val request = activateAccountRequest { name = ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).activateAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Activation token is missing")
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when credentials for new identity are missing`() {
    val request = activateAccountRequest {
      name = ACCOUNT_NAME
      activationToken = externalIdToApiId(ACTIVATION_TOKEN)
    }

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.activateAccount(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Id token is missing")
  }

  @Test
  fun `replaceAccountIdentity with openIdConnectidentity type returns account with same type`() {
    val request = replaceAccountIdentityRequest {
      name = ACCOUNT_NAME
      openId =
        ReplaceAccountIdentityRequestKt.openIdConnectCredentials {
          identityBearerToken = ID_TOKEN_2
        }
    }

    val result = runBlocking { client.withIdToken(ID_TOKEN).replaceAccountIdentity(request) }

    assertThat(result).isEqualTo(ACTIVATED_ACCOUNT)

    verifyProtoArgument(internalAccountsMock, AccountsCoroutineImplBase::replaceAccountIdentity)
      .isEqualTo(internalReplaceAccountIdentityRequest { externalAccountId = EXTERNAL_ACCOUNT_ID })
  }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when resource name is missing`() {
    val request = replaceAccountIdentityRequest {
      openId =
        ReplaceAccountIdentityRequestKt.openIdConnectCredentials {
          identityBearerToken = ID_TOKEN_2
        }
    }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).replaceAccountIdentity(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Resource name unspecified or invalid")
  }

  @Test
  fun `replaceAccountIdentity throws UNAUTHENTICATED when credentials are missing`() {
    val request = replaceAccountIdentityRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.replaceAccountIdentity(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `replaceAccountIdentity throws UNAUTHENTICATED when credentials are invalid`() = runBlocking {
    whenever(internalAccountsMock.authenticateAccount(any()))
      .thenThrow(StatusRuntimeException(Status.UNAUTHENTICATED))

    val request = replaceAccountIdentityRequest {}

    val exception =
      assertFailsWith<StatusException> {
        client.withIdToken(ID_TOKEN).replaceAccountIdentity(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("Account credentials are invalid or missing")
  }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when new credentials are missing`() {
    val request = replaceAccountIdentityRequest { name = ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(ID_TOKEN).replaceAccountIdentity(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("New id token is missing")
  }

  @Test
  fun `authenticate returns uri when issuer is the self issued provider`() {
    val request = authenticateRequest { issuer = SELF_ISSUED_ISSUER }

    val result = runBlocking { client.authenticate(request) }

    val resultUri = URI.create(result.authenticationRequestUri)

    val queryParamMap = mutableMapOf<String, String>()
    for (queryParam in resultUri.query.split("&")) {
      val keyValue = queryParam.split("=")
      queryParamMap[keyValue[0]] = keyValue[1]
    }

    assertThat(resultUri.scheme).isEqualTo("openid")
    assertThat(queryParamMap["scope"]).isEqualTo("openid")
    assertThat(queryParamMap["response_type"]).isEqualTo("id_token")
    assertThat(queryParamMap["state"]).isEqualTo(externalIdToApiId(OPEN_ID_REQUEST_PARAMS.state))
    assertThat(queryParamMap["nonce"]).isEqualTo(externalIdToApiId(OPEN_ID_REQUEST_PARAMS.nonce))
    assertThat(queryParamMap["client_id"]).isEqualTo(REDIRECT_URI)
  }

  @Test
  fun `authenticate throws INVALID_ARGUMENT when issuer is missing`() {
    val request = authenticateRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.authenticate(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Issuer unspecified")
  }
}

private val UNACTIVATED_ACCOUNT: Account = account {
  name = ACCOUNT_NAME
  creator = CREATOR_ACCOUNT_NAME
  activationParams =
    AccountKt.activationParams {
      activationToken = externalIdToApiId(ACTIVATION_TOKEN)
      ownedMeasurementConsumer = MEASUREMENT_CONSUMER_NAME
    }
  activationState = Account.ActivationState.UNACTIVATED
  measurementConsumerCreationToken = externalIdToApiId(MEASUREMENT_CONSUMER_CREATION_TOKEN)
}

private val ACTIVATED_ACCOUNT: Account = account {
  name = ACCOUNT_NAME
  creator = CREATOR_ACCOUNT_NAME
  activationState = Account.ActivationState.ACTIVATED
  openId =
    openIdConnectIdentity {
      issuer = ISSUER
      subject = SUBJECT
    }
}

private val UNACTIVATED_INTERNAL_ACCOUNT: InternalAccount = internalAccount {
  externalAccountId = EXTERNAL_ACCOUNT_ID
  externalCreatorAccountId = EXTERNAL_CREATOR_ACCOUNT_ID
  activationState = InternalActivationState.UNACTIVATED
  activationToken = ACTIVATION_TOKEN
  externalOwnedMeasurementConsumerId =
    apiIdToExternalId(
      MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
    )
  measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
}

private val ACTIVATED_INTERNAL_ACCOUNT: InternalAccount = internalAccount {
  externalAccountId = EXTERNAL_ACCOUNT_ID
  externalCreatorAccountId = EXTERNAL_CREATOR_ACCOUNT_ID
  activationState = InternalActivationState.ACTIVATED
  activationToken = ACTIVATION_TOKEN
  externalOwnedMeasurementConsumerId =
    apiIdToExternalId(
      MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
    )
  measurementConsumerCreationToken = MEASUREMENT_CONSUMER_CREATION_TOKEN
  openIdIdentity =
    InternalAccountKt.openIdConnectIdentity {
      issuer = ISSUER
      subject = SUBJECT
    }
}

private val OPEN_ID_REQUEST_PARAMS = openIdRequestParams {
  state = 1234L
  nonce = 4321L
}
