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
import java.security.GeneralSecurityException
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
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
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.Account as InternalAccount
import org.wfanet.measurement.internal.kingdom.Account.ActivationState as InternalActivationState
import org.wfanet.measurement.internal.kingdom.AccountKt as InternalAccountKt
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.account as internalAccount
import org.wfanet.measurement.internal.kingdom.activateAccountRequest as internalActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.openIdRequestParams
import org.wfanet.measurement.internal.kingdom.replaceAccountIdentityRequest as internalReplaceAccountIdentityRequest

private const val ACTIVATION_TOKEN = 12345672L

private const val EXTERNAL_ACCOUNT_ID = 12345678L
private const val EXTERNAL_CREATOR_ACCOUNT_ID = 56781234L

private const val ACCOUNT_NAME = "accounts/AAAAAAC8YU4"
private const val CREATOR_ACCOUNT_NAME = "accounts/AAAAAANiabI"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"

private const val ISSUER = "issuer"
private const val SUBJECT = "subject"

private const val REDIRECT_URI = "https://localhost:2048"
private const val SELF_ISSUED_ISSUER = "https://self-issued.me"

@RunWith(JUnit4::class)
class AccountsServiceTest {
  private val internalAccountsMock: AccountsCoroutineImplBase =
    mockService() {
      onBlocking { createAccount(any()) }.thenReturn(UNACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { activateAccount(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { replaceAccountIdentity(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { authenticateAccount(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { generateOpenIdRequestParams(any()) }.thenReturn(OPEN_ID_REQUEST_PARAMS)
      onBlocking { getOpenIdRequestParams(any()) }.thenReturn(OPEN_ID_REQUEST_PARAMS)
    }

  @get:Rule val internalGrpcTestServerRule = GrpcTestServerRule { addService(internalAccountsMock) }

  private lateinit var internalClient: InternalAccountsCoroutineStub

  @get:Rule
  var publicGrpcTestServerRule = GrpcTestServerRule {
    internalClient = InternalAccountsCoroutineStub(internalGrpcTestServerRule.channel)
    val service = AccountsService(internalClient, REDIRECT_URI)
    addService(service.withAccountAuthenticationServerInterceptor(internalClient, REDIRECT_URI))
  }

  private lateinit var client: AccountsCoroutineStub

  private val clock: Clock = Clock.systemUTC()

  @Before
  fun initClient() {
    client = AccountsCoroutineStub(publicGrpcTestServerRule.channel)
  }

  @Test
  fun `createAccount returns unactivated account`() {
    val request = createAccountRequest {
      account = account {
        activationParams =
          AccountKt.activationParams { ownedMeasurementConsumer = MEASUREMENT_CONSUMER_NAME }
      }
    }

    val result = runBlocking { client.withIdToken(generateIdToken()).createAccount(request) }

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
      account = account {
        activationParams = AccountKt.activationParams { ownedMeasurementConsumer = "43254" }
      }
    }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(generateIdToken()).createAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createAccount throws UNAUTHENTICATED when credentials are missing`() {
    val request = createAccountRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.createAccount(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createAccount throws UNAUTHENTICATED when credentials are invalid`() = runBlocking {
    whenever(internalAccountsMock.authenticateAccount(any()))
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = createAccountRequest {}

    val exception =
      assertFailsWith<StatusException> {
        client.withIdToken(generateIdToken()).createAccount(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `activateAccount returns activated account`() = runBlocking {
    val request = activateAccountRequest {
      name = ACCOUNT_NAME
      activationToken = externalIdToApiId(ACTIVATION_TOKEN)
    }

    val idToken = generateIdToken()
    val result = client.withIdToken(idToken).activateAccount(request)

    assertThat(result).isEqualTo(ACTIVATED_ACCOUNT)

    val openIdConnectIdentity =
      AccountsService.validateIdToken(
        idToken = idToken,
        redirectUri = REDIRECT_URI,
        internalAccountsStub = internalClient
      )
    verifyProtoArgument(internalAccountsMock, AccountsCoroutineImplBase::activateAccount)
      .isEqualTo(
        internalActivateAccountRequest {
          externalAccountId = EXTERNAL_ACCOUNT_ID
          activationToken = ACTIVATION_TOKEN
          identity = openIdConnectIdentity
        }
      )
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when resource name is missing`() {
    val request = activateAccountRequest { activationToken = externalIdToApiId(ACTIVATION_TOKEN) }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(generateIdToken()).activateAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when activation token is missing`() {
    val request = activateAccountRequest { name = ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(generateIdToken()).activateAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
  }

  @Test
  fun `replaceAccountIdentity with openIdConnectidentity type returns account with same type`() =
    runBlocking {
      val newIdToken = generateIdToken()
      val request = replaceAccountIdentityRequest {
        name = ACCOUNT_NAME
        openId =
          ReplaceAccountIdentityRequestKt.openIdConnectCredentials {
            identityBearerToken = newIdToken
          }
      }

      val result = client.withIdToken(generateIdToken()).replaceAccountIdentity(request)

      assertThat(result).isEqualTo(ACTIVATED_ACCOUNT)

      val openIdConnectIdentity =
        AccountsService.validateIdToken(
          idToken = newIdToken,
          redirectUri = REDIRECT_URI,
          internalAccountsStub = internalClient
        )
      verifyProtoArgument(internalAccountsMock, AccountsCoroutineImplBase::replaceAccountIdentity)
        .isEqualTo(
          internalReplaceAccountIdentityRequest {
            externalAccountId = EXTERNAL_ACCOUNT_ID
            identity = openIdConnectIdentity
          }
        )
    }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when resource name is missing`() {
    val request = replaceAccountIdentityRequest {
      openId =
        ReplaceAccountIdentityRequestKt.openIdConnectCredentials {
          identityBearerToken = runBlocking { generateIdToken() }
        }
    }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(generateIdToken()).replaceAccountIdentity(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceAccountIdentity throws UNAUTHENTICATED when credentials are missing`() {
    val request = replaceAccountIdentityRequest {}

    val exception =
      assertFailsWith<StatusException> { runBlocking { client.replaceAccountIdentity(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `replaceAccountIdentity throws UNAUTHENTICATED when credentials are invalid`() = runBlocking {
    whenever(internalAccountsMock.authenticateAccount(any()))
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = replaceAccountIdentityRequest {}

    val exception =
      assertFailsWith<StatusException> {
        client.withIdToken(generateIdToken()).replaceAccountIdentity(request)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when new credentials are missing`() {
    val request = replaceAccountIdentityRequest { name = ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { client.withIdToken(generateIdToken()).replaceAccountIdentity(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
  }

  @Test
  fun `validateIdToken throws GeneralSecurityException when nonce doesn't match`() {
    runBlocking {
      val idToken = generateIdToken()

      whenever(internalAccountsMock.getOpenIdRequestParams(any()))
        .thenReturn(OPEN_ID_REQUEST_PARAMS.copy { nonce += 5L })

      assertFailsWith<GeneralSecurityException> {
        AccountsService.validateIdToken(
          idToken = idToken,
          internalAccountsStub = internalClient,
          redirectUri = REDIRECT_URI
        )
      }
    }
  }

  @Test
  fun `validateIdToken throws GeneralSecurityException when format is incorrect`() {
    runBlocking {
      val idToken = generateIdToken() + ".152345"

      assertFailsWith<GeneralSecurityException> {
        AccountsService.validateIdToken(
          idToken = idToken,
          internalAccountsStub = internalClient,
          redirectUri = REDIRECT_URI
        )
      }
    }
  }

  @Test
  fun `validateIdToken throws GeneralSecurityException when signature doesn't match`() {
    runBlocking {
      val idToken = generateIdToken() + "5"

      assertFailsWith<GeneralSecurityException> {
        AccountsService.validateIdToken(
          idToken = idToken,
          internalAccountsStub = internalClient,
          redirectUri = REDIRECT_URI
        )
      }
    }
  }

  @Test
  fun `validateIdToken throws GeneralSecurityException when redirect uri doesn't match`() {
    runBlocking {
      val idToken = generateIdToken()

      assertFailsWith<GeneralSecurityException> {
        AccountsService.validateIdToken(
          idToken = idToken,
          internalAccountsStub = internalClient,
          redirectUri = REDIRECT_URI + "5"
        )
      }
    }
  }

  @Test
  fun `validateIdToken throws GeneralSecurityException when redirect uri is unexpected`() {
    runBlocking {
      val idToken = generateIdToken()

      assertFailsWith<GeneralSecurityException> {
        AccountsService.validateIdToken(
          idToken = idToken,
          internalAccountsStub = internalClient,
          redirectUri = ""
        )
      }
    }
  }

  private suspend fun generateIdToken(): String {
    val uriString =
      client
        .authenticate(authenticateRequest { issuer = SELF_ISSUED_ISSUER })
        .authenticationRequestUri
    return SelfIssuedIdTokens.generateIdToken(uriString, clock)
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
}

private val ACTIVATED_ACCOUNT: Account = account {
  name = ACCOUNT_NAME
  creator = CREATOR_ACCOUNT_NAME
  activationState = Account.ActivationState.ACTIVATED
  openId = openIdConnectIdentity {
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
  openIdIdentity =
    InternalAccountKt.openIdConnectIdentity {
      issuer = ISSUER
      subject = SUBJECT
    }
}

private val OPEN_ID_REQUEST_PARAMS = openIdRequestParams {
  state = 1234L
  nonce = 4321L
  isExpired = false
}
