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
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountKt
import org.wfanet.measurement.api.v2alpha.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.account
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
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

private const val ACTIVATION_TOKEN = 12345672L
private const val MEASUREMENT_CONSUMER_CREATION_TOKEN = 12345673L

private const val EXTERNAL_ACCOUNT_ID = 12345678L
private const val EXTERNAL_CREATOR_ACCOUNT_ID = 56781234L

private const val ACCOUNT_NAME = "accounts/AAAAAAC8YU4"
private const val CREATOR_ACCOUNT_NAME = "accounts/AAAAAANiabI"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"

private const val ID_TOKEN = "id_token"
private const val ISSUER = "issuer"
private const val SUBJECT = "subject"

@RunWith(JUnit4::class)
class AccountsServiceTest {
  private val internalAccountsMock: AccountsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { createAccount(any()) }.thenReturn(UNACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { activateAccount(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
      onBlocking { replaceAccountIdentity(any()) }.thenReturn(ACTIVATED_INTERNAL_ACCOUNT)
    }

  @get:Rule val internalGrpcTestServerRule = GrpcTestServerRule { addService(internalAccountsMock) }

  @get:Rule
  var publicGrpcTestServerRule = GrpcTestServerRule {
    val service =
      AccountsService(AccountsGrpcKt.AccountsCoroutineStub(internalGrpcTestServerRule.channel))
    addService(service.withAccountServerInterceptor())
  }

  private lateinit var client: AccountsCoroutineStub

  @Before
  fun initClient() {
    client = AccountsCoroutineStub(publicGrpcTestServerRule.channel)
  }

  // TODO("Not yet implemented")
  @Test fun `createAccount returns unactivated account`(): Unit = runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `createAccount throws INVALID_ARGUMENT when owned measurement consumer name is invalid`():
    Unit = runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `createAccount throws PERMISSION_DENIED when authentication credentials are invalid`(): Unit =
      runBlocking {}

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

  // TODO("Not yet implemented")
  @Test
  fun `replaceAccountIdentity with openIdConnectidentity type returns account with same type`():
    Unit = runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when resource name is missing`(): Unit =
      runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `replaceAccountIdentity throws PERMISSION_DENIED when credentials are invalid`(): Unit =
      runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when new credentials are missing`(): Unit =
      runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `authenticate returns auth request uri when issuer is the self-issued OpenId provider`():
    Unit = runBlocking {}

  // TODO("Not yet implemented")
  @Test
  fun `authenticate returns auth request uri when issuer is a third party OpenId provider`(): Unit =
      runBlocking {}
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
