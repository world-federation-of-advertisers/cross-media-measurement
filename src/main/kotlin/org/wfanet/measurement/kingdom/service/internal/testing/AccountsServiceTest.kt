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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.gson.JsonParser
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.common.openid.createRequestUri
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.activateAccountRequest
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.generateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.replaceAccountIdentityRequest

private const val FIXED_GENERATED_INTERNAL_ID_A = 1234L
private const val FIXED_GENERATED_EXTERNAL_ID_A = 5678L

private const val FIXED_GENERATED_INTERNAL_ID_B = 4321L
private const val FIXED_GENERATED_EXTERNAL_ID_B = 8765L

@RunWith(JUnit4::class)
abstract class AccountsServiceTest<T : AccountsCoroutineImplBase> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService
  )

  private val clock: Clock = Clock.systemUTC()

  private val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID_A),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID_A),
    )

  private val population = Population(clock, idGenerator)

  private lateinit var dataServices: TestDataServices

  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(idGenerator: IdGenerator): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(idGenerator)
  }

  @Before
  fun initService() {
    service = newService(idGenerator)
  }

  @Test
  fun `createAccount throws NOT_FOUND when creator account not found`() = runBlocking {
    val createAccountRequest = account { externalCreatorAccountId = 1L }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createAccount(createAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createAccount throws PERMISSION_DENIED when owned measurement consumer not found`() =
    runBlocking {
      service.createAccount(account {})

      val createAccountRequest = account {
        externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        externalOwnedMeasurementConsumerId = 1L
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createAccount(createAccountRequest) }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `createAccount throws PERMISSION_DENIED when caller doesn't own measurement consumer`() =
    runBlocking {
      val measurementConsumer =
        population.createMeasurementConsumer(dataServices.measurementConsumersService, service)

      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
      service.createAccount(account {})

      val createAccountRequest = account {
        externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_B
        externalOwnedMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }

      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B + 1L)
      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B + 1L)
      val exception =
        assertFailsWith<StatusRuntimeException> { service.createAccount(createAccountRequest) }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `createAccount returns account when there is no creator`() {
    val account = runBlocking { service.createAccount(account {}) }

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_A
          activationState = Account.ActivationState.UNACTIVATED
        }
      )
  }

  @Test
  fun `createAccount returns account when there is creator`() = runBlocking {
    service.createAccount(account {})

    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
    val createAccountRequest = account { externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A }
    val account = service.createAccount(createAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_B
          activationState = Account.ActivationState.UNACTIVATED
        }
      )
  }

  @Test
  fun `createAccount returns account with owned MC when creator owns MC`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService, service)

    val createAccountRequest = account {
      externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      externalOwnedMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    }

    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
    val account = service.createAccount(createAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_B
          activationState = Account.ActivationState.UNACTIVATED
          externalOwnedMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        }
      )
  }

  @Test
  fun `activateAccount throws NOT_FOUND when account not found`() = runBlocking {
    val idToken = generateIdToken(service)
    val openIdConnectIdentity = population.parseIdToken(idToken)

    val activateAccountRequest = activateAccountRequest {
      externalAccountId = 1L
      activationToken = 1L
      identity = openIdConnectIdentity
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `activateAccount throws PERMISSION_DENIED when activation token doesn't match database`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)

      service.createAccount(account {})

      val activateAccountRequest = activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = 1L
        identity = openIdConnectIdentity
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `activateAccount throws PERMISSION_DENIED when account has already been activated`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)
      service.createAccount(account {})
      val activateAccountRequest = activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_EXTERNAL_ID_A
        identity = openIdConnectIdentity
      }

      service.activateAccount(activateAccountRequest)

      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
      val idToken2 = generateIdToken(service)
      val openIdConnectIdentity2 = population.parseIdToken(idToken2)
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.activateAccount(activateAccountRequest.copy { identity = openIdConnectIdentity2 })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when issuer and subject pair already exists`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)

      service.createAccount(account {})

      service.activateAccount(
        activateAccountRequest {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_A
          identity = openIdConnectIdentity
        }
      )

      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
      service.createAccount(account {})
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.activateAccount(
            activateAccountRequest {
              externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
              activationToken = FIXED_GENERATED_EXTERNAL_ID_B
              identity = openIdConnectIdentity
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `activateAccount returns account when account is activated with open id connect identity`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)

      service.createAccount(account {})

      val activateAccountRequest = activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_EXTERNAL_ID_A
        identity = openIdConnectIdentity
      }

      val account = service.activateAccount(activateAccountRequest)

      assertThat(account)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          account {
            externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
            activationState = Account.ActivationState.ACTIVATED
          }
        )
    }

  @Test
  fun `activateAccount returns account when account is activated with owned MC`() = runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService, service)

    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
    service.createAccount(
      account {
        externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        externalOwnedMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      }
    )

    val idToken = generateIdToken(service)
    val openIdConnectIdentity = population.parseIdToken(idToken)
    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
      activationToken = FIXED_GENERATED_EXTERNAL_ID_B
      identity = openIdConnectIdentity
    }

    val account = service.activateAccount(activateAccountRequest)

    assertThat(account)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        account {
          externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          activationState = Account.ActivationState.ACTIVATED
          externalOwnedMeasurementConsumerIds += measurementConsumer.externalMeasurementConsumerId
        }
      )
  }

  @Test
  fun `replaceAccountIdentity throws NOT_FOUND when account not found`() = runBlocking {
    val idToken = generateIdToken(service)
    val openIdConnectIdentity = population.parseIdToken(idToken)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.replaceAccountIdentity(
          replaceAccountIdentityRequest {
            externalAccountId = 1L
            identity = openIdConnectIdentity
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `replaceAccountIdentity throws FAILED_PRECONDITION when account is unactivated`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)

      service.createAccount(account {})

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceAccountIdentity(
            replaceAccountIdentityRequest {
              externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
              identity = openIdConnectIdentity
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when passed-in id token already exists`() =
    runBlocking {
      val idToken = generateIdToken(service)
      val openIdConnectIdentity = population.parseIdToken(idToken)

      service.createAccount(account {})
      service.activateAccount(
        activateAccountRequest {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_A
          identity = openIdConnectIdentity
        }
      )

      idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
      idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
      val idToken2 = generateIdToken(service)
      val openIdConnectIdentity2 = population.parseIdToken(idToken2)
      service.createAccount(account {})
      service.activateAccount(
        activateAccountRequest {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          activationToken = FIXED_GENERATED_EXTERNAL_ID_B
          identity = openIdConnectIdentity2
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceAccountIdentity(
            replaceAccountIdentityRequest {
              externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
              identity = openIdConnectIdentity
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `replaceAccountIdentity returns account with new open id connect identity`() = runBlocking {
    val idToken = generateIdToken(service)
    val openIdConnectIdentity = population.parseIdToken(idToken)

    service.createAccount(account {})
    service.activateAccount(
      activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_EXTERNAL_ID_A
        identity = openIdConnectIdentity
      }
    )

    idGenerator.externalId = ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
    idGenerator.internalId = InternalId(FIXED_GENERATED_INTERNAL_ID_B)
    val idToken2 = generateIdToken(service)
    val openIdConnectIdentity2 = population.parseIdToken(idToken2)

    val account =
      service.replaceAccountIdentity(
        replaceAccountIdentityRequest {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          identity = openIdConnectIdentity2
        }
      )

    val tokenParts = idToken2.split(".")
    val claims =
      JsonParser.parseString(tokenParts[1].base64UrlDecode().toString(Charsets.UTF_8)).asJsonObject
    val iss = claims.get("iss").asString
    val sub = claims.get("sub").asString

    assertThat(account)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationState = Account.ActivationState.ACTIVATED
          openIdIdentity = openIdConnectIdentity {
            this.issuer = iss
            this.subject = sub
          }
        }
      )
  }

  @Test
  fun `authenticateAccount throws NOT_FOUND when identity doesn't exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.authenticateAccount(authenticateAccountRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `authenticateAccount returns the account when the account has been found`() = runBlocking {
    val idToken = generateIdToken(service)
    val openIdConnectIdentity = population.parseIdToken(idToken)
    val createdAccount = service.createAccount(account {})

    val activatedAccount =
      service.activateAccount(
        activateAccountRequest {
          externalAccountId = createdAccount.externalAccountId
          activationToken = createdAccount.activationToken
          identity = openIdConnectIdentity
        }
      )

    val authenticatedAccount =
      service.authenticateAccount(authenticateAccountRequest { identity = openIdConnectIdentity })

    assertThat(authenticatedAccount.openIdIdentity).isEqualTo(activatedAccount.openIdIdentity)
  }

  @Test
  fun `generateOpenIdRequestParams returns state and nonce`() {
    val params = runBlocking {
      service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    }

    assertThat(params.nonce != 0L)
    assertThat(params.state != 0L)
  }

  @Test
  fun `createMeasurementConsumerCreationToken returns token`() = runBlocking {
    val createTokenResponse =
      service.createMeasurementConsumerCreationToken(
        createMeasurementConsumerCreationTokenRequest {}
      )

    assertThat(createTokenResponse.measurementConsumerCreationToken).isNotEqualTo(0L)
  }

  private suspend fun generateIdToken(service: T): String {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    return SelfIssuedIdTokens.generateIdToken(
      createRequestUri(
        state = params.state,
        nonce = params.nonce,
        redirectUri = "",
        isSelfIssued = true,
      ),
      clock,
    )
  }
}
