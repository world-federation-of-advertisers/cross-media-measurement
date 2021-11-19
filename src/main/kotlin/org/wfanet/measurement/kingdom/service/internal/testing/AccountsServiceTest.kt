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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.net.URLEncoder
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.activateAccountRequest
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.generateOpenIdRequestParamsRequest
import org.wfanet.measurement.kingdom.deploy.common.service.withIdToken
import org.wfanet.measurement.tools.generateIdToken

private const val FIXED_GENERATED_INTERNAL_ID_A = 1234L
private const val FIXED_GENERATED_EXTERNAL_ID_A = 5678L

private const val FIXED_GENERATED_INTERNAL_ID_B = 4321L
private const val FIXED_GENERATED_EXTERNAL_ID_B = 8765L

private const val REDIRECT_URI = "https://localhost:2048"

@RunWith(JUnit4::class)
abstract class AccountsServiceTest<T : AccountsCoroutineImplBase> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
  )

  private val clock: Clock = Clock.systemUTC()

  private val idGeneratorA =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID_A),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID_A)
    )

  private val idGeneratorB =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID_B),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID_B)
    )

  private val population = Population(clock, idGeneratorA)

  private lateinit var dataServices: TestDataServices

  /**
   * Different instances of the service under test with different fixed id and string generators.
   */
  private lateinit var service: T
  private lateinit var serviceWithSecondFixedGenerator: T

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(idGenerator: IdGenerator): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(idGeneratorA)
  }

  @Before
  fun initService() {
    service = newService(idGeneratorA)
    serviceWithSecondFixedGenerator = newService(idGeneratorB)
  }

  @Test
  fun `createAccount throws NOT_FOUND when creator account not found`() = runBlocking {
    val createAccountRequest = account { externalCreatorAccountId = 1L }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createAccount(createAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.status.description).isEqualTo("Creator account not found")
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
    assertThat(exception.status.description)
      .isEqualTo("Caller does not own the owned measurement consumer")
  }

  @Test
  fun `createAccount throws PERMISSION_DENIED when caller doesn't own measurement consumer`() =
      runBlocking {
    val measurementConsumer =
      population.createMeasurementConsumer(dataServices.measurementConsumersService)

    service.createAccount(account {})

    val createAccountRequest = account {
      externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      externalOwnedMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createAccount(createAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Caller does not own the owned measurement consumer")
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
          measurementConsumerCreationToken = FIXED_GENERATED_EXTERNAL_ID_A
        }
      )
  }

  @Test
  fun `createAccount returns account when there is creator`() = runBlocking {
    service.createAccount(account {})

    val createAccountRequest = account { externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A }
    val account = serviceWithSecondFixedGenerator.createAccount(createAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationToken = FIXED_GENERATED_EXTERNAL_ID_B
          activationState = Account.ActivationState.UNACTIVATED
          measurementConsumerCreationToken = FIXED_GENERATED_EXTERNAL_ID_A
        }
      )
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when id token is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.authenticateAccount(authenticateAccountRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Id token is missing")
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when id token is in the wrong format`() {
    val idToken = ""

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withIdToken(idToken) {
          runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Id token is invalid")
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when state doesn't match`() = runBlocking {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(generateRequestUri(state = params.state + 5L, nonce = params.nonce), clock)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withIdToken(idToken) {
          runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Id token is invalid")
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when nonce doesn't match`() = runBlocking {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(generateRequestUri(state = params.state, nonce = params.nonce + 5L), clock)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withIdToken(idToken) {
          runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Id token is invalid")
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when signature is unverified`() = runBlocking {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(generateRequestUri(state = params.state, nonce = params.nonce + 5L), clock) +
        "5"

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withIdToken(idToken) {
          runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Id token is invalid")
  }

  @Test
  fun `authenticateAccount throws PERMISSION_DENIED when identity doesn't exist`() = runBlocking {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(generateRequestUri(state = params.state, nonce = params.nonce), clock)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withIdToken(idToken) {
          runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Account not found")
  }

  @Ignore
  @Test
  fun `authenticateAccount returns the account when the account has been found`() = runBlocking {
    val params = service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    val idToken =
      generateIdToken(generateRequestUri(state = params.state, nonce = params.nonce), clock)

    val createdAccount = service.createAccount(account {})

    withIdToken(idToken) {
      runBlocking {
        service.activateAccount(
          activateAccountRequest { activationToken = createdAccount.activationToken }
        )
      }
    }

    val authenticatedAccount =
      withIdToken(idToken) {
        runBlocking { service.authenticateAccount(authenticateAccountRequest {}) }
      }

    assertThat(authenticatedAccount).isEqualTo(createdAccount)
  }

  @Test
  fun `generateOpenIdRequestParams returns state, nonce, and max age`() {
    val params = runBlocking {
      service.generateOpenIdRequestParams(generateOpenIdRequestParamsRequest {})
    }

    assertThat(params.nonce != 0L)
    assertThat(params.state != 0L)
  }
}

private fun generateRequestUri(
  state: Long,
  nonce: Long,
): String {
  val uriParts = mutableListOf<String>()
  uriParts.add("openid://?response_type=id_token")
  uriParts.add("scope=openid")
  uriParts.add("state=" + externalIdToApiId(state))
  uriParts.add("nonce=" + externalIdToApiId(nonce))
  val redirectUri = URLEncoder.encode(REDIRECT_URI, "UTF-8")
  uriParts.add("client_id=$redirectUri")

  return uriParts.joinToString("&")
}
