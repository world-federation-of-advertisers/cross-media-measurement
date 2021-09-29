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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.StringGenerator
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.common.identity.testing.FixedStringGenerator
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.activationParams
import org.wfanet.measurement.internal.kingdom.AccountKt.usernameIdentity
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.activateAccountRequest
import org.wfanet.measurement.internal.kingdom.createAccountRequest
import org.wfanet.measurement.internal.kingdom.replaceAccountIdentityRequest
import org.wfanet.measurement.internal.kingdom.usernameCredentials

private const val FIXED_GENERATED_INTERNAL_ID_A = 1234L
private const val FIXED_GENERATED_EXTERNAL_ID_A = 5678L

private const val FIXED_GENERATED_INTERNAL_ID_B = 4321L
private const val FIXED_GENERATED_EXTERNAL_ID_B = 8765L

private const val FIXED_GENERATED_STRING_A = "text"
private const val FIXED_GENERATED_STRING_B = "text2"

@RunWith(JUnit4::class)
abstract class AccountsServiceTest<T : AccountsCoroutineImplBase> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
  )

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

  private val stringGeneratorA = FixedStringGenerator(FIXED_GENERATED_STRING_A)

  private val stringGeneratorB = FixedStringGenerator(FIXED_GENERATED_STRING_B)

  private lateinit var dataServices: TestDataServices

  /**
   * Different instances of the service under test with different fixed id and string generators.
   */
  private lateinit var service: T
  private lateinit var serviceWithSecondFixedGenerator: T

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(idGenerator: IdGenerator): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(idGenerator: IdGenerator, stringGenerator: StringGenerator): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(idGeneratorA)
  }

  @Before
  fun initService() {
    service = newService(idGeneratorA, stringGeneratorA)
    serviceWithSecondFixedGenerator = newService(idGeneratorB, stringGeneratorB)
  }

  @Test
  fun `createAccount returns account when no creator`() = runBlocking {
    val createAccountRequest = createAccountRequest {}

    val account = service.createAccount(createAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationParams = activationParams { activationToken = FIXED_GENERATED_STRING_A }
          activationState = Account.ActivationState.UNACTIVATED
          measurementConsumerCreationToken = FIXED_GENERATED_STRING_A
        }
      )
  }

  @Test
  fun `createAccount returns account when there is creator`() = runBlocking {
    service.createAccount(createAccountRequest {})

    val createAccountRequest = createAccountRequest {
      externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
    }
    val account = serviceWithSecondFixedGenerator.createAccount(createAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
          externalCreatorAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationParams = activationParams { activationToken = FIXED_GENERATED_STRING_B }
          activationState = Account.ActivationState.UNACTIVATED
          measurementConsumerCreationToken = FIXED_GENERATED_STRING_A
        }
      )
  }

  @Test
  fun `activateAccount throws NOT_FOUND when account not found`() = runBlocking {
    val activateAccountRequest = activateAccountRequest {
      externalAccountId = 1L
      usernameCredentials =
        usernameCredentials {
          username = "username"
          password = "password"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.status.description).isEqualTo("Account to activate has not been found")
  }

  @Test
  fun `activateAccount throws NOT_FOUND when identity not found in request`() = runBlocking {
    service.createAccount(createAccountRequest {})

    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      activationToken = FIXED_GENERATED_STRING_A
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.status.description).isEqualTo("Account identity not found in request")
  }

  @Test
  fun `activateAccount throws PERMISSION_DENIED when token doesn't match token in database`() =
      runBlocking {
    service.createAccount(createAccountRequest {})

    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Activation token is not valid for this account")
  }

  @Test
  fun `activateAccount throws PERMISSION_DENIED when account has already been activated`() =
      runBlocking {
    service.createAccount(createAccountRequest {})
    val username = "username"
    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      activationToken = FIXED_GENERATED_STRING_A
      usernameCredentials =
        usernameCredentials {
          this.username = username
          password = "password"
        }
    }
    service.activateAccount(activateAccountRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Activation token is not valid for this account")
  }

  @Test
  fun `activateAccount throws INVALID_ARGUMENT when passed-in username already exists`() =
      runBlocking {
    service.createAccount(createAccountRequest {})

    val username = "username"
    service.activateAccount(
      activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_STRING_A
        usernameCredentials =
          usernameCredentials {
            this.username = username
            password = "password"
          }
      }
    )

    serviceWithSecondFixedGenerator.createAccount(createAccountRequest {})
    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
      activationToken = FIXED_GENERATED_STRING_B
      usernameCredentials =
        usernameCredentials {
          this.username = username
          password = "password"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.activateAccount(activateAccountRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Username already exists")
  }

  @Test
  fun `activateAccount returns account when account is activated with username identity`() =
      runBlocking {
    service.createAccount(createAccountRequest {})

    val username = "username"
    val activateAccountRequest = activateAccountRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      activationToken = FIXED_GENERATED_STRING_A
      usernameCredentials =
        usernameCredentials {
          this.username = username
          password = "password"
        }
    }

    val account = service.activateAccount(activateAccountRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationState = Account.ActivationState.ACTIVATED
          measurementConsumerCreationToken = FIXED_GENERATED_STRING_A
          usernameIdentity = usernameIdentity { this.username = username }
        }
      )
  }

  @Test
  fun `replaceAccountIdentity throws NOT_FOUND when account not found`() = runBlocking {
    val replaceAccountIdentity = replaceAccountIdentityRequest {
      externalAccountId = 1L
      usernameCredentials =
        usernameCredentials {
          username = "username"
          password = "password"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.replaceAccountIdentity(replaceAccountIdentity)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.status.description).isEqualTo("Account was not found")
  }

  @Test
  fun `replaceAccountIdentity throws PERMISSION_DENIED when account is unactivated`() =
      runBlocking {
    service.createAccount(createAccountRequest {})

    val replaceAccountIdentity = replaceAccountIdentityRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      usernameCredentials =
        usernameCredentials {
          username = "username"
          password = "password"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.replaceAccountIdentity(replaceAccountIdentity)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).isEqualTo("Account has not been activated yet")
  }

  @Test
  fun `replaceAccountIdentity throws NOT_FOUND when identity not found in request`() = runBlocking {
    val replaceAccountIdentity = replaceAccountIdentityRequest { externalAccountId = 1L }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.replaceAccountIdentity(replaceAccountIdentity)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.status.description).isEqualTo("Account identity not found in request")
  }

  @Test
  fun `replaceAccountIdentity throws INVALID_ARGUMENT when passed-in username already exists`() =
      runBlocking {
    service.createAccount(createAccountRequest {})
    val username = "username"
    service.activateAccount(
      activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_STRING_A
        usernameCredentials =
          usernameCredentials {
            this.username = username
            password = "password"
          }
      }
    )

    serviceWithSecondFixedGenerator.createAccount(createAccountRequest {})
    serviceWithSecondFixedGenerator.activateAccount(
      activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
        activationToken = FIXED_GENERATED_STRING_B
        usernameCredentials =
          usernameCredentials {
            this.username = "username2"
            password = "password"
          }
      }
    )

    val replaceAccountIdentityRequest = replaceAccountIdentityRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_B
      usernameCredentials =
        usernameCredentials {
          this.username = username
          password = "password"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.replaceAccountIdentity(replaceAccountIdentityRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Username already exists")
  }

  @Test
  fun `replaceAccountIdentity returns account with new username identity`() = runBlocking {
    service.createAccount(createAccountRequest {})

    val username = "username"
    service.activateAccount(
      activateAccountRequest {
        externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
        activationToken = FIXED_GENERATED_STRING_A
        usernameCredentials =
          usernameCredentials {
            this.username = username
            password = "password"
          }
      }
    )

    val replaceAccountIdentityRequest = replaceAccountIdentityRequest {
      externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
      usernameCredentials =
        usernameCredentials {
          this.username = "username2"
          password = "password"
        }
    }

    val account = service.replaceAccountIdentity(replaceAccountIdentityRequest)

    assertThat(account)
      .isEqualTo(
        account {
          externalAccountId = FIXED_GENERATED_EXTERNAL_ID_A
          activationState = Account.ActivationState.ACTIVATED
          usernameIdentity = usernameIdentity { this.username = "username2" }
        }
      )
  }
}
