/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamClientAccountsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.clientAccount
import org.wfanet.measurement.internal.kingdom.createClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getClientAccountRequest
import org.wfanet.measurement.internal.kingdom.streamClientAccountsRequest

private const val RANDOM_SEED = 1

@RunWith(JUnit4::class)
abstract class ClientAccountsServiceTest<T : ClientAccountsCoroutineImplBase> {

  protected data class Services<T>(
    val clientAccountsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val dataProvidersService: DataProvidersCoroutineImplBase,
    val accountsService: AccountsCoroutineImplBase,
  )

  private val clock: Clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var clientAccountsService: T
    private set

  protected lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected lateinit var accountsService: AccountsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    clientAccountsService = services.clientAccountsService
    measurementConsumersService = services.measurementConsumersService
    dataProvidersService = services.dataProvidersService
    accountsService = services.accountsService
  }

  @Test
  fun `createClientAccount returns ClientAccount`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount = clientAccount {
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      externalDataProviderId = dataProvider.externalDataProviderId
      clientAccountReferenceId = "test-reference-id"
    }

    val result =
      clientAccountsService.createClientAccount(
        createClientAccountRequest { this.clientAccount = clientAccount }
      )

    assertThat(result)
      .ignoringFields(ClientAccount.EXTERNAL_CLIENT_ACCOUNT_ID_FIELD_NUMBER)
      .isEqualTo(clientAccount)
    assertThat(result.externalClientAccountId).isGreaterThan(0L)
  }

  @Test
  fun `createClientAccount fails with invalid MeasurementConsumer ID`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount = clientAccount {
      externalMeasurementConsumerId = 404L
      externalDataProviderId = dataProvider.externalDataProviderId
      clientAccountReferenceId = "test-reference-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.createClientAccount(
          createClientAccountRequest { this.clientAccount = clientAccount }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `createClientAccount fails with invalid DataProvider ID`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val clientAccount = clientAccount {
      externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
      externalDataProviderId = 404L
      clientAccountReferenceId = "test-reference-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.createClientAccount(
          createClientAccountRequest { this.clientAccount = clientAccount }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("DataProvider not found")
  }

  @Test
  fun `getClientAccount returns created ClientAccount`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "test-reference-id"
          }
        }
      )

    val result =
      clientAccountsService.getClientAccount(
        getClientAccountRequest {
          externalMeasurementConsumerId = clientAccount.externalMeasurementConsumerId
          externalClientAccountId = clientAccount.externalClientAccountId
        }
      )

    assertThat(result).isEqualTo(clientAccount)
  }

  @Test
  fun `getClientAccount fails when not found`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.getClientAccount(
          getClientAccountRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalClientAccountId = 404L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ClientAccount not found")
  }

  @Test
  fun `deleteClientAccount removes ClientAccount`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "test-reference-id"
          }
        }
      )

    val deletedClientAccount =
      clientAccountsService.deleteClientAccount(
        deleteClientAccountRequest {
          externalMeasurementConsumerId = clientAccount.externalMeasurementConsumerId
          externalClientAccountId = clientAccount.externalClientAccountId
        }
      )

    assertThat(deletedClientAccount).isEqualTo(clientAccount)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.getClientAccount(
          getClientAccountRequest {
            externalMeasurementConsumerId = clientAccount.externalMeasurementConsumerId
            externalClientAccountId = clientAccount.externalClientAccountId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteClientAccount fails when not found`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.deleteClientAccount(
          deleteClientAccountRequest {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalClientAccountId = 404L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("ClientAccount not found")
  }

  @Test
  fun `deleteClientAccount fails with invalid MeasurementConsumer ID`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        clientAccountsService.deleteClientAccount(
          deleteClientAccountRequest {
            externalMeasurementConsumerId = 404L
            externalClientAccountId = 1L
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("MeasurementConsumer not found")
  }

  @Test
  fun `streamClientAccounts returns results`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount1 =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "test-reference-id-1"
          }
        }
      )

    val clientAccount2 =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "test-reference-id-2"
          }
        }
      )

    val result: List<ClientAccount> =
      clientAccountsService
        .streamClientAccounts(
          streamClientAccountsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
          }
        )
        .toList()

    assertThat(result).containsExactly(clientAccount2, clientAccount1).inOrder()
  }

  @Test
  fun `streamClientAccounts with filter succeeds`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider1: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProvider2: DataProvider = population.createDataProvider(dataProvidersService)

    val clientAccount1 =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider1.externalDataProviderId
            clientAccountReferenceId = "test-reference-id-1"
          }
        }
      )

    clientAccountsService.createClientAccount(
      createClientAccountRequest {
        this.clientAccount = clientAccount {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalDataProviderId = dataProvider2.externalDataProviderId
          clientAccountReferenceId = "test-reference-id-2"
        }
      }
    )

    val result: List<ClientAccount> =
      clientAccountsService
        .streamClientAccounts(
          streamClientAccountsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              externalDataProviderId = dataProvider1.externalDataProviderId
            }
          }
        )
        .toList()

    assertThat(result).containsExactly(clientAccount1)
  }

  @Test
  fun `streamClientAccounts with limit succeeds`(): Unit = runBlocking {
    val measurementConsumer: MeasurementConsumer =
      population.createMeasurementConsumer(measurementConsumersService, accountsService)
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    clientAccountsService.createClientAccount(
      createClientAccountRequest {
        this.clientAccount = clientAccount {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "test-reference-id-1"
        }
      }
    )

    val clientAccount2 =
      clientAccountsService.createClientAccount(
        createClientAccountRequest {
          this.clientAccount = clientAccount {
            externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "test-reference-id-2"
          }
        }
      )

    val result: List<ClientAccount> =
      clientAccountsService
        .streamClientAccounts(
          streamClientAccountsRequest {
            filter = filter {
              externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
            }
            limit = 1
          }
        )
        .toList()

    assertThat(result).containsExactly(clientAccount2)
  }
}
