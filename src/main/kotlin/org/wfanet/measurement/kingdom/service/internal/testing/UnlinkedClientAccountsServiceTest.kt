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
import com.google.protobuf.struct
import com.google.protobuf.value
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroupKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.batchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.batchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.createUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException

@RunWith(JUnit4::class)
abstract class UnlinkedClientAccountsServiceTest<T : UnlinkedClientAccountsCoroutineImplBase> {
  @get:Rule val duchyIdSetter = DuchyIdSetter(Population.DUCHIES)

  protected data class Services<T>(
    val unlinkedClientAccountsService: T,
    val dataProvidersService: DataProvidersCoroutineImplBase,
  )

  private val clock: Clock = Clock.systemUTC()
  private val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  private val population = Population(clock, idGenerator)

  protected lateinit var unlinkedClientAccountsService: T
    private set

  protected lateinit var dataProvidersService: DataProvidersCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initServices() {
    val services = newServices(idGenerator)
    unlinkedClientAccountsService = services.unlinkedClientAccountsService
    dataProvidersService = services.dataProvidersService
  }

  @Test
  fun `createUnlinkedClientAccount returns UnlinkedClientAccount`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val account = unlinkedClientAccount {
      externalDataProviderId = dataProvider.externalDataProviderId
      clientAccountReferenceId = "ref-1"
      entityMetadata = ENTITY_METADATA
      eventGroupReferenceId = "eg-1"
    }

    val response =
      unlinkedClientAccountsService.createUnlinkedClientAccount(
        createUnlinkedClientAccountRequest { unlinkedClientAccount = account }
      )

    assertThat(response)
      .ignoringFields(UnlinkedClientAccount.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(account)
    assertThat(response.hasCreateTime()).isTrue()

    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listed.unlinkedClientAccountsList.single()).isEqualTo(response)
  }

  @Test
  fun `createUnlinkedClientAccount round-trips entity_key`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val response =
      unlinkedClientAccountsService.createUnlinkedClientAccount(
        createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-1"
            entityKey =
              EventGroupKt.entityKey {
                entityType = "advertiser"
                entityId = "acct-123"
              }
          }
        }
      )

    assertThat(response.hasEntityKey()).isTrue()
    assertThat(response.entityKey)
      .isEqualTo(
        EventGroupKt.entityKey {
          entityType = "advertiser"
          entityId = "acct-123"
        }
      )
    assertThat(response.hasEventGroupReferenceId()).isFalse()

    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listed.unlinkedClientAccountsList.single()).isEqualTo(response)
  }

  @Test
  fun `createUnlinkedClientAccount round-trips an unset observed_event_group`(): Unit =
    runBlocking {
      val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

      val response =
        unlinkedClientAccountsService.createUnlinkedClientAccount(
          createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "ref-1"
            }
          }
        )

      assertThat(response.observedEventGroupCase)
        .isEqualTo(UnlinkedClientAccount.ObservedEventGroupCase.OBSERVEDEVENTGROUP_NOT_SET)
      assertThat(response.hasEntityMetadata()).isFalse()

      val listed =
        unlinkedClientAccountsService.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
      assertThat(listed.unlinkedClientAccountsList.single()).isEqualTo(response)
    }

  @Test
  fun `createUnlinkedClientAccount fails when already exists`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    unlinkedClientAccountsService.createUnlinkedClientAccount(
      createUnlinkedClientAccountRequest {
        unlinkedClientAccount = unlinkedClientAccount {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-1"
        }
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.createUnlinkedClientAccount(
          createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "ref-1"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.UNLINKED_CLIENT_ACCOUNT_ALREADY_EXISTS.name
          metadata["external_data_provider_id"] = dataProvider.externalDataProviderId.toString()
          metadata["client_account_reference_id"] = "ref-1"
        }
      )
  }

  @Test
  fun `createUnlinkedClientAccount fails when DataProvider not found`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.createUnlinkedClientAccount(
          createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = 404L
              clientAccountReferenceId = "ref-1"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.DATA_PROVIDER_NOT_FOUND.name
          metadata["external_data_provider_id"] = "404"
        }
      )
  }

  @Test
  fun `createUnlinkedClientAccount fails when external DataProvider ID is unset`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          unlinkedClientAccountsService.createUnlinkedClientAccount(
            createUnlinkedClientAccountRequest {
              unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createUnlinkedClientAccount fails with empty reference ID`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.createUnlinkedClientAccount(
          createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createUnlinkedClientAccount fails when reference ID too long`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.createUnlinkedClientAccount(
          createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "a".repeat(37)
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts returns accounts`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val response =
      unlinkedClientAccountsService.batchCreateUnlinkedClientAccounts(
        batchCreateUnlinkedClientAccountsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          requests += createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "batch-ref-1"
            }
          }
          requests += createUnlinkedClientAccountRequest {
            unlinkedClientAccount = unlinkedClientAccount {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "batch-ref-2"
            }
          }
        }
      )

    assertThat(response.unlinkedClientAccountsList).hasSize(2)
    for (created in response.unlinkedClientAccountsList) {
      assertThat(created.externalDataProviderId).isEqualTo(dataProvider.externalDataProviderId)
      assertThat(created.hasCreateTime()).isTrue()
    }
    assertThat(response.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("batch-ref-1", "batch-ref-2")
      .inOrder()

    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listed.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("batch-ref-1", "batch-ref-2")
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts fails with duplicate reference ID`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.batchCreateUnlinkedClientAccounts(
          batchCreateUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            requests += createUnlinkedClientAccountRequest {
              unlinkedClientAccount = unlinkedClientAccount {
                externalDataProviderId = dataProvider.externalDataProviderId
                clientAccountReferenceId = "dup"
              }
            }
            requests += createUnlinkedClientAccountRequest {
              unlinkedClientAccount = unlinkedClientAccount {
                externalDataProviderId = dataProvider.externalDataProviderId
                clientAccountReferenceId = "dup"
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts fails when external DataProvider ID is unset`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          unlinkedClientAccountsService.batchCreateUnlinkedClientAccounts(
            batchCreateUnlinkedClientAccountsRequest {
              requests += createUnlinkedClientAccountRequest {
                unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getUnlinkedClientAccount returns created account`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val created =
      unlinkedClientAccountsService.createUnlinkedClientAccount(
        createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-1"
          }
        }
      )

    val response =
      unlinkedClientAccountsService.getUnlinkedClientAccount(
        getUnlinkedClientAccountRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-1"
        }
      )

    assertThat(response).isEqualTo(created)
  }

  @Test
  fun `getUnlinkedClientAccount fails when not found`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.getUnlinkedClientAccount(
          getUnlinkedClientAccountRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "missing"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.UNLINKED_CLIENT_ACCOUNT_NOT_FOUND.name
          metadata["external_data_provider_id"] = dataProvider.externalDataProviderId.toString()
          metadata["client_account_reference_id"] = "missing"
        }
      )
  }

  @Test
  fun `deleteUnlinkedClientAccount removes account`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val created =
      unlinkedClientAccountsService.createUnlinkedClientAccount(
        createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-1"
          }
        }
      )

    val deleted =
      unlinkedClientAccountsService.deleteUnlinkedClientAccount(
        deleteUnlinkedClientAccountRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-1"
        }
      )
    assertThat(deleted).isEqualTo(created)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.getUnlinkedClientAccount(
          getUnlinkedClientAccountRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-1"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteUnlinkedClientAccount fails when not found`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.deleteUnlinkedClientAccount(
          deleteUnlinkedClientAccountRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "missing"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = KingdomInternalException.DOMAIN
          reason = ErrorCode.UNLINKED_CLIENT_ACCOUNT_NOT_FOUND.name
          metadata["external_data_provider_id"] = dataProvider.externalDataProviderId.toString()
          metadata["client_account_reference_id"] = "missing"
        }
      )
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts removes accounts`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    unlinkedClientAccountsService.batchCreateUnlinkedClientAccounts(
      batchCreateUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        requests += createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-1"
          }
        }
        requests += createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount {
            externalDataProviderId = dataProvider.externalDataProviderId
            clientAccountReferenceId = "ref-2"
          }
        }
      }
    )

    unlinkedClientAccountsService.batchDeleteUnlinkedClientAccounts(
      batchDeleteUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        requests += deleteUnlinkedClientAccountRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-1"
        }
        requests += deleteUnlinkedClientAccountRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-2"
        }
      }
    )

    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listed.unlinkedClientAccountsList).isEmpty()
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts fails when not found`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.batchDeleteUnlinkedClientAccounts(
          batchDeleteUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            requests += deleteUnlinkedClientAccountRequest {
              externalDataProviderId = dataProvider.externalDataProviderId
              clientAccountReferenceId = "missing"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `listUnlinkedClientAccounts returns results`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    createAccount(dataProvider, "ref-1")
    createAccount(dataProvider, "ref-2")

    val response =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )

    assertThat(response.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("ref-1", "ref-2")
      .inOrder()
  }

  @Test
  fun `listUnlinkedClientAccounts is isolated per DataProvider`(): Unit = runBlocking {
    val dataProviderA: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProviderB: DataProvider = population.createDataProvider(dataProvidersService)
    createAccount(dataProviderA, "a-ref-1")
    createAccount(dataProviderB, "b-ref-1")

    val listA =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProviderA.externalDataProviderId }
        }
      )
    assertThat(listA.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("a-ref-1")
  }

  @Test
  fun `listUnlinkedClientAccounts can paginate using pageToken`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    createAccount(dataProvider, "ref-1")
    createAccount(dataProvider, "ref-2")

    val page1 =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          pageSize = 1
        }
      )
    assertThat(page1.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("ref-1")
    assertThat(page1.hasNextPageToken()).isTrue()

    val page2 =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          pageSize = 1
          pageToken = page1.nextPageToken
        }
      )
    assertThat(page2.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("ref-2")
  }

  @Test
  fun `listUnlinkedClientAccounts coerces page size above max`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    createAccount(dataProvider, "ref-1")

    val response =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          pageSize = MAX_PAGE_SIZE + 1
        }
      )

    assertThat(response.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("ref-1")
  }

  @Test
  fun `listUnlinkedClientAccounts fails with negative page size`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
            pageSize = -1
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  private suspend fun createAccount(
    dataProvider: DataProvider,
    referenceId: String,
  ): UnlinkedClientAccount {
    return unlinkedClientAccountsService.createUnlinkedClientAccount(
      createUnlinkedClientAccountRequest {
        unlinkedClientAccount = unlinkedClientAccount {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = referenceId
        }
      }
    )
  }

  companion object {
    private const val RANDOM_SEED = 1
    private const val MAX_PAGE_SIZE = 1000

    private val ENTITY_METADATA = struct {
      fields["brand"] = value { stringValue = "Blammo!" }
      fields["objective"] = value { stringValue = "awareness" }
    }
  }
}
