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
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Instant
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
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroupKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.replaceUnlinkedClientAccountsRequest
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
  fun `replaceUnlinkedClientAccounts inserts new accounts`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)
    val startTime = Instant.now()

    val response =
      unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          unlinkedClientAccounts += unlinkedClientAccount {
            clientAccountReferenceId = "ref-1"
            brands += "brand-a"
            brands += "brand-b"
            eventGroupReferenceId = "eg-1"
          }
        }
      )

    assertThat(response.unlinkedClientAccountsList).hasSize(1)
    val account = response.unlinkedClientAccountsList.single()
    assertThat(account)
      .ignoringFields(UnlinkedClientAccount.FIRST_OBSERVED_TIME_FIELD_NUMBER)
      .isEqualTo(
        unlinkedClientAccount {
          externalDataProviderId = dataProvider.externalDataProviderId
          clientAccountReferenceId = "ref-1"
          brands += "brand-a"
          brands += "brand-b"
          eventGroupReferenceId = "eg-1"
        }
      )
    assertThat(account.hasFirstObservedTime()).isTrue()
    // FirstObservedTime is stamped with the transaction commit timestamp.
    assertThat(account.firstObservedTime.toInstant()).isGreaterThan(startTime)
    // The value returned equals the value persisted (the commit timestamp).
    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listed.unlinkedClientAccountsList.single().firstObservedTime)
      .isEqualTo(account.firstObservedTime)
    // The stored row round-trips the `event_group_reference_id` oneof member.
    assertThat(listed.unlinkedClientAccountsList.single()).isEqualTo(account)
  }

  @Test
  fun `replaceUnlinkedClientAccounts round-trips entity_key`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val response =
      unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          unlinkedClientAccounts += unlinkedClientAccount {
            clientAccountReferenceId = "ref-1"
            entityKey =
              EventGroupKt.entityKey {
                entityType = "advertiser"
                entityId = "acct-123"
              }
          }
        }
      )

    val account = response.unlinkedClientAccountsList.single()
    assertThat(account.hasEntityKey()).isTrue()
    assertThat(account.entityKey)
      .isEqualTo(
        EventGroupKt.entityKey {
          entityType = "advertiser"
          entityId = "acct-123"
        }
      )
    assertThat(account.hasEventGroupReferenceId()).isFalse()

    val listed =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    // The entity_key oneof member round-trips through storage.
    assertThat(listed.unlinkedClientAccountsList.single()).isEqualTo(account)
  }

  @Test
  fun `replaceUnlinkedClientAccounts preserves FirstObservedTime for re-observed account`(): Unit =
    runBlocking {
      val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

      val first =
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
          }
        )
      val firstObservedTime = first.unlinkedClientAccountsList.single().firstObservedTime

      val second =
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount {
              clientAccountReferenceId = "ref-1"
              brands += "brand-updated"
            }
          }
        )
      val secondAccount = second.unlinkedClientAccountsList.single()

      assertThat(secondAccount.firstObservedTime).isEqualTo(firstObservedTime)
      assertThat(secondAccount.brandsList).containsExactly("brand-updated")
    }

  @Test
  fun `replaceUnlinkedClientAccounts deletes accounts no longer present`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-2" }
      }
    )

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-2" }
      }
    )

    val listResponse =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )

    assertThat(listResponse.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("ref-2")
  }

  @Test
  fun `replaceUnlinkedClientAccounts with empty set deletes all accounts`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
      }
    )

    val response =
      unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
        }
      )
    assertThat(response.unlinkedClientAccountsList).isEmpty()

    val listResponse =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
        }
      )
    assertThat(listResponse.unlinkedClientAccountsList).isEmpty()
  }

  @Test
  fun `replaceUnlinkedClientAccounts fails when DataProvider not found`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = 404L
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
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
  fun `replaceUnlinkedClientAccounts fails with duplicate reference ID`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "dup" }
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "dup" }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listUnlinkedClientAccounts can paginate using pageToken`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-2" }
      }
    )

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
  fun `replaceUnlinkedClientAccounts is isolated per DataProvider`(): Unit = runBlocking {
    val dataProviderA: DataProvider = population.createDataProvider(dataProvidersService)
    val dataProviderB: DataProvider = population.createDataProvider(dataProvidersService)

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProviderA.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "a-ref-1" }
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "a-ref-2" }
      }
    )
    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProviderB.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "b-ref-1" }
      }
    )

    // Reconcile DP-A down to an empty set; DP-B rows must be untouched.
    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProviderA.externalDataProviderId
      }
    )

    val listA =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProviderA.externalDataProviderId }
        }
      )
    assertThat(listA.unlinkedClientAccountsList).isEmpty()

    val listB =
      unlinkedClientAccountsService.listUnlinkedClientAccounts(
        listUnlinkedClientAccountsRequest {
          this.filter = filter { externalDataProviderId = dataProviderB.externalDataProviderId }
        }
      )
    assertThat(listB.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
      .containsExactly("b-ref-1")
  }

  @Test
  fun `replaceUnlinkedClientAccounts handles insert, keep, and delete in a single call`(): Unit =
    runBlocking {
      val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

      val first =
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "keep" }
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "drop" }
          }
        )
      val keptFirstObservedTime =
        first.unlinkedClientAccountsList
          .single { it.clientAccountReferenceId == "keep" }
          .firstObservedTime

      // In a single call: keep "keep", drop "drop", and add "add".
      val second =
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "keep" }
            unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "add" }
          }
        )

      assertThat(second.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly("keep", "add")
      val kept = second.unlinkedClientAccountsList.single { it.clientAccountReferenceId == "keep" }
      assertThat(kept.firstObservedTime).isEqualTo(keptFirstObservedTime)

      val listResponse =
        unlinkedClientAccountsService.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            this.filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
      assertThat(listResponse.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly("keep", "add")
    }

  @Test
  fun `replaceUnlinkedClientAccounts fails when external DataProvider ID is unset`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `replaceUnlinkedClientAccounts fails with empty reference ID`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts += unlinkedClientAccount { brands += "brand-a" }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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

  @Test
  fun `listUnlinkedClientAccounts coerces page size above max`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
      replaceUnlinkedClientAccountsRequest {
        externalDataProviderId = dataProvider.externalDataProviderId
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-1" }
      }
    )

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

  companion object {
    private const val RANDOM_SEED = 1
    private const val MAX_PAGE_SIZE = 1000
  }

  @Test
  fun `replaceUnlinkedClientAccounts fails when reference ID too long`(): Unit = runBlocking {
    val dataProvider: DataProvider = population.createDataProvider(dataProvidersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        unlinkedClientAccountsService.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            externalDataProviderId = dataProvider.externalDataProviderId
            unlinkedClientAccounts +=
              unlinkedClientAccount { clientAccountReferenceId = "a".repeat(37) }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
