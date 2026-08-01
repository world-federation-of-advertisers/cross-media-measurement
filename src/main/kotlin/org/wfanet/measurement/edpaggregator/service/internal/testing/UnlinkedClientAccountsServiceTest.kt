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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.ListUnlinkedClientAccountsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccount
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.edpaggregator.listUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.edpaggregator.replaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.edpaggregator.unlinkedClientAccount

@RunWith(JUnit4::class)
abstract class UnlinkedClientAccountsServiceTest {
  private lateinit var service: UnlinkedClientAccountsServiceCoroutineImplBase

  protected abstract fun newService(): UnlinkedClientAccountsServiceCoroutineImplBase

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `replaceUnlinkedClientAccounts inserts new accounts with FirstObservedTime`() =
    runBlocking<Unit> {
      val startTime = Instant.now()

      val response =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
          }
        )

      assertThat(response.unlinkedClientAccountsList)
        .ignoringFields(UnlinkedClientAccount.FIRST_OBSERVED_TIME_FIELD_NUMBER)
        .containsExactly(
          UNLINKED_CLIENT_ACCOUNT_A.copy { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID },
          UNLINKED_CLIENT_ACCOUNT_B.copy { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID },
        )
      for (account in response.unlinkedClientAccountsList) {
        assertThat(account.firstObservedTime.toInstant()).isGreaterThan(startTime)
      }

      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listed.unlinkedClientAccountsList)
        .containsExactlyElementsIn(response.unlinkedClientAccountsList)
    }

  @Test
  fun `replaceUnlinkedClientAccounts preserves FirstObservedTime for still-unlinked accounts`() =
    runBlocking {
      val firstResponse =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
          }
        )
      val originalFirstObservedTime =
        firstResponse.unlinkedClientAccountsList.single().firstObservedTime

      // Second reconcile still contains account A, with updated brands and event group.
      val secondResponse =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            unlinkedClientAccounts +=
              UNLINKED_CLIENT_ACCOUNT_A.copy {
                brands.clear()
                brands += "brand-updated"
                eventGroupReferenceId = "event-group-updated"
              }
          }
        )

      val account = secondResponse.unlinkedClientAccountsList.single()
      assertThat(account.firstObservedTime).isEqualTo(originalFirstObservedTime)
      assertThat(account.brandsList).containsExactly("brand-updated")
      assertThat(account.eventGroupReferenceId).isEqualTo("event-group-updated")

      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      val listedAccount = listed.unlinkedClientAccountsList.single()
      assertThat(listedAccount.firstObservedTime).isEqualTo(originalFirstObservedTime)
      assertThat(listedAccount.eventGroupReferenceId).isEqualTo("event-group-updated")
    }

  @Test
  fun `replaceUnlinkedClientAccounts deletes accounts absent from the new set`() =
    runBlocking<Unit> {
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
        }
      )

      // Account B is no longer unlinked; only A remains.
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
        }
      )

      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listed.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_A)
    }

  @Test
  fun `replaceUnlinkedClientAccounts handles insert, keep, and delete in a single call`() =
    runBlocking<Unit> {
      val firstResponse =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
          }
        )
      val originalFirstObservedTimeA =
        firstResponse.unlinkedClientAccountsList
          .single { it.clientAccountReferenceId == CLIENT_ACCOUNT_REFERENCE_ID_A }
          .firstObservedTime

      // Keep A, delete B, insert C -- all in one reconcile call.
      val secondResponse =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
            unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_C
          }
        )

      assertThat(secondResponse.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_A, CLIENT_ACCOUNT_REFERENCE_ID_C)
      val keptA =
        secondResponse.unlinkedClientAccountsList.single {
          it.clientAccountReferenceId == CLIENT_ACCOUNT_REFERENCE_ID_A
        }
      assertThat(keptA.firstObservedTime).isEqualTo(originalFirstObservedTimeA)

      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listed.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_A, CLIENT_ACCOUNT_REFERENCE_ID_C)
    }

  @Test
  fun `replaceUnlinkedClientAccounts is isolated per DataProvider`() =
    runBlocking<Unit> {
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
        }
      )
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = OTHER_DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_C
        }
      )

      // Re-reconcile DP-1 down to {A}; DP-2 must be unaffected.
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
        }
      )

      val listedDp1 =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listedDp1.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_A)

      val listedDp2 =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            dataProviderResourceId = OTHER_DATA_PROVIDER_RESOURCE_ID
          }
        )
      assertThat(listedDp2.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_C)
    }

  @Test
  fun `replaceUnlinkedClientAccounts with empty set deletes all stored accounts`() =
    runBlocking<Unit> {
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
        }
      )

      val response =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )
      assertThat(response.unlinkedClientAccountsList).isEmpty()

      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listed.unlinkedClientAccountsList).isEmpty()
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_provider_resource_id"
          }
        )
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT if clientAccountReferenceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              unlinkedClientAccounts += unlinkedClientAccount { brands += "brand-a" }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "unlinked_client_accounts.0.client_account_reference_id"
          }
        )
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT for duplicate reference IDs`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
              unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(Errors.getReason(exception.errorInfo!!))
        .isEqualTo(Errors.Reason.INVALID_FIELD_VALUE)
    }

  @Test
  fun `listUnlinkedClientAccounts returns empty for unknown DataProvider`() =
    runBlocking<Unit> {
      val listed =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      assertThat(listed.unlinkedClientAccountsList).isEmpty()
    }

  @Test
  fun `listUnlinkedClientAccounts throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listUnlinkedClientAccounts(listUnlinkedClientAccountsRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_provider_resource_id"
          }
        )
    }

  @Test
  fun `listUnlinkedClientAccounts paginates results`() =
    runBlocking<Unit> {
      service.replaceUnlinkedClientAccounts(
        replaceUnlinkedClientAccountsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_A
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_B
          unlinkedClientAccounts += UNLINKED_CLIENT_ACCOUNT_C
        }
      )

      val firstPage =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = 2
          }
        )
      assertThat(firstPage.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_A, CLIENT_ACCOUNT_REFERENCE_ID_B)
        .inOrder()
      assertThat(firstPage.hasNextPageToken()).isTrue()
      assertThat(firstPage.nextPageToken.after.clientAccountReferenceId)
        .isEqualTo(CLIENT_ACCOUNT_REFERENCE_ID_B)

      val secondPage =
        service.listUnlinkedClientAccounts(
          listUnlinkedClientAccountsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = 2
            pageToken = listUnlinkedClientAccountsPageToken {
              after =
                ListUnlinkedClientAccountsPageTokenKt.after {
                  clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID_B
                }
            }
          }
        )
      assertThat(secondPage.unlinkedClientAccountsList.map { it.clientAccountReferenceId })
        .containsExactly(CLIENT_ACCOUNT_REFERENCE_ID_C)
      assertThat(secondPage.hasNextPageToken()).isFalse()
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val OTHER_DATA_PROVIDER_RESOURCE_ID = "data-provider-2"
    private const val CLIENT_ACCOUNT_REFERENCE_ID_A = "client-account-a"
    private const val CLIENT_ACCOUNT_REFERENCE_ID_B = "client-account-b"
    private const val CLIENT_ACCOUNT_REFERENCE_ID_C = "client-account-c"

    private val UNLINKED_CLIENT_ACCOUNT_A = unlinkedClientAccount {
      clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID_A
      brands += "brand-a"
      brands += "brand-b"
      eventGroupReferenceId = "event-group-a"
    }

    private val UNLINKED_CLIENT_ACCOUNT_B = unlinkedClientAccount {
      clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID_B
      brands += "brand-c"
      eventGroupReferenceId = "event-group-b"
    }

    private val UNLINKED_CLIENT_ACCOUNT_C = unlinkedClientAccount {
      clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID_C
      brands += "brand-d"
      eventGroupReferenceId = "event-group-c"
    }
  }
}
