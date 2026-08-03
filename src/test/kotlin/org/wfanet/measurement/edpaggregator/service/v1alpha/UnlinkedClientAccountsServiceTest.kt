// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerUnlinkedClientAccountsService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.UnlinkedClientAccountKey
import org.wfanet.measurement.edpaggregator.v1alpha.replaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.unlinkedClientAccount
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineImplBase as InternalUnlinkedClientAccountsServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineStub as InternalUnlinkedClientAccountsServiceCoroutineStub

@RunWith(JUnit4::class)
class UnlinkedClientAccountsServiceTest {
  private lateinit var internalService: InternalUnlinkedClientAccountsServiceCoroutineImplBase
  private lateinit var service: UnlinkedClientAccountsService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService =
      SpannerUnlinkedClientAccountsService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      UnlinkedClientAccountsService(
        InternalUnlinkedClientAccountsServiceCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `replaceUnlinkedClientAccounts delegates to internal service and returns accounts`() =
    runBlocking<Unit> {
      val startTime = Instant.now()

      val response =
        service.replaceUnlinkedClientAccounts(
          replaceUnlinkedClientAccountsRequest {
            parent = DATA_PROVIDER_KEY.toName()
            unlinkedClientAccounts += unlinkedClientAccount {
              clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
              brands += "brand-a"
              eventGroupReferenceId = "event-group-a"
            }
          }
        )

      val account = response.unlinkedClientAccountsList.single()
      assertThat(account.clientAccountReferenceId).isEqualTo(CLIENT_ACCOUNT_REFERENCE_ID)
      assertThat(account.brandsList).containsExactly("brand-a")
      assertThat(account.eventGroupReferenceId).isEqualTo("event-group-a")
      assertThat(account.firstObservedTime.toInstant()).isGreaterThan(startTime)

      val key = assertNotNull(UnlinkedClientAccountKey.fromName(account.name))
      assertThat(key.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(key.unlinkedClientAccountId).isEqualTo(CLIENT_ACCOUNT_REFERENCE_ID)
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when parent is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              unlinkedClientAccounts += unlinkedClientAccount {
                clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when parent is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              parent = "not-a-valid-data-provider-name"
              unlinkedClientAccounts += unlinkedClientAccount {
                clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when reference id is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              parent = DATA_PROVIDER_KEY.toName()
              unlinkedClientAccounts += unlinkedClientAccount { brands += "brand-a" }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when reference id is duplicated`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.replaceUnlinkedClientAccounts(
            replaceUnlinkedClientAccountsRequest {
              parent = DATA_PROVIDER_KEY.toName()
              unlinkedClientAccounts += unlinkedClientAccount {
                clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
              }
              unlinkedClientAccounts += unlinkedClientAccount {
                clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  companion object {
    private const val DATA_PROVIDER_ID = "data-provider-1"
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private const val CLIENT_ACCOUNT_REFERENCE_ID = "client-account-a"

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
