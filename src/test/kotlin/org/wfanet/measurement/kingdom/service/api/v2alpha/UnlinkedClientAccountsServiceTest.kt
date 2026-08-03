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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountKey
import org.wfanet.measurement.api.v2alpha.replaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.unlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase as InternalUnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineStub as InternalUnlinkedClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.replaceUnlinkedClientAccountsRequest as internalReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.replaceUnlinkedClientAccountsResponse as internalReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount as internalUnlinkedClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException

private const val EXTERNAL_DATA_PROVIDER_ID = 789L
private val DATA_PROVIDER_NAME = makeDataProvider(EXTERNAL_DATA_PROVIDER_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(999L)
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val REFERENCE_ID = "ref-1"

private val INTERNAL_UNLINKED_CLIENT_ACCOUNT = internalUnlinkedClientAccount {
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  clientAccountReferenceId = REFERENCE_ID
  brands += "brand-a"
  eventGroupReferenceId = "eg-1"
  firstObservedTime = timestamp { seconds = 12345 }
}

private val UNLINKED_CLIENT_ACCOUNT: UnlinkedClientAccount = unlinkedClientAccount {
  name =
    UnlinkedClientAccountKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID), REFERENCE_ID).toName()
  clientAccountReferenceId = REFERENCE_ID
  brands += "brand-a"
  eventGroupReferenceId = "eg-1"
  firstObservedTime = timestamp { seconds = 12345 }
}

@RunWith(JUnit4::class)
class UnlinkedClientAccountsServiceTest {
  private val internalServiceMock: InternalUnlinkedClientAccountsCoroutineImplBase = mockService {
    onBlocking { replaceUnlinkedClientAccounts(any()) }
      .thenReturn(
        internalReplaceUnlinkedClientAccountsResponse {
          unlinkedClientAccounts += INTERNAL_UNLINKED_CLIENT_ACCOUNT
        }
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: UnlinkedClientAccountsService

  @Before
  fun initService() {
    service =
      UnlinkedClientAccountsService(
        InternalUnlinkedClientAccountsCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `replaceUnlinkedClientAccounts returns reconciled accounts and delegates`() {
    val request = replaceUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccounts += unlinkedClientAccount {
        clientAccountReferenceId = REFERENCE_ID
        brands += "brand-a"
        eventGroupReferenceId = "eg-1"
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.replaceUnlinkedClientAccounts(request) }
      }

    assertThat(result.unlinkedClientAccountsList).containsExactly(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::replaceUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalReplaceUnlinkedClientAccountsRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          unlinkedClientAccounts += internalUnlinkedClientAccount {
            clientAccountReferenceId = REFERENCE_ID
            brands += "brand-a"
            eventGroupReferenceId = "eg-1"
          }
        }
      )
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws UNAUTHENTICATED when no principal is found`() {
    val request = replaceUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.replaceUnlinkedClientAccounts(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws PERMISSION_DENIED when principal does not own DP`() {
    val request = replaceUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws PERMISSION_DENIED for wrong principal type`() {
    val request = replaceUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when parent is invalid`() {
    val request = replaceUnlinkedClientAccountsRequest { parent = "invalid" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when reference id is empty`() {
    val request = replaceUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccounts += unlinkedClientAccount { brands += "brand-a" }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when reference id is duplicated`() {
    val request = replaceUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "dup" }
      unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "dup" }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws INVALID_ARGUMENT when batch size exceeded`() {
    val request = replaceUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      for (i in 0 until 1001) {
        unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = "ref-$i" }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `replaceUnlinkedClientAccounts throws NOT_FOUND when DataProvider not found`() {
    internalServiceMock.stub {
      onBlocking { replaceUnlinkedClientAccounts(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }

    val request = replaceUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccounts += unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.replaceUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }
}
