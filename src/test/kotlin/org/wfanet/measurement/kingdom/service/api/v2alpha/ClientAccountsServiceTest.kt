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
import com.google.protobuf.Empty
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
import org.wfanet.measurement.api.v2alpha.ClientAccount
import org.wfanet.measurement.api.v2alpha.ListClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerClientAccountKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchCreateClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.batchDeleteClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.clientAccount
import org.wfanet.measurement.api.v2alpha.createClientAccountRequest
import org.wfanet.measurement.api.v2alpha.deleteClientAccountRequest
import org.wfanet.measurement.api.v2alpha.getClientAccountRequest
import org.wfanet.measurement.api.v2alpha.listClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ClientAccount as InternalClientAccount
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase as InternalClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineStub as InternalClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.batchCreateClientAccountsResponse as internalBatchCreateClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.batchDeleteClientAccountsResponse as internalBatchDeleteClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.clientAccount as internalClientAccount
import org.wfanet.measurement.internal.kingdom.createClientAccountRequest as internalCreateClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteClientAccountRequest as internalDeleteClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getClientAccountRequest as internalGetClientAccountRequest
import org.wfanet.measurement.internal.kingdom.listClientAccountsResponse as internalListClientAccountsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID_2 = 456L
private const val EXTERNAL_DATA_PROVIDER_ID = 789L
private const val EXTERNAL_CLIENT_ACCOUNT_ID = 111L
private const val EXTERNAL_CLIENT_ACCOUNT_ID_2 = 222L

private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID)).toName()
private val MEASUREMENT_CONSUMER_NAME_2 =
  MeasurementConsumerKey(externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID_2)).toName()
private val DATA_PROVIDER_NAME = makeDataProvider(EXTERNAL_DATA_PROVIDER_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(999L)

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

private val CLIENT_ACCOUNT_NAME =
  MeasurementConsumerClientAccountKey(
      externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
      externalIdToApiId(EXTERNAL_CLIENT_ACCOUNT_ID),
    )
    .toName()

private val CLIENT_ACCOUNT_NAME_2 =
  MeasurementConsumerClientAccountKey(
      externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
      externalIdToApiId(EXTERNAL_CLIENT_ACCOUNT_ID_2),
    )
    .toName()

private const val CLIENT_ACCOUNT_REFERENCE_ID = "test-reference-id"

private val CLIENT_ACCOUNT: ClientAccount = clientAccount {
  name = CLIENT_ACCOUNT_NAME
  dataProvider = DATA_PROVIDER_NAME
  clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
}

private val INTERNAL_CLIENT_ACCOUNT: InternalClientAccount = internalClientAccount {
  externalClientAccountId = EXTERNAL_CLIENT_ACCOUNT_ID
  externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
  createTime = timestamp { seconds = 12345 }
}

@RunWith(JUnit4::class)
class ClientAccountsServiceTest {
  private val internalClientAccountsMock: InternalClientAccountsCoroutineImplBase = mockService {
    onBlocking { createClientAccount(any()) }.thenReturn(INTERNAL_CLIENT_ACCOUNT)
    onBlocking { batchCreateClientAccounts(any()) }
      .thenReturn(internalBatchCreateClientAccountsResponse { clientAccounts += INTERNAL_CLIENT_ACCOUNT })
    onBlocking { getClientAccount(any()) }.thenReturn(INTERNAL_CLIENT_ACCOUNT)
    onBlocking { deleteClientAccount(any()) }.thenReturn(INTERNAL_CLIENT_ACCOUNT)
    onBlocking { batchDeleteClientAccounts(any()) }
      .thenReturn(internalBatchDeleteClientAccountsResponse { clientAccounts += INTERNAL_CLIENT_ACCOUNT })
    onBlocking { listClientAccounts(any()) }
      .thenReturn(internalListClientAccountsResponse { clientAccounts += INTERNAL_CLIENT_ACCOUNT })
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalClientAccountsMock) }

  private lateinit var service: ClientAccountsService

  @Before
  fun initService() {
    service = ClientAccountsService(InternalClientAccountsCoroutineStub(grpcTestServerRule.channel))
  }

  // createClientAccount Tests

  @Test
  fun `createClientAccount returns client account`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createClientAccount(request) }
      }

    assertThat(result).isEqualTo(CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalClientAccountsMock,
        InternalClientAccountsCoroutineImplBase::createClientAccount,
      )
      .isEqualTo(
        internalCreateClientAccountRequest {
          clientAccount = internalClientAccount {
            externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
          }
        }
      )
  }

  @Test
  fun `createClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createClientAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createClientAccount throws PERMISSION_DENIED when principal doesn't own MC`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createClientAccount throws INVALID_ARGUMENT when parent is invalid`() {
    val request = createClientAccountRequest {
      parent = "invalid"
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createClientAccount throws INVALID_ARGUMENT when client_account is missing`() {
    val request = createClientAccountRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("client_account must be specified")
  }

  @Test
  fun `createClientAccount throws INVALID_ARGUMENT when data_provider is missing`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount { clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("data_provider must be specified")
  }

  @Test
  fun `createClientAccount throws INVALID_ARGUMENT when client_account_reference_id is missing`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount { dataProvider = DATA_PROVIDER_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("client_account_reference_id must be specified")
  }

  @Test
  fun `createClientAccount throws INVALID_ARGUMENT when reference_id exceeds 36 chars`() {
    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = "a".repeat(37)
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("36 characters")
  }

  @Test
  fun `createClientAccount throws NOT_FOUND when MeasurementConsumer not found`() {
    internalClientAccountsMock.stub {
      onBlocking { createClientAccount(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
        )
    }

    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createClientAccount throws NOT_FOUND when DataProvider not found`() {
    internalClientAccountsMock.stub {
      onBlocking { createClientAccount(any()) }
        .thenThrow(
          DataProviderNotFoundException(
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_DATA_PROVIDER_ID)
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }

    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createClientAccount throws ALREADY_EXISTS when duplicate reference ID`() {
    internalClientAccountsMock.stub {
      onBlocking { createClientAccount(any()) }
        .thenThrow(
          ClientAccountAlreadyExistsException(
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              CLIENT_ACCOUNT_REFERENCE_ID,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS, "ClientAccount already exists.")
        )
    }

    val request = createClientAccountRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      clientAccount = clientAccount {
        dataProvider = DATA_PROVIDER_NAME
        clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  // getClientAccount Tests

  @Test
  fun `getClientAccount returns client account when MC principal accesses via MC parent`() {
    val request = getClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.getClientAccount(request) }
      }

    assertThat(result).isEqualTo(CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalClientAccountsMock,
        InternalClientAccountsCoroutineImplBase::getClientAccount,
      )
      .isEqualTo(
        internalGetClientAccountRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          externalClientAccountId = EXTERNAL_CLIENT_ACCOUNT_ID
        }
      )
  }

  @Test
  fun `getClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = getClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getClientAccount(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getClientAccount throws PERMISSION_DENIED when MC principal doesn't match`() {
    val request = getClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.getClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getClientAccount throws PERMISSION_DENIED when unauthorized principal type`() {
    val request = getClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getClientAccount throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getClientAccount(getClientAccountRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getClientAccount throws PERMISSION_DENIED when not found (hides existence)`() {
    internalClientAccountsMock.stub {
      onBlocking { getClientAccount(any()) }
        .thenThrow(
          ClientAccountNotFoundException(
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_CLIENT_ACCOUNT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
        )
    }

    val request = getClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  // listClientAccounts Tests

  @Test
  fun `listClientAccounts returns client accounts when MC parent`() {
    val request = listClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = 10
    }

    val result: ListClientAccountsResponse =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listClientAccounts(request) }
      }

    assertThat(result.clientAccountsList).containsExactly(CLIENT_ACCOUNT)
  }

  @Test
  fun `listClientAccounts returns client accounts when DP parent`() {
    val request = listClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
    }

    val result: ListClientAccountsResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listClientAccounts(request) }
      }

    assertThat(result.clientAccountsList).containsExactly(CLIENT_ACCOUNT)
  }

  @Test
  fun `listClientAccounts throws UNAUTHENTICATED when no principal is found`() {
    val request = listClientAccountsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listClientAccounts(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listClientAccounts throws PERMISSION_DENIED when principal mismatches`() {
    val request = listClientAccountsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listClientAccounts throws INVALID_ARGUMENT when parent is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listClientAccounts(listClientAccountsRequest { parent = "invalid" })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listClientAccounts throws INVALID_ARGUMENT when page size is negative`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listClientAccounts(
              listClientAccountsRequest {
                parent = MEASUREMENT_CONSUMER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  // deleteClientAccount Tests

  @Test
  fun `deleteClientAccount returns empty when successful`() {
    val request = deleteClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val result: Empty =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.deleteClientAccount(request) }
      }

    assertThat(result).isEqualTo(Empty.getDefaultInstance())

    verifyProtoArgument(
        internalClientAccountsMock,
        InternalClientAccountsCoroutineImplBase::deleteClientAccount,
      )
      .isEqualTo(
        internalDeleteClientAccountRequest {
          externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
          externalClientAccountId = EXTERNAL_CLIENT_ACCOUNT_ID
        }
      )
  }

  @Test
  fun `deleteClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.deleteClientAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteClientAccount throws PERMISSION_DENIED when principal doesn't own MC`() {
    val request = deleteClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.deleteClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteClientAccount throws PERMISSION_DENIED when DP tries to delete`() {
    val request = deleteClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteClientAccount throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteClientAccount(deleteClientAccountRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteClientAccount throws PERMISSION_DENIED when not found`() {
    internalClientAccountsMock.stub {
      onBlocking { deleteClientAccount(any()) }
        .thenThrow(
          ClientAccountNotFoundException(
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_MEASUREMENT_CONSUMER_ID),
              org.wfanet.measurement.common.identity.ExternalId(EXTERNAL_CLIENT_ACCOUNT_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
        )
    }

    val request = deleteClientAccountRequest { name = CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  // batchCreateClientAccounts Tests

  @Test
  fun `batchCreateClientAccounts returns client accounts`() {
    val request = batchCreateClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createClientAccountRequest {
        parent = MEASUREMENT_CONSUMER_NAME
        clientAccount = clientAccount {
          dataProvider = DATA_PROVIDER_NAME
          clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
        }
      }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchCreateClientAccounts(request) }
      }

    assertThat(result.clientAccountsList).containsExactly(CLIENT_ACCOUNT)
  }

  @Test
  fun `batchCreateClientAccounts throws INVALID_ARGUMENT when parent mismatch`() {
    val request = batchCreateClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requests += createClientAccountRequest {
        parent = MEASUREMENT_CONSUMER_NAME_2
        clientAccount = clientAccount {
          dataProvider = DATA_PROVIDER_NAME
          clientAccountReferenceId = CLIENT_ACCOUNT_REFERENCE_ID
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
  
  // batchDeleteClientAccounts Tests

  @Test
  fun `batchDeleteClientAccounts returns empty when successful`() {
    val request = batchDeleteClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += CLIENT_ACCOUNT_NAME
    }

    val result: Empty =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchDeleteClientAccounts(request) }
      }

    assertThat(result).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `batchDeleteClientAccounts throws PERMISSION_DENIED when DP tries to delete`() {
    val request = batchDeleteClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += CLIENT_ACCOUNT_NAME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchDeleteClientAccounts throws INVALID_ARGUMENT when name doesn't match parent`() {
    val differentMcClientAccountName =
      MeasurementConsumerClientAccountKey(
          externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID_2),
          externalIdToApiId(EXTERNAL_CLIENT_ACCOUNT_ID),
        )
        .toName()

    val request = batchDeleteClientAccountsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      names += differentMcClientAccountName
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchDeleteClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("does not match parent")
  }
}
