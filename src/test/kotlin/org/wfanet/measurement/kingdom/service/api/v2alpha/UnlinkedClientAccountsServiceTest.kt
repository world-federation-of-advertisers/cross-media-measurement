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
import com.google.protobuf.struct
import com.google.protobuf.timestamp
import com.google.protobuf.value
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
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountKey
import org.wfanet.measurement.api.v2alpha.batchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.batchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.createUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.deleteUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.getUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.listUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.unlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.EventGroupKt as InternalEventGroupKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequestKt
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount as InternalUnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase as InternalUnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineStub as InternalUnlinkedClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.batchCreateUnlinkedClientAccountsRequest as internalBatchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.batchCreateUnlinkedClientAccountsResponse as internalBatchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.batchDeleteUnlinkedClientAccountsRequest as internalBatchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.createUnlinkedClientAccountRequest as internalCreateUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteUnlinkedClientAccountRequest as internalDeleteUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getUnlinkedClientAccountRequest as internalGetUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsPageToken as internalListUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsRequest as internalListUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsResponse as internalListUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount as internalUnlinkedClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountNotFoundException

private const val EXTERNAL_DATA_PROVIDER_ID = 789L
private const val EXTERNAL_DATA_PROVIDER_ID_2 = 999L
private val DATA_PROVIDER_NAME = makeDataProvider(EXTERNAL_DATA_PROVIDER_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(EXTERNAL_DATA_PROVIDER_ID_2)
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val REFERENCE_ID = "ref-1"

private val UNLINKED_CLIENT_ACCOUNT_NAME =
  UnlinkedClientAccountKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID), REFERENCE_ID).toName()

private val ENTITY_METADATA = struct { fields["brand"] = value { stringValue = "Blammo!" } }

private val INTERNAL_UNLINKED_CLIENT_ACCOUNT: InternalUnlinkedClientAccount =
  internalUnlinkedClientAccount {
    externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
    clientAccountReferenceId = REFERENCE_ID
    entityMetadata = ENTITY_METADATA
    eventGroupReferenceId = "eg-1"
    createTime = timestamp { seconds = 12345 }
  }

private val UNLINKED_CLIENT_ACCOUNT: UnlinkedClientAccount = unlinkedClientAccount {
  name = UNLINKED_CLIENT_ACCOUNT_NAME
  clientAccountReferenceId = REFERENCE_ID
  entityMetadata = ENTITY_METADATA
  eventGroupReferenceId = "eg-1"
  createTime = timestamp { seconds = 12345 }
}

@RunWith(JUnit4::class)
class UnlinkedClientAccountsServiceTest {
  private val internalServiceMock: InternalUnlinkedClientAccountsCoroutineImplBase = mockService {
    onBlocking { createUnlinkedClientAccount(any()) }.thenReturn(INTERNAL_UNLINKED_CLIENT_ACCOUNT)
    onBlocking { batchCreateUnlinkedClientAccounts(any()) }
      .thenReturn(
        internalBatchCreateUnlinkedClientAccountsResponse {
          unlinkedClientAccounts += INTERNAL_UNLINKED_CLIENT_ACCOUNT
        }
      )
    onBlocking { getUnlinkedClientAccount(any()) }.thenReturn(INTERNAL_UNLINKED_CLIENT_ACCOUNT)
    onBlocking { deleteUnlinkedClientAccount(any()) }.thenReturn(INTERNAL_UNLINKED_CLIENT_ACCOUNT)
    onBlocking { batchDeleteUnlinkedClientAccounts(any()) }.thenReturn(Empty.getDefaultInstance())
    onBlocking { listUnlinkedClientAccounts(any()) }
      .thenReturn(
        internalListUnlinkedClientAccountsResponse {
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

  // createUnlinkedClientAccount tests

  @Test
  fun `createUnlinkedClientAccount returns account and translates request`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount {
        clientAccountReferenceId = REFERENCE_ID
        entityMetadata = ENTITY_METADATA
        eventGroupReferenceId = "eg-1"
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createUnlinkedClientAccount(request) }
      }

    assertThat(result).isEqualTo(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::createUnlinkedClientAccount,
      )
      .isEqualTo(
        internalCreateUnlinkedClientAccountRequest {
          unlinkedClientAccount = internalUnlinkedClientAccount {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            clientAccountReferenceId = REFERENCE_ID
            entityMetadata = ENTITY_METADATA
            eventGroupReferenceId = "eg-1"
          }
        }
      )
  }

  @Test
  fun `createUnlinkedClientAccount translates entity_key`() {
    internalServiceMock.stub {
      onBlocking { createUnlinkedClientAccount(any()) }
        .thenReturn(
          internalUnlinkedClientAccount {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            clientAccountReferenceId = REFERENCE_ID
            entityKey =
              InternalEventGroupKt.entityKey {
                entityType = "advertiser"
                entityId = "acct-123"
              }
          }
        )
    }

    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount {
        clientAccountReferenceId = REFERENCE_ID
        entityKey =
          EventGroupKt.entityKey {
            entityType = "advertiser"
            entityId = "acct-123"
          }
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createUnlinkedClientAccount(request) }
      }

    assertThat(result.hasEntityKey()).isTrue()
    assertThat(result.entityKey)
      .isEqualTo(
        EventGroupKt.entityKey {
          entityType = "advertiser"
          entityId = "acct-123"
        }
      )

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::createUnlinkedClientAccount,
      )
      .isEqualTo(
        internalCreateUnlinkedClientAccountRequest {
          unlinkedClientAccount = internalUnlinkedClientAccount {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            clientAccountReferenceId = REFERENCE_ID
            entityKey =
              InternalEventGroupKt.entityKey {
                entityType = "advertiser"
                entityId = "acct-123"
              }
          }
        }
      )
  }

  @Test
  fun `createUnlinkedClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createUnlinkedClientAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createUnlinkedClientAccount throws PERMISSION_DENIED when principal does not own DP`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createUnlinkedClientAccount throws PERMISSION_DENIED for wrong principal type`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createUnlinkedClientAccount throws INVALID_ARGUMENT when parent is invalid`() {
    val request = createUnlinkedClientAccountRequest {
      parent = "invalid"
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createUnlinkedClientAccount throws INVALID_ARGUMENT when account missing`() {
    val request = createUnlinkedClientAccountRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createUnlinkedClientAccount throws INVALID_ARGUMENT when reference id is empty`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { entityMetadata = ENTITY_METADATA }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createUnlinkedClientAccount throws INVALID_ARGUMENT when reference id too long`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "a".repeat(37) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("36 characters")
  }

  @Test
  fun `createUnlinkedClientAccount throws INVALID_ARGUMENT when reference id is not URL-safe`() {
    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "not/url safe" }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("URL-safe")
  }

  @Test
  fun `createUnlinkedClientAccount throws NOT_FOUND when DataProvider not found`() {
    internalServiceMock.stub {
      onBlocking { createUnlinkedClientAccount(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(EXTERNAL_DATA_PROVIDER_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
        )
    }

    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createUnlinkedClientAccount throws ALREADY_EXISTS when duplicate reference id`() {
    internalServiceMock.stub {
      onBlocking { createUnlinkedClientAccount(any()) }
        .thenThrow(
          UnlinkedClientAccountAlreadyExistsException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              REFERENCE_ID,
            )
            .asStatusRuntimeException(
              Status.Code.ALREADY_EXISTS,
              "UnlinkedClientAccount already exists.",
            )
        )
    }

    val request = createUnlinkedClientAccountRequest {
      parent = DATA_PROVIDER_NAME
      unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  // batchCreateUnlinkedClientAccounts tests

  @Test
  fun `batchCreateUnlinkedClientAccounts returns accounts and translates request`() {
    val request = batchCreateUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createUnlinkedClientAccountRequest {
        parent = DATA_PROVIDER_NAME
        unlinkedClientAccount = unlinkedClientAccount {
          clientAccountReferenceId = REFERENCE_ID
          entityMetadata = ENTITY_METADATA
          eventGroupReferenceId = "eg-1"
        }
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
      }

    assertThat(result.unlinkedClientAccountsList).containsExactly(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::batchCreateUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalBatchCreateUnlinkedClientAccountsRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          requests += internalCreateUnlinkedClientAccountRequest {
            unlinkedClientAccount = internalUnlinkedClientAccount {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              clientAccountReferenceId = REFERENCE_ID
              entityMetadata = ENTITY_METADATA
              eventGroupReferenceId = "eg-1"
            }
          }
        }
      )
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts throws PERMISSION_DENIED for wrong principal type`() {
    val request = batchCreateUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts throws INVALID_ARGUMENT when parent mismatch`() {
    val request = batchCreateUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createUnlinkedClientAccountRequest {
        parent = DATA_PROVIDER_NAME_2
        unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = REFERENCE_ID }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts allows unset parent in child request`() {
    val request = batchCreateUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createUnlinkedClientAccountRequest {
        unlinkedClientAccount = unlinkedClientAccount {
          clientAccountReferenceId = REFERENCE_ID
          entityMetadata = ENTITY_METADATA
          eventGroupReferenceId = "eg-1"
        }
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
      }

    assertThat(result.unlinkedClientAccountsList).containsExactly(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::batchCreateUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalBatchCreateUnlinkedClientAccountsRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          requests += internalCreateUnlinkedClientAccountRequest {
            unlinkedClientAccount = internalUnlinkedClientAccount {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
              clientAccountReferenceId = REFERENCE_ID
              entityMetadata = ENTITY_METADATA
              eventGroupReferenceId = "eg-1"
            }
          }
        }
      )
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts throws INVALID_ARGUMENT when reference id duplicated`() {
    val request = batchCreateUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createUnlinkedClientAccountRequest {
        unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "dup" }
      }
      requests += createUnlinkedClientAccountRequest {
        unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "dup" }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateUnlinkedClientAccounts throws INVALID_ARGUMENT when batch size exceeded`() {
    val request = batchCreateUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      for (i in 0 until 1001) {
        requests += createUnlinkedClientAccountRequest {
          unlinkedClientAccount = unlinkedClientAccount { clientAccountReferenceId = "ref-$i" }
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  // getUnlinkedClientAccount tests

  @Test
  fun `getUnlinkedClientAccount returns account and translates request`() {
    val request = getUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getUnlinkedClientAccount(request) }
      }

    assertThat(result).isEqualTo(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::getUnlinkedClientAccount,
      )
      .isEqualTo(
        internalGetUnlinkedClientAccountRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          clientAccountReferenceId = REFERENCE_ID
        }
      )
  }

  @Test
  fun `getUnlinkedClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = getUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.getUnlinkedClientAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getUnlinkedClientAccount throws PERMISSION_DENIED when principal does not own DP`() {
    val request = getUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.getUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getUnlinkedClientAccount throws PERMISSION_DENIED for wrong principal type`() {
    val request = getUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getUnlinkedClientAccount throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getUnlinkedClientAccount(getUnlinkedClientAccountRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getUnlinkedClientAccount throws PERMISSION_DENIED when not found`() {
    internalServiceMock.stub {
      onBlocking { getUnlinkedClientAccount(any()) }
        .thenThrow(
          UnlinkedClientAccountNotFoundException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              REFERENCE_ID,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "UnlinkedClientAccount not found.")
        )
    }

    val request = getUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  // listUnlinkedClientAccounts tests

  @Test
  fun `listUnlinkedClientAccounts returns accounts and translates filter`() {
    val request = listUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listUnlinkedClientAccounts(request) }
      }

    assertThat(result.unlinkedClientAccountsList).containsExactly(UNLINKED_CLIENT_ACCOUNT)

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::listUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalListUnlinkedClientAccountsRequest {
          filter =
            ListUnlinkedClientAccountsRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            }
          pageSize = 10
        }
      )
  }

  @Test
  fun `listUnlinkedClientAccounts encodes next page token`() {
    val internalToken = internalListUnlinkedClientAccountsPageToken {
      after =
        ListUnlinkedClientAccountsPageTokenKt.after {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          clientAccountReferenceId = REFERENCE_ID
        }
    }
    internalServiceMock.stub {
      onBlocking { listUnlinkedClientAccounts(any()) }
        .thenReturn(
          internalListUnlinkedClientAccountsResponse {
            unlinkedClientAccounts += INTERNAL_UNLINKED_CLIENT_ACCOUNT
            nextPageToken = internalToken
          }
        )
    }

    val request = listUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listUnlinkedClientAccounts(request) }
      }

    assertThat(result.nextPageToken).isEqualTo(internalToken.toByteString().base64UrlEncode())
  }

  @Test
  fun `listUnlinkedClientAccounts decodes page token and passes it through`() {
    val internalToken = internalListUnlinkedClientAccountsPageToken {
      after =
        ListUnlinkedClientAccountsPageTokenKt.after {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          clientAccountReferenceId = REFERENCE_ID
        }
    }

    val request = listUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
      pageToken = internalToken.toByteString().base64UrlEncode()
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listUnlinkedClientAccounts(request) }
    }

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::listUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalListUnlinkedClientAccountsRequest {
          filter =
            ListUnlinkedClientAccountsRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            }
          pageSize = 10
          pageToken = internalToken
        }
      )
  }

  @Test
  fun `listUnlinkedClientAccounts throws INVALID_ARGUMENT when page token DP mismatch`() {
    val internalToken = internalListUnlinkedClientAccountsPageToken {
      after =
        ListUnlinkedClientAccountsPageTokenKt.after {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID_2
          clientAccountReferenceId = REFERENCE_ID
        }
    }

    val request = listUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageToken = internalToken.toByteString().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listUnlinkedClientAccounts throws UNAUTHENTICATED when no principal is found`() {
    val request = listUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.listUnlinkedClientAccounts(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listUnlinkedClientAccounts throws PERMISSION_DENIED when principal mismatches`() {
    val request = listUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.listUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listUnlinkedClientAccounts throws INVALID_ARGUMENT when parent is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.listUnlinkedClientAccounts(listUnlinkedClientAccountsRequest { parent = "x" })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listUnlinkedClientAccounts uses default page size when unspecified`() {
    val request = listUnlinkedClientAccountsRequest { parent = DATA_PROVIDER_NAME }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listUnlinkedClientAccounts(request) }
    }

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::listUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalListUnlinkedClientAccountsRequest {
          filter =
            ListUnlinkedClientAccountsRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            }
          pageSize = 50
        }
      )
  }

  @Test
  fun `listUnlinkedClientAccounts coerces page size above max`() {
    val request = listUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 5000
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listUnlinkedClientAccounts(request) }
    }

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::listUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalListUnlinkedClientAccountsRequest {
          filter =
            ListUnlinkedClientAccountsRequestKt.filter {
              externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            }
          pageSize = 1000
        }
      )
  }

  @Test
  fun `listUnlinkedClientAccounts throws INVALID_ARGUMENT when page size negative`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.listUnlinkedClientAccounts(
              listUnlinkedClientAccountsRequest {
                parent = DATA_PROVIDER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  // deleteUnlinkedClientAccount tests

  @Test
  fun `deleteUnlinkedClientAccount returns empty and translates request`() {
    val request = deleteUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val result: Empty =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.deleteUnlinkedClientAccount(request) }
      }

    assertThat(result).isEqualTo(Empty.getDefaultInstance())

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::deleteUnlinkedClientAccount,
      )
      .isEqualTo(
        internalDeleteUnlinkedClientAccountRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          clientAccountReferenceId = REFERENCE_ID
        }
      )
  }

  @Test
  fun `deleteUnlinkedClientAccount throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.deleteUnlinkedClientAccount(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteUnlinkedClientAccount throws PERMISSION_DENIED for wrong principal type`() {
    val request = deleteUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteUnlinkedClientAccount throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteUnlinkedClientAccount(deleteUnlinkedClientAccountRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteUnlinkedClientAccount throws PERMISSION_DENIED when not found`() {
    internalServiceMock.stub {
      onBlocking { deleteUnlinkedClientAccount(any()) }
        .thenThrow(
          UnlinkedClientAccountNotFoundException(
              ExternalId(EXTERNAL_DATA_PROVIDER_ID),
              REFERENCE_ID,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "UnlinkedClientAccount not found.")
        )
    }

    val request = deleteUnlinkedClientAccountRequest { name = UNLINKED_CLIENT_ACCOUNT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteUnlinkedClientAccount(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  // batchDeleteUnlinkedClientAccounts tests

  @Test
  fun `batchDeleteUnlinkedClientAccounts returns empty and translates request`() {
    val request = batchDeleteUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      names += UNLINKED_CLIENT_ACCOUNT_NAME
    }

    val result: Empty =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchDeleteUnlinkedClientAccounts(request) }
      }

    assertThat(result).isEqualTo(Empty.getDefaultInstance())

    verifyProtoArgument(
        internalServiceMock,
        InternalUnlinkedClientAccountsCoroutineImplBase::batchDeleteUnlinkedClientAccounts,
      )
      .isEqualTo(
        internalBatchDeleteUnlinkedClientAccountsRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          requests += internalDeleteUnlinkedClientAccountRequest {
            externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
            clientAccountReferenceId = REFERENCE_ID
          }
        }
      )
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts throws PERMISSION_DENIED for wrong principal type`() {
    val request = batchDeleteUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      names += UNLINKED_CLIENT_ACCOUNT_NAME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.batchDeleteUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts throws INVALID_ARGUMENT when name duplicated`() {
    val request = batchDeleteUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      names += UNLINKED_CLIENT_ACCOUNT_NAME
      names += UNLINKED_CLIENT_ACCOUNT_NAME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("duplicate")
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts throws INVALID_ARGUMENT when name does not match parent`() {
    val otherName =
      UnlinkedClientAccountKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID_2), REFERENCE_ID)
        .toName()
    val request = batchDeleteUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      names += otherName
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("does not match parent")
  }

  @Test
  fun `batchDeleteUnlinkedClientAccounts throws INVALID_ARGUMENT when batch size exceeded`() {
    val request = batchDeleteUnlinkedClientAccountsRequest {
      parent = DATA_PROVIDER_NAME
      for (i in 0 until 1001) {
        names +=
          UnlinkedClientAccountKey(externalIdToApiId(EXTERNAL_DATA_PROVIDER_ID), "ref-$i").toName()
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchDeleteUnlinkedClientAccounts(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
