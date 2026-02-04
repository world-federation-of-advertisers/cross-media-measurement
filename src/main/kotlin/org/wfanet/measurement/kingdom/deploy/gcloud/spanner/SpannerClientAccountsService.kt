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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchCreateClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.BatchDeleteClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.BatchDeleteClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.CreateClientAccountRequest
import org.wfanet.measurement.internal.kingdom.DeleteClientAccountRequest
import org.wfanet.measurement.internal.kingdom.GetClientAccountRequest
import org.wfanet.measurement.internal.kingdom.ListClientAccountsPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.ListClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.listClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.listClientAccountsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchCreateClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchDeleteClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteClientAccountByMeasurementConsumer

class SpannerClientAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ClientAccountsCoroutineImplBase(coroutineContext) {
  override suspend fun createClientAccount(request: CreateClientAccountRequest): ClientAccount {
    if (!request.hasClientAccount()) {
      throw RequiredFieldNotSetException("client_account")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.clientAccount.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("client_account.external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.clientAccount.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("client_account.external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.clientAccount.clientAccountReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException("client_account.client_account_reference_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return handleCreateExceptions {
      CreateClientAccount(request.clientAccount).execute(client, idGenerator)
    }
  }

  override suspend fun batchCreateClientAccounts(
    request: BatchCreateClientAccountsRequest
  ): BatchCreateClientAccountsResponse {
    if (request.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for ((index, subRequest) in request.requestsList.withIndex()) {
      if (!subRequest.hasClientAccount()) {
        throw RequiredFieldNotSetException("requests.$index.client_account")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      val clientExternalMcId = subRequest.clientAccount.externalMeasurementConsumerId
      if (clientExternalMcId != 0L && clientExternalMcId != request.externalMeasurementConsumerId) {
        throw InvalidFieldValueException(
            "requests.$index.client_account.external_measurement_consumer_id"
          ) { fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.clientAccount.externalDataProviderId == 0L) {
        throw RequiredFieldNotSetException(
            "requests.$index.client_account.external_data_provider_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.clientAccount.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests.$index.client_account.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleCreateExceptions {
      BatchCreateClientAccounts(request).execute(client, idGenerator)
    }
  }

  override suspend fun batchCreateClientAccounts(
    request: BatchCreateClientAccountsRequest
  ): BatchCreateClientAccountsResponse {
    if (request.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for ((index, subRequest) in request.requestsList.withIndex()) {
      if (!subRequest.hasClientAccount()) {
        throw RequiredFieldNotSetException("requests.$index.client_account")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      val clientExternalMcId = subRequest.clientAccount.externalMeasurementConsumerId
      if (clientExternalMcId != 0L && clientExternalMcId != request.externalMeasurementConsumerId) {
        throw InvalidFieldValueException("requests.$index.client_account.external_measurement_consumer_id") {
            fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.clientAccount.externalDataProviderId == 0L) {
        throw RequiredFieldNotSetException(
            "requests.$index.client_account.external_data_provider_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.clientAccount.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests.$index.client_account.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleCreateExceptions {
      BatchCreateClientAccounts(request).execute(client, idGenerator)
    }
  }

  override suspend fun getClientAccount(request: GetClientAccountRequest): ClientAccount {
    if (request.externalClientAccountId == 0L) {
      throw RequiredFieldNotSetException("external_client_account_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val externalClientAccountId = ExternalId(request.externalClientAccountId)
    val reader = ClientAccountReader()

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (request.externalParentIdCase) {
      GetClientAccountRequest.ExternalParentIdCase.EXTERNAL_MEASUREMENT_CONSUMER_ID -> {
        val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
        reader.readByMeasurementConsumer(
          client.singleUse(),
          externalMeasurementConsumerId,
          externalClientAccountId,
        )
          ?: throw ClientAccountNotFoundException(
              externalMeasurementConsumerId,
              externalClientAccountId,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      GetClientAccountRequest.ExternalParentIdCase.EXTERNAL_DATA_PROVIDER_ID -> {
        val externalDataProviderId = ExternalId(request.externalDataProviderId)
        reader.readByDataProvider(
          client.singleUse(),
          externalDataProviderId,
          externalClientAccountId,
        )
          ?: throw ClientAccountNotFoundByDataProviderException(
              externalDataProviderId,
              externalClientAccountId,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
      GetClientAccountRequest.ExternalParentIdCase.EXTERNALPARENTID_NOT_SET ->
        throw Status.INVALID_ARGUMENT.withDescription("external_parent_id not specified")
          .asRuntimeException()
    }.clientAccount
  }

  override suspend fun deleteClientAccount(request: DeleteClientAccountRequest): ClientAccount {
    if (request.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalClientAccountId == 0L) {
      throw RequiredFieldNotSetException("external_client_account_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    val externalClientAccountId = ExternalId(request.externalClientAccountId)

    return handleDeleteExceptions {
      DeleteClientAccountByMeasurementConsumer(
          externalMeasurementConsumerId,
          externalClientAccountId,
        )
        .execute(client, idGenerator)
    }
  }

  override suspend fun batchDeleteClientAccounts(
    request: BatchDeleteClientAccountsRequest
  ): BatchDeleteClientAccountsResponse {
    if (request.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for ((index, subRequest) in request.requestsList.withIndex()) {
      val clientExternalMcId = subRequest.externalMeasurementConsumerId
      if (clientExternalMcId != 0L && clientExternalMcId != request.externalMeasurementConsumerId) {
        throw InvalidFieldValueException("requests.$index.external_measurement_consumer_id") {
            fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.externalClientAccountId == 0L) {
        throw RequiredFieldNotSetException("requests.$index.external_client_account_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleDeleteExceptions {
      BatchDeleteClientAccounts(request).execute(client, idGenerator)
    }
  }

  override suspend fun batchDeleteClientAccounts(
    request: BatchDeleteClientAccountsRequest
  ): BatchDeleteClientAccountsResponse {
    if (request.externalMeasurementConsumerId == 0L) {
      throw RequiredFieldNotSetException("external_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for ((index, subRequest) in request.requestsList.withIndex()) {
      val clientExternalMcId = subRequest.externalMeasurementConsumerId
      if (clientExternalMcId != 0L && clientExternalMcId != request.externalMeasurementConsumerId) {
        throw InvalidFieldValueException("requests.$index.external_measurement_consumer_id") {
            fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.externalClientAccountId == 0L) {
        throw RequiredFieldNotSetException("requests.$index.external_client_account_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleDeleteExceptions { BatchDeleteClientAccounts(request).execute(client, idGenerator) }
  }

  override suspend fun listClientAccounts(
    request: ListClientAccountsRequest
  ): ListClientAccountsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { "Page size cannot be less than 0" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after = if (request.hasPageToken()) request.pageToken.after else null

    val clientAccountList =
      StreamClientAccounts(request.filter, pageSize + 1, after).execute(client.singleUse()).toList()

    if (clientAccountList.isEmpty()) {
      return ListClientAccountsResponse.getDefaultInstance()
    }

    return listClientAccountsResponse {
      clientAccounts.addAll(clientAccountList.take(pageSize).map { it.clientAccount })

      if (clientAccountList.size > pageSize) {
        val lastAccount = clientAccounts.last()
        nextPageToken = listClientAccountsPageToken {
          this.after =
            ListClientAccountsPageTokenKt.after {
              externalMeasurementConsumerId = lastAccount.externalMeasurementConsumerId
              externalClientAccountId = lastAccount.externalClientAccountId
              createTime = lastAccount.createTime
            }
        }
      }
    }
  }

  private suspend fun <T> handleCreateExceptions(block: suspend () -> T): T {
    try {
      return block()
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ClientAccountAlreadyExistsException) {
      throw e.asStatusRuntimeException(
        Status.Code.ALREADY_EXISTS,
        "ClientAccount with this reference ID already exists for DataProvider.",
      )
    }
  }

  private suspend fun <T> handleDeleteExceptions(block: suspend () -> T): T {
    try {
      return block()
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ClientAccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 1000
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
