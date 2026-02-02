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
 import org.wfanet.measurement.common.grpc.grpcRequire
 import org.wfanet.measurement.common.identity.ExternalId
 import org.wfanet.measurement.common.identity.IdGenerator
 import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
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
 import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
 import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamClientAccounts
 import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
 import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateClientAccount
 import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteClientAccountByMeasurementConsumer
 
 class SpannerClientAccountsService(
   private val idGenerator: IdGenerator,
   private val client: AsyncDatabaseClient,
   coroutineContext: CoroutineContext = EmptyCoroutineContext,
 ) : ClientAccountsCoroutineImplBase(coroutineContext) {
   override suspend fun createClientAccount(request: CreateClientAccountRequest): ClientAccount {
     grpcRequire(request.hasClientAccount()) { "client_account not specified" }
     grpcRequire(request.clientAccount.externalMeasurementConsumerId != 0L) {
       "external_measurement_consumer_id not specified"
     }
     grpcRequire(request.clientAccount.externalDataProviderId != 0L) {
       "external_data_provider_id not specified"
     }
     grpcRequire(request.clientAccount.clientAccountReferenceId.isNotEmpty()) {
       "client_account_reference_id not specified"
     }
 
     try {
       return CreateClientAccount(request.clientAccount).execute(client, idGenerator)
     } catch (e: MeasurementConsumerNotFoundException) {
       throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
     } catch (e: DataProviderNotFoundException) {
       throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "DataProvider not found.")
     } catch (e: ClientAccountAlreadyExistsException) {
       throw e.asStatusRuntimeException(
         Status.Code.ALREADY_EXISTS,
         "ClientAccount with this reference ID already exists for DataProvider.",
       )
     }
   }
 
   override suspend fun getClientAccount(request: GetClientAccountRequest): ClientAccount {
     grpcRequire(request.externalClientAccountId != 0L) {
       "external_client_account_id not specified"
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
             .asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
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
             .asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
       }
       GetClientAccountRequest.ExternalParentIdCase.EXTERNALPARENTID_NOT_SET ->
         throw Status.INVALID_ARGUMENT.withDescription("external_parent_id not specified")
           .asRuntimeException()
     }.clientAccount
   }
 
   override suspend fun deleteClientAccount(request: DeleteClientAccountRequest): ClientAccount {
     grpcRequire(request.externalMeasurementConsumerId != 0L) {
       "external_measurement_consumer_id not specified"
     }
     grpcRequire(request.externalClientAccountId != 0L) {
       "external_client_account_id not specified"
     }
 
     val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
     val externalClientAccountId = ExternalId(request.externalClientAccountId)
 
     try {
       return DeleteClientAccountByMeasurementConsumer(
           externalMeasurementConsumerId,
           externalClientAccountId,
         )
         .execute(client, idGenerator)
     } catch (e: MeasurementConsumerNotFoundException) {
       throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found.")
     } catch (e: ClientAccountNotFoundException) {
       throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ClientAccount not found.")
     }
   }
 
   override suspend fun listClientAccounts(
     request: ListClientAccountsRequest
   ): ListClientAccountsResponse {
     grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }
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
       for ((index, result) in clientAccountList.withIndex()) {
         if (index == pageSize) {
           val lastAccount = clientAccounts.last()
           nextPageToken = listClientAccountsPageToken {
             this.after =
               ListClientAccountsPageTokenKt.after {
                 externalMeasurementConsumerId = lastAccount.externalMeasurementConsumerId
                 externalClientAccountId = lastAccount.externalClientAccountId
                 createTime = lastAccount.createTime
               }
           }
         } else {
           clientAccounts += result.clientAccount
         }
       }
     }
   }
 
   companion object {
     private const val MAX_PAGE_SIZE = 1000
     private const val DEFAULT_PAGE_SIZE = 50
   }
 }
