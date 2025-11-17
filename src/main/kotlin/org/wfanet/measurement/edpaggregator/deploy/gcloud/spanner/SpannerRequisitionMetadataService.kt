/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RequisitionMetadataResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.batchCreateRequisitionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.fetchLatestCmmsCreateTime
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCmmsRequisition
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRequisitionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRequisitionMetadataAction
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRequisitionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.requisitionMetadataExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRequisitionMetadataState
import org.wfanet.measurement.edpaggregator.service.internal.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataNotFoundByCmmsRequisitionException
import org.wfanet.measurement.edpaggregator.service.internal.RequisitionMetadataNotFoundException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.CreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.FulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.GetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.LookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.MarkWithdrawnRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.QueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.batchCreateRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataResponse

class SpannerRequisitionMetadataService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RequisitionMetadataServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    try {
      validateRequisitionMetadataRequest(request, "")
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val initialState =
      if (request.requisitionMetadata.refusalMessage.isNotEmpty()) {
        State.REQUISITION_METADATA_STATE_REFUSED
      } else {
        State.REQUISITION_METADATA_STATE_STORED
      }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRequisitionMetadata"))
    val requisitionMetadata: RequisitionMetadata =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            try {
              UUID.fromString(request.requestId)
            } catch (e: IllegalArgumentException) {
              throw InvalidFieldValueException("request_id", e)
                .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
            }

            val existing: RequisitionMetadataResult? =
              txn.getRequisitionMetadataByCreateRequestId(
                request.requisitionMetadata.dataProviderResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.requisitionMetadata
            }
          }

          val requisitionMetadataId =
            idGenerator.generateNewId { id ->
              txn.requisitionMetadataExists(request.requisitionMetadata.dataProviderResourceId, id)
            }
          val requisitionMetadataResourceId =
            request.requisitionMetadata.requisitionMetadataResourceId.ifBlank {
              "$REQUISITION_METADATA_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
            }
          val actionId = idGenerator.generateId()

          txn.insertRequisitionMetadata(
            requisitionMetadataId,
            requisitionMetadataResourceId,
            initialState,
            request.requisitionMetadata,
            request.requestId,
          )

          txn.insertRequisitionMetadataAction(
            request.requisitionMetadata.dataProviderResourceId,
            requisitionMetadataId,
            actionId,
            State.REQUISITION_METADATA_STATE_UNSPECIFIED,
            initialState,
          )

          request.requisitionMetadata.copy {
            state = initialState
            this.requisitionMetadataResourceId = requisitionMetadataResourceId
            clearCreateTime()
            clearUpdateTime()
            clearEtag()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RequisitionMetadataAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    return if (requisitionMetadata.hasCreateTime()) {
      requisitionMetadata
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      requisitionMetadata.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    }
  }

  override suspend fun batchCreateRequisitionMetadata(
    request: BatchCreateRequisitionMetadataRequest
  ): BatchCreateRequisitionMetadataResponse {
    if (request.requestsList.isEmpty()) {
      return BatchCreateRequisitionMetadataResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val requestIdSet = mutableSetOf<String>()
    val cmmsRequisitionSet = mutableSetOf<String>()
    val blobUriSet = mutableSetOf<String>()

    request.requestsList.forEachIndexed { index, subRequest ->
      if (
        dataProviderResourceId.isNotEmpty() &&
          subRequest.requisitionMetadata.dataProviderResourceId != dataProviderResourceId
      ) {
        val childDataProviderResourceId = subRequest.requisitionMetadata.dataProviderResourceId
        throw DataProviderMismatchException(dataProviderResourceId, childDataProviderResourceId)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!cmmsRequisitionSet.add(subRequest.requisitionMetadata.cmmsRequisition)) {
        val cmmsRequisition = subRequest.requisitionMetadata.cmmsRequisition
        throw InvalidFieldValueException("requests.$index.requisition_metadata.cmms_requisition") {
            "cmms requisition $cmmsRequisition is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!blobUriSet.add(subRequest.requisitionMetadata.blobUri)) {
        val blobUri = subRequest.requisitionMetadata.blobUri
        throw InvalidFieldValueException("requests.$index.requisition_metadata.blob_uri") {
            "blob uri $blobUri is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isNotEmpty()) {
        if (!requestIdSet.add(subRequest.requestId)) {
          throw InvalidFieldValueException("requests.$index.request_id") {
              "request id $requestId is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

      try {
        validateRequisitionMetadataRequest(subRequest, "requests.$index.")
      } catch (e: RequiredFieldNotSetException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateRequisitionMetadata"))

    val results =
      try {
        transactionRunner.run { txn -> txn.batchCreateRequisitionMetadata(request.requestsList) }
      } catch (e: SpannerException) {
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRequisitionMetadataResponse {
      requisitionMetadata +=
        results.map { result ->
          if (result.hasCreateTime()) {
            result
          } else {
            result.copy {
              createTime = commitTimestamp
              updateTime = commitTimestamp
              etag = ETags.computeETag(commitTimestamp.toInstant())
            }
          }
        }
    }
  }

  /**
   * Checks whether the specified request is valid.
   *
   * @throws RequiredFieldNotSetException
   * @throws InvalidFieldValueException
   */
  private fun validateRequisitionMetadataRequest(
    request: CreateRequisitionMetadataRequest,
    fieldPathPrefix: String,
  ) {
    val requestId = request.requestId
    if (requestId.isNotEmpty()) {
      try {
        UUID.fromString(requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("${fieldPathPrefix}request_id", e)
      }
    }

    if (!request.hasRequisitionMetadata()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}requisition_metadata")
    }

    val requisitionMetadata = request.requisitionMetadata
    if (requisitionMetadata.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException(
        "${fieldPathPrefix}requisition_metadata.data_provider_resource_id"
      )
    }

    if (requisitionMetadata.cmmsRequisition.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}requisition_metadata.cmms_requisition")
    }

    if (requisitionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}blob_uri")
    }

    if (requisitionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}blob_type_url")
    }

    if (requisitionMetadata.groupId.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}group_id")
    }

    if (requisitionMetadata.report.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}report")
    }
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requisitionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requisition_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getRequisitionMetadataByResourceId(
            request.dataProviderResourceId,
            request.requisitionMetadataResourceId,
          )
          .requisitionMetadata
      }
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (request.lookupKeyCase) {
          LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
            txn.getRequisitionMetadataByCmmsRequisition(
              request.dataProviderResourceId,
              request.cmmsRequisition,
            )
          LookupRequisitionMetadataRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
            throw RequiredFieldNotSetException("lookup_key")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }.requisitionMetadata
      }
    } catch (e: RequisitionMetadataNotFoundByCmmsRequisitionException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn.fetchLatestCmmsCreateTime(request.dataProviderResourceId)
    }
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.workItem.isEmpty()) {
      throw RequiredFieldNotSetException("work_item")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      val requisitionMetadata =
        transitionState(
          request.dataProviderResourceId,
          request.requisitionMetadataResourceId,
          request.etag,
          State.REQUISITION_METADATA_STATE_QUEUED,
        ) {
          set("WorkItem").to(request.workItem)
        }
      requisitionMetadata.copy { workItem = request.workItem }
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    return try {
      transitionState(
        request.dataProviderResourceId,
        request.requisitionMetadataResourceId,
        request.etag,
        State.REQUISITION_METADATA_STATE_PROCESSING,
      )
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    return try {
      transitionState(
        request.dataProviderResourceId,
        request.requisitionMetadataResourceId,
        request.etag,
        State.REQUISITION_METADATA_STATE_FULFILLED,
      )
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  override suspend fun listRequisitionMetadata(
    request: ListRequisitionMetadataRequest
  ): ListRequisitionMetadataResponse {

    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val rows: Flow<RequisitionMetadata> =
        txn
          .readRequisitionMetadata(
            request.dataProviderResourceId,
            filter = request.filter,
            limit = pageSize + 1,
            after = after,
          )
          .map { it.requisitionMetadata }

      return listRequisitionMetadataResponse {
        rows.collectIndexed { index, item ->
          if (index == pageSize) {
            val lastIncluded = this.requisitionMetadata.last()
            nextPageToken = listRequisitionMetadataPageToken {
              this.after =
                ListRequisitionMetadataPageTokenKt.after {
                  updateTime = lastIncluded.updateTime
                  requisitionMetadataResourceId = lastIncluded.requisitionMetadataResourceId
                }
            }
          } else {
            this.requisitionMetadata += item
          }
        }
      }
    }
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    if (request.refusalMessage.isEmpty()) {
      throw RequiredFieldNotSetException("refusal_message")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      val requisitionMetadata =
        transitionState(
          request.dataProviderResourceId,
          request.requisitionMetadataResourceId,
          request.etag,
          State.REQUISITION_METADATA_STATE_REFUSED,
        ) {
          set("RefusalMessage").to(request.refusalMessage)
        }
      requisitionMetadata.copy { refusalMessage = request.refusalMessage }
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  override suspend fun markWithdrawnRequisitionMetadata(
    request: MarkWithdrawnRequisitionMetadataRequest
  ): RequisitionMetadata {

    return try {
      transitionState(
        request.dataProviderResourceId,
        request.requisitionMetadataResourceId,
        request.etag,
        State.REQUISITION_METADATA_STATE_WITHDRAWN,
      )
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RequisitionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  /**
   * Returns the copy of original RequisitionMetadata with updated state and updateTime.
   *
   * @throws RequiredFieldNotSetException
   * @throws RequisitionMetadataNotFoundException
   * @throws EtagMismatchException
   */
  private suspend fun transitionState(
    dataProviderResourceId: String,
    requisitionMetadataResourceId: String,
    requestEtag: String,
    nextState: State,
    block: (Mutation.WriteBuilder.() -> Unit)? = null,
  ): RequisitionMetadata {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (requisitionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requisition_metadata_resource_id")
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=queueRequisitionMetadata"))
    val updatedRequisitionMetadata =
      transactionRunner.run { txn ->
        val result =
          txn.getRequisitionMetadataByResourceId(
            dataProviderResourceId,
            requisitionMetadataResourceId,
          )
        val requisitionMetadata = result.requisitionMetadata

        if (requestEtag != requisitionMetadata.etag) {
          throw EtagMismatchException(requestEtag, requisitionMetadata.etag)
        }
        txn.updateRequisitionMetadataState(
          dataProviderResourceId,
          result.requisitionMetadataId,
          nextState,
          block,
        )
        txn.insertRequisitionMetadataAction(
          dataProviderResourceId,
          result.requisitionMetadataId,
          idGenerator.generateId(),
          requisitionMetadata.state,
          nextState,
        )
        requisitionMetadata
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return updatedRequisitionMetadata.copy {
      state = nextState
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
    }
  }

  companion object {
    private const val REQUISITION_METADATA_RESOURCE_ID_PREFIX = "req"
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
