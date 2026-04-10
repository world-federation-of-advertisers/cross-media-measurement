// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Options
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionMetadataBatchResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findExistingBatchByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionMetadataBatchByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionMetadataBatchExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionMetadataBatches
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.softDeleteRawImpressionMetadataBatch
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionMetadataBatchState
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionMetadataBatchRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionMetadataBatchFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionMetadataBatchProcessedRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionBatchState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatch
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatch

class SpannerRawImpressionMetadataBatchService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RawImpressionMetadataBatchServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionMetadataBatch(
    request: CreateRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val requestId: String = request.requestId
    if (requestId.isNotEmpty()) {
      try {
        UUID.fromString(requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRawImpressionMetadataBatch"))

    val result: RawImpressionMetadataBatch =
      transactionRunner.run { txn ->
        val existing: RawImpressionMetadataBatchResult? =
          txn.findExistingBatchByRequestId(request.dataProviderResourceId, requestId)
        if (existing != null) {
          return@run existing.rawImpressionMetadataBatch
        }

        val batchId: Long =
          idGenerator.generateNewId { id ->
            txn.rawImpressionMetadataBatchExists(request.dataProviderResourceId, id)
          }

        val resolvedBatchResourceId: String =
          "batch-${UUID.randomUUID()}"

        txn.insertRawImpressionMetadataBatch(
          batchId,
          request.dataProviderResourceId,
          resolvedBatchResourceId,
          requestId,
        )

        rawImpressionMetadataBatch {
          dataProviderResourceId = request.dataProviderResourceId
          batchResourceId = resolvedBatchResourceId
          state = RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED
        }
      }

    if (result.hasCreateTime()) {
      return result
    }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return result.copy {
      createTime = commitTimestamp
      updateTime = commitTimestamp
    }
  }

  override suspend fun getRawImpressionMetadataBatch(
    request: GetRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getRawImpressionMetadataBatchByResourceId(
            request.dataProviderResourceId,
            request.batchResourceId,
          )
          .rawImpressionMetadataBatch
      }
    } catch (e: RawImpressionMetadataBatchNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRawImpressionMetadataBatches(
    request: ListRawImpressionMetadataBatchesRequest
  ): ListRawImpressionMetadataBatchesResponse {
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

    val pageSize: Int =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after: ListRawImpressionMetadataBatchesPageToken.After? =
      if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val batchFlow: Flow<RawImpressionMetadataBatch> =
        txn
          .readRawImpressionMetadataBatches(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            request.showDeleted,
            after,
          )
          .map { it.rawImpressionMetadataBatch }
      return listRawImpressionMetadataBatchesResponse {
        batchFlow.collectIndexed { index, batch ->
          if (index == pageSize) {
            nextPageToken = listRawImpressionMetadataBatchesPageToken {
              this.after =
                ListRawImpressionMetadataBatchesPageTokenKt.after {
                  createTime =
                    this@listRawImpressionMetadataBatchesResponse.rawImpressionMetadataBatches
                      .last()
                      .createTime
                  batchResourceId =
                    this@listRawImpressionMetadataBatchesResponse.rawImpressionMetadataBatches
                      .last()
                      .batchResourceId
                }
            }
          } else {
            rawImpressionMetadataBatches += batch
          }
        }
      }
    }
  }

  override suspend fun deleteRawImpressionMetadataBatch(
    request: DeleteRawImpressionMetadataBatchRequest
  ): RawImpressionMetadataBatch {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=deleteRawImpressionMetadataBatch"))

    val deletedBatch: RawImpressionMetadataBatch =
      try {
        transactionRunner.run { txn ->
          val result: RawImpressionMetadataBatchResult =
            txn.getRawImpressionMetadataBatchByResourceId(
              request.dataProviderResourceId,
              request.batchResourceId,
            )

          if (result.rawImpressionMetadataBatch.hasDeleteTime()) {
            throw RawImpressionMetadataBatchNotFoundException(
              request.dataProviderResourceId,
              request.batchResourceId,
            )
          }

          txn.softDeleteRawImpressionMetadataBatch(request.dataProviderResourceId, result.batchId)

          result.rawImpressionMetadataBatch.copy {
            clearUpdateTime()
            clearDeleteTime()
          }
        }
      } catch (e: RawImpressionMetadataBatchNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return deletedBatch.copy {
      updateTime = commitTimestamp
      deleteTime = commitTimestamp
    }
  }

  override suspend fun markRawImpressionMetadataBatchProcessed(
    request: MarkRawImpressionMetadataBatchProcessedRequest
  ): RawImpressionMetadataBatch {
    try {
      return transitionBatchState(
        request.dataProviderResourceId,
        request.batchResourceId,
        RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_PROCESSED,
        setOf(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED),
        "markRawImpressionMetadataBatchProcessed",
      )
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RawImpressionMetadataBatchNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: RawImpressionMetadataBatchStateInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun markRawImpressionMetadataBatchFailed(
    request: MarkRawImpressionMetadataBatchFailedRequest
  ): RawImpressionMetadataBatch {
    try {
      return transitionBatchState(
        request.dataProviderResourceId,
        request.batchResourceId,
        RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_FAILED,
        setOf(RawImpressionBatchState.RAW_IMPRESSION_BATCH_STATE_CREATED),
        "markRawImpressionMetadataBatchFailed",
      )
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: RawImpressionMetadataBatchNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: RawImpressionMetadataBatchStateInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  private suspend fun transitionBatchState(
    dataProviderResourceId: String,
    batchResourceId: String,
    targetState: RawImpressionBatchState,
    expectedStates: Set<RawImpressionBatchState>,
    action: String,
  ): RawImpressionMetadataBatch {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=$action"))

    val updatedBatch: RawImpressionMetadataBatch =
      transactionRunner.run { txn ->
        val result: RawImpressionMetadataBatchResult =
          txn.getRawImpressionMetadataBatchByResourceId(dataProviderResourceId, batchResourceId)

        if (result.rawImpressionMetadataBatch.state !in expectedStates) {
          throw RawImpressionMetadataBatchStateInvalidException(
            dataProviderResourceId,
            batchResourceId,
            result.rawImpressionMetadataBatch.state,
            expectedStates,
          )
        }

        txn.updateRawImpressionMetadataBatchState(
          dataProviderResourceId,
          result.batchId,
          targetState,
        )

        result.rawImpressionMetadataBatch.copy {
          state = targetState
          clearUpdateTime()
        }
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return updatedBatch.copy { updateTime = commitTimestamp }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
