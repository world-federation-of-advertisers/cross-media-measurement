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
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.ImpressionMetadataResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.ModelLineBoundResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.batchCreateImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readModelLinesBounds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateImpressionMetadataState
import org.wfanet.measurement.edpaggregator.service.internal.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsRequest
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.CreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.GetImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.computeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataResponse

class SpannerImpressionMetadataService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ImpressionMetadataServiceCoroutineImplBase(coroutineContext) {

  override suspend fun getImpressionMetadata(
    request: GetImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.impressionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getImpressionMetadataByResourceId(
            request.dataProviderResourceId,
            request.impressionMetadataResourceId,
          )
          .impressionMetadata
      }
    } catch (e: ImpressionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun createImpressionMetadata(
    request: CreateImpressionMetadataRequest
  ): ImpressionMetadata {
    try {
      validateImpressionMetadataRequest(request, "")
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createImpressionMetadata"))

    val result =
      try {
          transactionRunner.run { txn -> txn.batchCreateImpressionMetadata(listOf(request)) }
        } catch (e: SpannerException) {
          throw e
        }
        .single()

    if (result.hasCreateTime()) {
      return result
    }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return result.copy {
      createTime = commitTimestamp
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
    }
  }

  override suspend fun batchCreateImpressionMetadata(
    request: BatchCreateImpressionMetadataRequest
  ): BatchCreateImpressionMetadataResponse {
    if (request.requestsList.isEmpty()) {
      return BatchCreateImpressionMetadataResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val blobUriSet = mutableSetOf<String>()
    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      if (
        dataProviderResourceId.isNotEmpty() &&
          subRequest.impressionMetadata.dataProviderResourceId != dataProviderResourceId
      ) {
        val childDataProviderResourceId = subRequest.impressionMetadata.dataProviderResourceId
        throw DataProviderMismatchException(dataProviderResourceId, childDataProviderResourceId)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!blobUriSet.add(subRequest.impressionMetadata.blobUri)) {
        val blobUri = subRequest.impressionMetadata.blobUri
        throw InvalidFieldValueException("requests.$index.impression_metadata.blob_uri") {
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
        validateImpressionMetadataRequest(subRequest, "requests.$index.")
      } catch (e: RequiredFieldNotSetException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (e: InvalidFieldValueException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateImpressionMetadata"))

    val results =
      try {
        transactionRunner.run { txn -> txn.batchCreateImpressionMetadata(request.requestsList) }
      } catch (e: SpannerException) {
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateImpressionMetadataResponse {
      impressionMetadata +=
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

  override suspend fun listImpressionMetadata(
    request: ListImpressionMetadataRequest
  ): ListImpressionMetadataResponse {
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
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val impressionMetadataList: Flow<ImpressionMetadata> =
        txn
          .readImpressionMetadata(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            after,
          )
          .map { it.impressionMetadata }
      return listImpressionMetadataResponse {
        impressionMetadataList.collectIndexed { index, impressionMetadata ->
          if (index == pageSize) {
            nextPageToken = listImpressionMetadataPageToken {
              this.after =
                ListImpressionMetadataPageTokenKt.after {
                  impressionMetadataResourceId =
                    this@listImpressionMetadataResponse.impressionMetadata
                      .last()
                      .impressionMetadataResourceId
                }
            }
          } else {
            this.impressionMetadata += impressionMetadata
          }
        }
      }
    }
  }

  override suspend fun deleteImpressionMetadata(
    request: DeleteImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.impressionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=deleteImpressionMetadata"))

    val deletedImpressionMetadata =
      try {
        transactionRunner.run { txn ->
          val result =
            txn.getImpressionMetadataByResourceId(
              request.dataProviderResourceId,
              request.impressionMetadataResourceId,
            )

          if (result.impressionMetadata.state == State.IMPRESSION_METADATA_STATE_DELETED) {
            throw ImpressionMetadataNotFoundException(
              request.dataProviderResourceId,
              request.impressionMetadataResourceId,
            )
          }

          txn.updateImpressionMetadataState(
            result.impressionMetadata.dataProviderResourceId,
            result.impressionMetadataId,
            State.IMPRESSION_METADATA_STATE_DELETED,
          )

          result.impressionMetadata.copy {
            state = State.IMPRESSION_METADATA_STATE_DELETED
            clearUpdateTime()
            clearEtag()
          }
        }
      } catch (e: ImpressionMetadataNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return deletedImpressionMetadata.copy {
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
    }
  }

  override suspend fun batchDeleteImpressionMetadata(
    request: BatchDeleteImpressionMetadataRequest
  ): BatchDeleteImpressionMetadataResponse {
    if (request.requestsList.isEmpty()) {
      return BatchDeleteImpressionMetadataResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.requestsList.first().dataProviderResourceId
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requests.0.data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val impressionMetadataResourceIdSet = mutableSetOf<String>()

    request.requestsList.forEachIndexed { index, it ->
      if (it.dataProviderResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.data_provider_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (it.dataProviderResourceId != dataProviderResourceId) {
        throw InvalidFieldValueException("requests.$index.data_provider_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (it.impressionMetadataResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.impression_metadata_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!impressionMetadataResourceIdSet.add(it.impressionMetadataResourceId)) {
        throw InvalidFieldValueException("requests.$index.impression_metadata_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchDeleteImpressionMetadata"))

    val deletedList: List<ImpressionMetadata> = buildList {
      transactionRunner.run { txn ->
        val existingImpressionMetadataByResourceId =
          txn.getImpressionMetadataByResourceIds(
            dataProviderResourceId,
            request.requestsList.map { it.impressionMetadataResourceId },
          )

        request.requestsList.forEach {
          val result: ImpressionMetadataResult? =
            existingImpressionMetadataByResourceId[it.impressionMetadataResourceId]

          if (
            result == null ||
              result.impressionMetadata.state == State.IMPRESSION_METADATA_STATE_DELETED
          ) {
            throw ImpressionMetadataNotFoundException(
                dataProviderResourceId,
                it.impressionMetadataResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          }

          txn.updateImpressionMetadataState(
            dataProviderResourceId,
            existingImpressionMetadataByResourceId[it.impressionMetadataResourceId]!!
              .impressionMetadataId,
            State.IMPRESSION_METADATA_STATE_DELETED,
          )
          add(
            existingImpressionMetadataByResourceId[it.impressionMetadataResourceId]!!
              .impressionMetadata
          )
        }
      }
    }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return batchDeleteImpressionMetadataResponse {
      impressionMetadata +=
        deletedList.map {
          it.copy {
            state = State.IMPRESSION_METADATA_STATE_DELETED
            updateTime = commitTimestamp
            etag = ETags.computeETag(commitTimestamp.toInstant())
          }
        }
    }
  }

  override suspend fun computeModelLineBounds(
    request: ComputeModelLineBoundsRequest
  ): ComputeModelLineBoundsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.cmmsModelLineList.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val results: List<ModelLineBoundResult> =
      databaseClient
        .singleUse()
        .readModelLinesBounds(request.dataProviderResourceId, request.cmmsModelLineList)

    return computeModelLineBoundsResponse {
      modelLineBounds.putAll(results.associate { it.cmmsModelLine to it.bound })
    }
  }

  /**
   * Checks whether the specified create impression metadata request is valid.
   *
   * @throws RequiredFieldNotSetException
   */
  private fun validateImpressionMetadataRequest(
    request: CreateImpressionMetadataRequest,
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

    if (!request.hasImpressionMetadata()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata")
    }

    if (request.impressionMetadata.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException(
        "${fieldPathPrefix}impression_metadata.data_provider_resource_id"
      )
    }

    if (request.impressionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.blob_uri")
    }

    if (request.impressionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.blob_type_url")
    }

    if (request.impressionMetadata.eventGroupReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException(
        "${fieldPathPrefix}impression_metadata.event_group_reference_id"
      )
    }

    if (request.impressionMetadata.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.cmms_model_line")
    }

    if (!request.impressionMetadata.hasInterval()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.interval")
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
