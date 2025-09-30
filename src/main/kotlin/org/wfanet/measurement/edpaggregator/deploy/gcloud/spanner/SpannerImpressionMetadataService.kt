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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.ImpressionMetadataResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.ModelLineBoundResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.impressionMetadataExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readModelLinesBounds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateImpressionMetadataState
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
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
import org.wfanet.measurement.internal.edpaggregator.computeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataResponse

class SpannerImpressionMetadataService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
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
      validateImpressionMetadataRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val initialState = State.IMPRESSION_METADATA_STATE_ACTIVE

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createImpressionMetadata"))
    val impressionMetadata: ImpressionMetadata =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            try {
              UUID.fromString(request.requestId)
            } catch (e: IllegalArgumentException) {
              throw InvalidFieldValueException("request_id", e)
                .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
            }

            val existing: ImpressionMetadataResult? =
              txn.getImpressionMetadataByCreateRequestId(
                request.impressionMetadata.dataProviderResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.impressionMetadata
            }
          }

          val impressionMetadataId =
            idGenerator.generateNewId { id ->
              txn.impressionMetadataExists(request.impressionMetadata.dataProviderResourceId, id)
            }
          val impressionMetadataResourceId =
            request.impressionMetadata.impressionMetadataResourceId.ifEmpty {
              IMPRESSION_METADATA_PREFIX + UUID.randomUUID()
            }

          txn.insertImpressionMetadata(
            impressionMetadataId,
            impressionMetadataResourceId,
            initialState,
            request.impressionMetadata,
            request.requestId,
          )
          request.impressionMetadata.copy {
            state = initialState
            this.impressionMetadataResourceId = impressionMetadataResourceId
            clearCreateTime()
            clearUpdateTime()
            clearEtag()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw ImpressionMetadataAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    return if (impressionMetadata.hasCreateTime()) {
      impressionMetadata
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      impressionMetadata.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
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
            throw ImpressionMetadataStateInvalidException(
              request.dataProviderResourceId,
              request.impressionMetadataResourceId,
              result.impressionMetadata.state,
              setOf(State.IMPRESSION_METADATA_STATE_ACTIVE),
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
      } catch (e: ImpressionMetadataStateInvalidException) {
        throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return deletedImpressionMetadata.copy {
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
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
  private fun validateImpressionMetadataRequest(request: CreateImpressionMetadataRequest) {
    if (request.impressionMetadata.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata.data_provider_resource_id")
    }
    if (request.impressionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata.blob_uri")
    }
    if (request.impressionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata.blob_type_url")
    }
    if (request.impressionMetadata.eventGroupReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata.event_group_reference_id")
    }
    if (request.impressionMetadata.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata.cmms_model_line")
    }
    if (!request.impressionMetadata.hasInterval()) {
      throw RequiredFieldNotSetException("impression_metadata.interval")
    }
  }

  companion object {
    private const val IMPRESSION_METADATA_PREFIX = "imp-"
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
