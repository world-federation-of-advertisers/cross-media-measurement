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
import com.google.protobuf.Empty
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.*
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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.deleteImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getImpressionMetadataIdByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.impressionMetadataExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertImpressionMetadata
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readImpressionMetadata
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.CreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.GetImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse
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
      validateImpressionMetadata(request.impressionMetadata)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val initialState = State.IMPRESSION_METADATA_STATE_ACTIVE

    val transactionRunner =
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
              "imp-" + UUID.randomUUID()
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

  override suspend fun deleteImpressionMetadata(request: DeleteImpressionMetadataRequest): Empty {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.impressionMetadataResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_metadata_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      databaseClient.readWriteTransaction(Options.tag("action=deleteImpressionMetadata")).run { txn
        ->
        val impressionMetadataId =
          txn.getImpressionMetadataIdByResourceId(
            request.dataProviderResourceId,
            request.impressionMetadataResourceId,
          )
        txn.deleteImpressionMetadata(request.dataProviderResourceId, impressionMetadataId)
      }
    } catch (e: ImpressionMetadataNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun listImpressionMetadata(
    request: ListImpressionMetadataRequest
  ): ListImpressionMetadataResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
        "$fieldName must be non-negative"
      }
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
        txn.readImpressionMetadata(pageSize + 1, after).map { it.impressionMetadata }
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

  private fun validateImpressionMetadata(impressionMetadata: ImpressionMetadata) {
    if (impressionMetadata.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (impressionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("blob_uri")
    }
    if (impressionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("blob_type_url")
    }
    if (impressionMetadata.eventGroupReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException("event_group_reference_id")
    }
    if (impressionMetadata.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_model_line")
    }
    if (!impressionMetadata.hasInterval()) {
      throw RequiredFieldNotSetException("interval")
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
