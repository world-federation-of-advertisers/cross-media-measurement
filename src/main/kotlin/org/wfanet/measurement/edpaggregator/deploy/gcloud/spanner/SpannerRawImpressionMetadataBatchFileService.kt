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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionMetadataBatchFileResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findExistingBatchFilesByBlobUris
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findExistingBatchFilesByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getBatchFilesByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionMetadataBatchByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionMetadataBatchFileByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionMetadataBatchFile
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionMetadataBatchFileExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionMetadataBatchFiles
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.softDeleteRawImpressionMetadataBatchFile
import org.wfanet.measurement.edpaggregator.service.internal.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchFileAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionMetadataBatchNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionMetadataBatchFileRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionMetadataBatchFileServiceGrpcKt.RawImpressionMetadataBatchFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionMetadataBatchFilesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionMetadataBatchFile

class SpannerRawImpressionMetadataBatchFileService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RawImpressionMetadataBatchFileServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionMetadataBatchFile(
    request: CreateRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!request.hasRawImpressionMetadataBatchFile()) {
      throw RequiredFieldNotSetException("raw_impression_metadata_batch_file")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionMetadataBatchFile.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_metadata_batch_file.blob_uri")
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
      databaseClient.readWriteTransaction(
        Options.tag("action=createRawImpressionMetadataBatchFile")
      )
    val result: RawImpressionMetadataBatchFile =
      try {
        transactionRunner.run { txn ->
          val batchResult =
            txn.getRawImpressionMetadataBatchByResourceId(
              request.dataProviderResourceId,
              request.batchResourceId,
            )
          val existingByRequestId: Map<String, RawImpressionMetadataBatchFileResult> =
            txn.findExistingBatchFilesByRequestIds(
              request.dataProviderResourceId,
              batchResult.batchId,
              listOf(requestId),
            )
          if (existingByRequestId.containsKey(requestId)) {
            return@run existingByRequestId.getValue(requestId).rawImpressionMetadataBatchFile
          }
          val existingByBlobUri: Map<String, RawImpressionMetadataBatchFileResult> =
            txn.findExistingBatchFilesByBlobUris(
              request.dataProviderResourceId,
              listOf(request.rawImpressionMetadataBatchFile.blobUri),
            )
          if (existingByBlobUri.isNotEmpty()) {
            throw RawImpressionMetadataBatchFileAlreadyExistsException(
              request.rawImpressionMetadataBatchFile.blobUri
            )
          }
          val fileId: Long =
            idGenerator.generateNewId { id ->
              txn.rawImpressionMetadataBatchFileExists(
                request.dataProviderResourceId,
                batchResult.batchId,
                id,
              )
            }
          val resolvedFileResourceId: String = "file-${UUID.randomUUID()}"
          txn.insertRawImpressionMetadataBatchFile(
            fileId,
            request.dataProviderResourceId,
            batchResult.batchId,
            resolvedFileResourceId,
            request.rawImpressionMetadataBatchFile.blobUri,
            requestId,
          )
          rawImpressionMetadataBatchFile {
            dataProviderResourceId = request.dataProviderResourceId
            batchResourceId = request.batchResourceId
            fileResourceId = resolvedFileResourceId
            blobUri = request.rawImpressionMetadataBatchFile.blobUri
          }
        }
      } catch (e: RawImpressionMetadataBatchNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: RawImpressionMetadataBatchFileAlreadyExistsException) {
        throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
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

  override suspend fun batchCreateRawImpressionMetadataBatchFiles(
    request: BatchCreateRawImpressionMetadataBatchFilesRequest
  ): BatchCreateRawImpressionMetadataBatchFilesResponse {
    if (request.requestsList.isEmpty()) {
      return BatchCreateRawImpressionMetadataBatchFilesResponse.getDefaultInstance()
    }
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val blobUriSet = mutableSetOf<String>()
    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      if (
        subRequest.dataProviderResourceId.isNotEmpty() &&
          subRequest.dataProviderResourceId != request.dataProviderResourceId
      ) {
        throw DataProviderMismatchException(
            request.dataProviderResourceId,
            subRequest.dataProviderResourceId,
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!subRequest.hasRawImpressionMetadataBatchFile()) {
        throw RequiredFieldNotSetException("requests.$index.raw_impression_metadata_batch_file")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.rawImpressionMetadataBatchFile.blobUri.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests.$index.raw_impression_metadata_batch_file.blob_uri"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!blobUriSet.add(subRequest.rawImpressionMetadataBatchFile.blobUri)) {
        throw InvalidFieldValueException(
            "requests.$index.raw_impression_metadata_batch_file.blob_uri"
          ) {
            "blob uri is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val subRequestId: String = subRequest.requestId
      if (subRequestId.isNotEmpty()) {
        if (!requestIdSet.add(subRequestId)) {
          throw InvalidFieldValueException("requests.$index.request_id") {
              "request id $subRequestId is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        try {
          UUID.fromString(subRequestId)
        } catch (e: IllegalArgumentException) {
          throw InvalidFieldValueException("requests.$index.request_id", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=batchCreateRawImpressionMetadataBatchFiles")
      )
    val results: List<RawImpressionMetadataBatchFile> =
      try {
        transactionRunner.run { txn ->
          val batchResult =
            txn.getRawImpressionMetadataBatchByResourceId(
              request.dataProviderResourceId,
              request.batchResourceId,
            )
          val existingByRequestId: Map<String, RawImpressionMetadataBatchFileResult> =
            txn.findExistingBatchFilesByRequestIds(
              request.dataProviderResourceId,
              batchResult.batchId,
              request.requestsList.map { it.requestId },
            )
          val existingByBlobUri: Map<String, RawImpressionMetadataBatchFileResult> =
            txn.findExistingBatchFilesByBlobUris(
              request.dataProviderResourceId,
              request.requestsList.map { it.rawImpressionMetadataBatchFile.blobUri },
            )
          val resultsByIndex = mutableMapOf<Int, RawImpressionMetadataBatchFile>()
          request.requestsList.forEachIndexed { index, subRequest ->
            if (
              subRequest.requestId.isNotEmpty() &&
                existingByRequestId.containsKey(subRequest.requestId)
            ) {
              resultsByIndex[index] =
                existingByRequestId.getValue(subRequest.requestId).rawImpressionMetadataBatchFile
              return@forEachIndexed
            }
            val blobUri: String = subRequest.rawImpressionMetadataBatchFile.blobUri
            if (existingByBlobUri.containsKey(blobUri)) {
              throw RawImpressionMetadataBatchFileAlreadyExistsException(blobUri)
            }
            val fileId: Long =
              idGenerator.generateNewId { id ->
                txn.rawImpressionMetadataBatchFileExists(
                  request.dataProviderResourceId,
                  batchResult.batchId,
                  id,
                )
              }
            val resolvedFileResourceId: String = "file-${UUID.randomUUID()}"
            txn.insertRawImpressionMetadataBatchFile(
              fileId,
              request.dataProviderResourceId,
              batchResult.batchId,
              resolvedFileResourceId,
              blobUri,
              subRequest.requestId,
            )
            resultsByIndex[index] = rawImpressionMetadataBatchFile {
              dataProviderResourceId = request.dataProviderResourceId
              batchResourceId = request.batchResourceId
              fileResourceId = resolvedFileResourceId
              this.blobUri = blobUri
            }
          }
          request.requestsList.indices.map { resultsByIndex.getValue(it) }
        }
      } catch (e: RawImpressionMetadataBatchNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: RawImpressionMetadataBatchFileAlreadyExistsException) {
        throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRawImpressionMetadataBatchFilesResponse {
      rawImpressionMetadataBatchFiles +=
        results.map { result ->
          if (result.hasCreateTime()) {
            result
          } else {
            result.copy {
              createTime = commitTimestamp
              updateTime = commitTimestamp
            }
          }
        }
    }
  }

  override suspend fun getRawImpressionMetadataBatchFile(
    request: GetRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.fileResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("file_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getRawImpressionMetadataBatchFileByResourceId(
            request.dataProviderResourceId,
            request.batchResourceId,
            request.fileResourceId,
          )
          .rawImpressionMetadataBatchFile
      }
    } catch (e: RawImpressionMetadataBatchFileNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRawImpressionMetadataBatchFiles(
    request: ListRawImpressionMetadataBatchFilesRequest
  ): ListRawImpressionMetadataBatchFilesResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
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
    val after: ListRawImpressionMetadataBatchFilesPageToken.After? =
      if (request.hasPageToken()) request.pageToken.after else null
    databaseClient.singleUse().use { txn ->
      val fileFlow: Flow<RawImpressionMetadataBatchFile> =
        txn
          .readRawImpressionMetadataBatchFiles(
            request.dataProviderResourceId,
            request.batchResourceId,
            request.filter,
            pageSize + 1,
            request.showDeleted,
            after,
          )
          .map { it.rawImpressionMetadataBatchFile }
      return listRawImpressionMetadataBatchFilesResponse {
        fileFlow.collectIndexed { index, file ->
          if (index == pageSize) {
            nextPageToken = listRawImpressionMetadataBatchFilesPageToken {
              this.after =
                ListRawImpressionMetadataBatchFilesPageTokenKt.after {
                  createTime =
                    this@listRawImpressionMetadataBatchFilesResponse.rawImpressionMetadataBatchFiles
                      .last()
                      .createTime
                  batchResourceId =
                    this@listRawImpressionMetadataBatchFilesResponse.rawImpressionMetadataBatchFiles
                      .last()
                      .batchResourceId
                  fileResourceId =
                    this@listRawImpressionMetadataBatchFilesResponse.rawImpressionMetadataBatchFiles
                      .last()
                      .fileResourceId
                }
            }
          } else {
            rawImpressionMetadataBatchFiles += file
          }
        }
      }
    }
  }

  override suspend fun deleteRawImpressionMetadataBatchFile(
    request: DeleteRawImpressionMetadataBatchFileRequest
  ): RawImpressionMetadataBatchFile {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.batchResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("batch_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.fileResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("file_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=deleteRawImpressionMetadataBatchFile")
      )
    val deletedFile: RawImpressionMetadataBatchFile =
      try {
        transactionRunner.run { txn ->
          val result: RawImpressionMetadataBatchFileResult =
            txn.getRawImpressionMetadataBatchFileByResourceId(
              request.dataProviderResourceId,
              request.batchResourceId,
              request.fileResourceId,
            )
          if (result.rawImpressionMetadataBatchFile.hasDeleteTime()) {
            throw RawImpressionMetadataBatchFileNotFoundException(
              request.dataProviderResourceId,
              request.batchResourceId,
              request.fileResourceId,
            )
          }
          txn.softDeleteRawImpressionMetadataBatchFile(
            request.dataProviderResourceId,
            result.batchId,
            result.fileId,
          )
          result.rawImpressionMetadataBatchFile.copy {
            clearUpdateTime()
            clearDeleteTime()
          }
        }
      } catch (e: RawImpressionMetadataBatchFileNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return deletedFile.copy {
      updateTime = commitTimestamp
      deleteTime = commitTimestamp
    }
  }

  override suspend fun batchDeleteRawImpressionMetadataBatchFiles(
    request: BatchDeleteRawImpressionMetadataBatchFilesRequest
  ): BatchDeleteRawImpressionMetadataBatchFilesResponse {
    if (request.requestsList.isEmpty()) {
      return BatchDeleteRawImpressionMetadataBatchFilesResponse.getDefaultInstance()
    }
    val dataProviderResourceId: String = request.requestsList.first().dataProviderResourceId
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("requests.0.data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val batchResourceId: String = request.requestsList.first().batchResourceId
    val fileResourceIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      if (subRequest.dataProviderResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.data_provider_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.dataProviderResourceId != dataProviderResourceId) {
        throw DataProviderMismatchException(
            dataProviderResourceId,
            subRequest.dataProviderResourceId,
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.batchResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.batch_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.batchResourceId != batchResourceId) {
        throw InvalidFieldValueException("requests.$index.batch_resource_id") {
            "batch_resource_id must be the same for all requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!fileResourceIdSet.add(subRequest.fileResourceId)) {
        throw InvalidFieldValueException("requests.$index.file_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=batchDeleteRawImpressionMetadataBatchFiles")
      )
    val deletedList: List<RawImpressionMetadataBatchFile> =
      try {
        transactionRunner.run { txn ->
          val existingByResourceId: Map<String, RawImpressionMetadataBatchFileResult> =
            txn.getBatchFilesByResourceIds(
              dataProviderResourceId,
              batchResourceId,
              request.requestsList.map { it.fileResourceId },
            )
          buildList {
            request.requestsList.forEach { subRequest ->
              val result: RawImpressionMetadataBatchFileResult? =
                existingByResourceId[subRequest.fileResourceId]
              if (result == null || result.rawImpressionMetadataBatchFile.hasDeleteTime()) {
                throw RawImpressionMetadataBatchFileNotFoundException(
                  dataProviderResourceId,
                  subRequest.batchResourceId,
                  subRequest.fileResourceId,
                )
              }
              txn.softDeleteRawImpressionMetadataBatchFile(
                dataProviderResourceId,
                result.batchId,
                result.fileId,
              )
              add(result.rawImpressionMetadataBatchFile)
            }
          }
        }
      } catch (e: RawImpressionMetadataBatchFileNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchDeleteRawImpressionMetadataBatchFilesResponse {
      rawImpressionMetadataBatchFiles +=
        deletedList.map {
          it.copy {
            updateTime = commitTimestamp
            deleteTime = commitTimestamp
          }
        }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
