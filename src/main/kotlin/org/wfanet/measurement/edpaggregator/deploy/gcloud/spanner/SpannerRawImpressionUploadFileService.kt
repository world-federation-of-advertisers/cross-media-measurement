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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findExistingUploadFilesByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadFileByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getUploadFilesByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadFileExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadFiles
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.softDeleteRawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.service.internal.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadFileNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionUploadFileRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFile
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.batchDeleteRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadFile

class SpannerRawImpressionUploadFileService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadFileServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUploadFile(
    request: CreateRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    val file: RawImpressionUploadFile = request.rawImpressionUploadFile
    if (file.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_file.data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (file.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException(
          "raw_impression_upload_file.raw_impression_upload_resource_id"
        )
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (file.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_file.blob_uri")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val requestId: String = request.requestId
    validateRequestId(requestId, "request_id")

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRawImpressionUploadFile"))
    val result: RawImpressionUploadFile =
      try {
        transactionRunner.run { txn ->
          if (
            !txn.rawImpressionUploadExists(
              file.dataProviderResourceId,
              file.rawImpressionUploadResourceId,
            )
          ) {
            throw RawImpressionUploadNotFoundException(
              file.dataProviderResourceId,
              file.rawImpressionUploadResourceId,
            )
          }
          val existingByRequestId: Map<String, RawImpressionUploadFile> =
            txn.findExistingUploadFilesByRequestIds(
              file.dataProviderResourceId,
              file.rawImpressionUploadResourceId,
              listOf(requestId),
            )
          if (existingByRequestId.containsKey(requestId)) {
            return@run existingByRequestId.getValue(requestId)
          }
          val fileResourceId: String =
            generateFileResourceId(
              txn,
              file.dataProviderResourceId,
              file.rawImpressionUploadResourceId,
            )
          txn.insertRawImpressionUploadFile(
            file.dataProviderResourceId,
            file.rawImpressionUploadResourceId,
            fileResourceId,
            file.blobUri,
            requestId,
          )
          rawImpressionUploadFile {
            dataProviderResourceId = file.dataProviderResourceId
            rawImpressionUploadResourceId = file.rawImpressionUploadResourceId
            this.fileResourceId = fileResourceId
            blobUri = file.blobUri
          }
        }
      } catch (e: RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
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

  override suspend fun batchCreateRawImpressionUploadFiles(
    request: BatchCreateRawImpressionUploadFilesRequest
  ): BatchCreateRawImpressionUploadFilesResponse {
    if (request.requestsList.isEmpty()) {
      return BatchCreateRawImpressionUploadFilesResponse.getDefaultInstance()
    }
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val requestIdSet = mutableSetOf<String>()
    request.requestsList.forEachIndexed { index, subRequest ->
      val file: RawImpressionUploadFile = subRequest.rawImpressionUploadFile
      if (
        file.dataProviderResourceId.isNotEmpty() &&
          file.dataProviderResourceId != request.dataProviderResourceId
      ) {
        throw DataProviderMismatchException(
            request.dataProviderResourceId,
            file.dataProviderResourceId,
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (
        file.rawImpressionUploadResourceId.isNotEmpty() &&
          file.rawImpressionUploadResourceId != request.rawImpressionUploadResourceId
      ) {
        throw InvalidFieldValueException(
            "requests.$index.raw_impression_upload_file.raw_impression_upload_resource_id"
          ) {
            "raw_impression_upload_resource_id does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (file.blobUri.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.raw_impression_upload_file.blob_uri")
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
        validateRequestId(subRequestId, "requests.$index.request_id")
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateRawImpressionUploadFiles"))
    val results: List<RawImpressionUploadFile> =
      try {
        transactionRunner.run { txn ->
          if (
            !txn.rawImpressionUploadExists(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )
          ) {
            throw RawImpressionUploadNotFoundException(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )
          }
          val existingByRequestId: Map<String, RawImpressionUploadFile> =
            txn.findExistingUploadFilesByRequestIds(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.requestsList.map { it.requestId },
            )
          request.requestsList.map { subRequest ->
            if (
              subRequest.requestId.isNotEmpty() &&
                existingByRequestId.containsKey(subRequest.requestId)
            ) {
              return@map existingByRequestId.getValue(subRequest.requestId)
            }
            val blobUri: String = subRequest.rawImpressionUploadFile.blobUri
            val fileResourceId: String =
              generateFileResourceId(
                txn,
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
              )
            txn.insertRawImpressionUploadFile(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              fileResourceId,
              blobUri,
              subRequest.requestId,
            )
            rawImpressionUploadFile {
              dataProviderResourceId = request.dataProviderResourceId
              rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
              this.fileResourceId = fileResourceId
              this.blobUri = blobUri
            }
          }
        }
      } catch (e: RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRawImpressionUploadFilesResponse {
      rawImpressionUploadFiles +=
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

  override suspend fun getRawImpressionUploadFile(
    request: GetRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.fileResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("file_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return try {
      databaseClient.singleUse().use { txn ->
        txn.getRawImpressionUploadFileByResourceId(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.fileResourceId,
        )
      }
    } catch (e: RawImpressionUploadFileNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRawImpressionUploadFiles(
    request: ListRawImpressionUploadFilesRequest
  ): ListRawImpressionUploadFilesResponse {
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
    val after: ListRawImpressionUploadFilesPageToken.After? =
      if (request.hasPageToken()) request.pageToken.after else null
    databaseClient.singleUse().use { txn ->
      val fileFlow: Flow<RawImpressionUploadFile> =
        txn.readRawImpressionUploadFiles(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.filter,
          pageSize + 1,
          request.showDeleted,
          after,
        )
      return listRawImpressionUploadFilesResponse {
        fileFlow.collectIndexed { index, file ->
          if (index == pageSize) {
            nextPageToken = listRawImpressionUploadFilesPageToken {
              this.after =
                ListRawImpressionUploadFilesPageTokenKt.after {
                  createTime =
                    this@listRawImpressionUploadFilesResponse.rawImpressionUploadFiles
                      .last()
                      .createTime
                  rawImpressionUploadResourceId =
                    this@listRawImpressionUploadFilesResponse.rawImpressionUploadFiles
                      .last()
                      .rawImpressionUploadResourceId
                  fileResourceId =
                    this@listRawImpressionUploadFilesResponse.rawImpressionUploadFiles
                      .last()
                      .fileResourceId
                }
            }
          } else {
            rawImpressionUploadFiles += file
          }
        }
      }
    }
  }

  override suspend fun deleteRawImpressionUploadFile(
    request: DeleteRawImpressionUploadFileRequest
  ): RawImpressionUploadFile {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.fileResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("file_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=deleteRawImpressionUploadFile"))
    val deletedFile: RawImpressionUploadFile =
      try {
        transactionRunner.run { txn ->
          val result: RawImpressionUploadFile =
            txn.getRawImpressionUploadFileByResourceId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.fileResourceId,
            )
          if (result.hasDeleteTime()) {
            throw RawImpressionUploadFileNotFoundException(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.fileResourceId,
            )
          }
          txn.softDeleteRawImpressionUploadFile(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.fileResourceId,
          )
          result.copy {
            clearUpdateTime()
            clearDeleteTime()
          }
        }
      } catch (e: RawImpressionUploadFileNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return deletedFile.copy {
      updateTime = commitTimestamp
      deleteTime = commitTimestamp
    }
  }

  override suspend fun batchDeleteRawImpressionUploadFiles(
    request: BatchDeleteRawImpressionUploadFilesRequest
  ): BatchDeleteRawImpressionUploadFilesResponse {
    if (request.requestsList.isEmpty()) {
      return BatchDeleteRawImpressionUploadFilesResponse.getDefaultInstance()
    }
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val fileResourceIdSet = mutableSetOf<String>()
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
      if (
        subRequest.rawImpressionUploadResourceId.isNotEmpty() &&
          subRequest.rawImpressionUploadResourceId != request.rawImpressionUploadResourceId
      ) {
        throw InvalidFieldValueException("requests.$index.raw_impression_upload_resource_id") {
            "raw_impression_upload_resource_id does not match the parent"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.fileResourceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.file_resource_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!fileResourceIdSet.add(subRequest.fileResourceId)) {
        throw InvalidFieldValueException("requests.$index.file_resource_id") {
            "file_resource_id is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchDeleteRawImpressionUploadFiles"))
    val deletedList: List<RawImpressionUploadFile> =
      try {
        transactionRunner.run { txn ->
          val existingByResourceId: Map<String, RawImpressionUploadFile> =
            txn.getUploadFilesByResourceIds(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.requestsList.map { it.fileResourceId },
            )
          request.requestsList.map { subRequest ->
            val result: RawImpressionUploadFile? = existingByResourceId[subRequest.fileResourceId]
            if (result == null || result.hasDeleteTime()) {
              throw RawImpressionUploadFileNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                subRequest.fileResourceId,
              )
            }
            txn.softDeleteRawImpressionUploadFile(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              subRequest.fileResourceId,
            )
            result
          }
        }
      } catch (e: RawImpressionUploadFileNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchDeleteRawImpressionUploadFilesResponse {
      rawImpressionUploadFiles +=
        deletedList.map {
          it.copy {
            updateTime = commitTimestamp
            deleteTime = commitTimestamp
          }
        }
    }
  }

  private fun validateRequestId(requestId: String, fieldName: String) {
    if (requestId.isEmpty()) return
    try {
      UUID.fromString(requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(fieldName, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private suspend fun generateFileResourceId(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ): String {
    var fileResourceId: String = UUID.randomUUID().toString()
    while (
      txn.rawImpressionUploadFileExists(
        dataProviderResourceId,
        rawImpressionUploadResourceId,
        fileResourceId,
      )
    ) {
      fileResourceId = UUID.randomUUID().toString()
    }
    return fileResourceId
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
