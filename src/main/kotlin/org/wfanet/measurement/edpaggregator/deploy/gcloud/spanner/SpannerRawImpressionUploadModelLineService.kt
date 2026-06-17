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
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionUploadModelLineResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadModelLineByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadModelLinesByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadModelLineExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadModelLineState
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadModelLineNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadModelLineStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState as State
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadModelLinesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadModelLinesResponse

class SpannerRawImpressionUploadModelLineService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RawImpressionUploadModelLineServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUploadModelLine(
    request: CreateRawImpressionUploadModelLineRequest
  ): RawImpressionUploadModelLine {
    try {
      validateCreateRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=createRawImpressionUploadModelLine")
      )
    val modelLine: RawImpressionUploadModelLine =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            val existing =
              txn.findRawImpressionUploadModelLineByRequestId(
                request.dataProviderResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.rawImpressionUploadModelLine
            }
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  request.dataProviderResourceId,
                  request.rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val rawImpressionUploadModelLineId =
            idGenerator.generateNewId { id ->
              txn.rawImpressionUploadModelLineExists(
                request.dataProviderResourceId,
                rawImpressionUploadId,
                id,
              )
            }

          val resourceId =
            "$RAW_IMPRESSION_UPLOAD_MODEL_LINE_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
          val etag = UUID.randomUUID().toString()

          txn.insertRawImpressionUploadModelLine(
            rawImpressionUploadId = rawImpressionUploadId,
            rawImpressionUploadModelLineId = rawImpressionUploadModelLineId,
            rawImpressionUploadModelLineResourceId = resourceId,
            dataProviderResourceId = request.dataProviderResourceId,
            cmmsModelLine = request.rawImpressionUploadModelLine.cmmsModelLine,
            createRequestId = request.requestId,
            etag = etag,
          )

          request.rawImpressionUploadModelLine.copy {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
            state = State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED
            clearCreateTime()
            clearUpdateTime()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS
            .withDescription("RawImpressionUploadModelLine already exists")
            .withCause(e)
            .asRuntimeException()
        }
        throw e
      }

    return if (modelLine.hasCreateTime()) {
      modelLine
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      modelLine.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
      }
    }
  }

  override suspend fun batchCreateRawImpressionUploadModelLines(
    request: BatchCreateRawImpressionUploadModelLinesRequest
  ): BatchCreateRawImpressionUploadModelLinesResponse {
    if (request.requestsList.isEmpty()) {
      return BatchCreateRawImpressionUploadModelLinesResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId

    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requestIdSet = mutableSetOf<String>()
    val cmmsModelLineSet = mutableSetOf<String>()

    request.requestsList.forEachIndexed { index, subRequest ->
      if (subRequest.rawImpressionUploadModelLine.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException(
          "requests.$index.raw_impression_upload_model_line.cmms_model_line"
        ).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!cmmsModelLineSet.add(subRequest.rawImpressionUploadModelLine.cmmsModelLine)) {
        throw InvalidFieldValueException(
          "requests.$index.raw_impression_upload_model_line.cmms_model_line"
        ) { "cmms_model_line is duplicate in the batch of requests" }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isNotEmpty()) {
        try {
          UUID.fromString(requestId)
        } catch (e: IllegalArgumentException) {
          throw InvalidFieldValueException("requests.$index.request_id", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        if (!requestIdSet.add(requestId)) {
          throw InvalidFieldValueException("requests.$index.request_id") {
              "request id $requestId is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=batchCreateRawImpressionUploadModelLines")
      )

    val results: List<RawImpressionUploadModelLine> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadId(dataProviderResourceId, rawImpressionUploadResourceId)
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val existingByRequestId: Map<String, RawImpressionUploadModelLineResult> =
            txn.findRawImpressionUploadModelLinesByRequestIds(
              dataProviderResourceId,
              request.requestsList.mapNotNull { it.requestId.ifEmpty { null } },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              existing.rawImpressionUploadModelLine
            } else {
              val rawImpressionUploadModelLineId =
                idGenerator.generateNewId { id ->
                  txn.rawImpressionUploadModelLineExists(
                    dataProviderResourceId,
                    rawImpressionUploadId,
                    id,
                  )
                }

              val resourceId =
                "$RAW_IMPRESSION_UPLOAD_MODEL_LINE_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
              val etag = UUID.randomUUID().toString()

              txn.insertRawImpressionUploadModelLine(
                rawImpressionUploadId = rawImpressionUploadId,
                rawImpressionUploadModelLineId = rawImpressionUploadModelLineId,
                rawImpressionUploadModelLineResourceId = resourceId,
                dataProviderResourceId = dataProviderResourceId,
                cmmsModelLine = subRequest.rawImpressionUploadModelLine.cmmsModelLine,
                createRequestId = subRequest.requestId,
                etag = etag,
              )

              subRequest.rawImpressionUploadModelLine.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                state = State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED
                clearCreateTime()
                clearUpdateTime()
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS
            .withDescription("RawImpressionUploadModelLine already exists")
            .withCause(e)
            .asRuntimeException()
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRawImpressionUploadModelLinesResponse {
      rawImpressionUploadModelLines +=
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

  override suspend fun getRawImpressionUploadModelLine(
    request: GetRawImpressionUploadModelLineRequest
  ): RawImpressionUploadModelLine {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getRawImpressionUploadModelLineByResourceIds(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.cmmsModelLine,
        )
        ?.rawImpressionUploadModelLine
        ?: throw RawImpressionUploadModelLineNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.cmmsModelLine,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRawImpressionUploadModelLines(
    request: ListRawImpressionUploadModelLinesRequest
  ): ListRawImpressionUploadModelLinesResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE
      else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val rows: Flow<RawImpressionUploadModelLine> =
        txn
          .readRawImpressionUploadModelLines(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            filter = if (request.hasFilter()) request.filter else null,
            limit = pageSize + 1,
            after = after,
          )
          .map { it.rawImpressionUploadModelLine }

      return listRawImpressionUploadModelLinesResponse {
        rows.collectIndexed { index, item ->
          if (index == pageSize) {
            val lastIncluded = this.rawImpressionUploadModelLines.last()
            nextPageToken = listRawImpressionUploadModelLinesPageToken {
              this.after =
                ListRawImpressionUploadModelLinesPageTokenKt.after {
                  createTime = lastIncluded.createTime
                  rawImpressionUploadResourceId = lastIncluded.rawImpressionUploadResourceId
                  cmmsModelLine = lastIncluded.cmmsModelLine
                }
            }
          } else {
            this.rawImpressionUploadModelLines += item
          }
        }
      }
    }
  }

  override suspend fun markRawImpressionUploadModelLinePoolAssigning(
    request: MarkRawImpressionUploadModelLinePoolAssigningRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.cmmsModelLine,
      State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
      validPreviousStates =
        setOf(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED),
    )
  }

  override suspend fun markRawImpressionUploadModelLineRanking(
    request: MarkRawImpressionUploadModelLineRankingRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.cmmsModelLine,
      State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
      validPreviousStates =
        setOf(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING),
    )
  }

  override suspend fun markRawImpressionUploadModelLineLabeling(
    request: MarkRawImpressionUploadModelLineLabelingRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.cmmsModelLine,
      State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
      validPreviousStates =
        setOf(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING),
    )
  }

  override suspend fun markRawImpressionUploadModelLineCompleted(
    request: MarkRawImpressionUploadModelLineCompletedRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.cmmsModelLine,
      State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED,
      validPreviousStates =
        setOf(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING),
    )
  }

  override suspend fun markRawImpressionUploadModelLineFailed(
    request: MarkRawImpressionUploadModelLineFailedRequest
  ): RawImpressionUploadModelLine {
    if (request.errorMessage.isEmpty()) {
      throw RequiredFieldNotSetException("error_message")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val modelLine =
      transitionState(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.cmmsModelLine,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED,
        validPreviousStates =
          setOf(
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
          ),
      ) {
        set("ErrorMessage").to(request.errorMessage)
      }
    return modelLine.copy { errorMessage = request.errorMessage }
  }

  /**
   * Transitions the state of a [RawImpressionUploadModelLine].
   *
   * @throws io.grpc.StatusRuntimeException with INVALID_ARGUMENT if required fields are missing
   * @throws io.grpc.StatusRuntimeException with NOT_FOUND if the resource does not exist
   * @throws io.grpc.StatusRuntimeException with FAILED_PRECONDITION if the current state is invalid
   */
  private suspend fun transitionState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
    nextState: State,
    validPreviousStates: Set<State>,
    block: (com.google.cloud.spanner.Mutation.WriteBuilder.() -> Unit)? = null,
  ): RawImpressionUploadModelLine {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=transitionState"))
    val updatedModelLine =
      transactionRunner.run { txn ->
        val result =
          txn.getRawImpressionUploadModelLineByResourceIds(
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            cmmsModelLine,
          )
            ?: throw RawImpressionUploadModelLineNotFoundException(
                dataProviderResourceId,
                rawImpressionUploadResourceId,
                cmmsModelLine,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentState = result.rawImpressionUploadModelLine.state
        if (currentState !in validPreviousStates) {
          throw RawImpressionUploadModelLineStateInvalidException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              cmmsModelLine,
              result.rawImpressionUploadModelLine.state,
              validPreviousStates,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }

        val newEtag = UUID.randomUUID().toString()

        txn.updateRawImpressionUploadModelLineState(
          dataProviderResourceId,
          result.rawImpressionUploadId,
          result.rawImpressionUploadModelLineId,
          nextState,
          newEtag,
          block,
        )
        result.rawImpressionUploadModelLine
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return updatedModelLine.copy {
      state = nextState
      updateTime = commitTimestamp
    }
  }

  /**
   * Validates a [CreateRawImpressionUploadModelLineRequest].
   *
   * @throws RequiredFieldNotSetException if required fields are missing
   * @throws InvalidFieldValueException if field values are invalid
   */
  private fun validateCreateRequest(request: CreateRawImpressionUploadModelLineRequest) {
    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
      }
    }

    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
    }
    if (!request.hasRawImpressionUploadModelLine()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line")
    }
    if (request.rawImpressionUploadModelLine.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line.cmms_model_line")
    }
  }

  companion object {
    private const val RAW_IMPRESSION_UPLOAD_MODEL_LINE_RESOURCE_ID_PREFIX = "riuml"
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
