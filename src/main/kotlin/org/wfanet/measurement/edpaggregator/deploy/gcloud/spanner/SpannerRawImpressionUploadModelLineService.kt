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
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionUploadModelLineResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.countNonCompletedRawImpressionUploadModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findInProgressModelLinesForModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadModelLineByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadModelLinesByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineByResourceIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadModelLineExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadModelLineState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadModelLineConcurrentException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadModelLineNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadModelLineStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
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
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
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
      databaseClient.readWriteTransaction(Options.tag("action=createRawImpressionUploadModelLine"))
    val modelLine: RawImpressionUploadModelLine =
      try {
        transactionRunner.run { txn ->
          val existing =
            txn.findRawImpressionUploadModelLineByRequestId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.requestId,
            )
          if (existing != null) {
            if (
              existing.rawImpressionUploadModelLine.cmmsModelLine !=
                request.rawImpressionUploadModelLine.cmmsModelLine
            ) {
              throw Status.ALREADY_EXISTS.withDescription(
                  "RawImpressionUploadModelLine already exists for request_id " +
                    "${request.requestId} with a different cmms_model_line"
                )
                .asRuntimeException()
            }
            return@run existing.rawImpressionUploadModelLine
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )

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

          txn.insertRawImpressionUploadModelLine(
            rawImpressionUploadId = rawImpressionUploadId,
            rawImpressionUploadModelLineId = rawImpressionUploadModelLineId,
            rawImpressionUploadModelLineResourceId = resourceId,
            dataProviderResourceId = request.dataProviderResourceId,
            cmmsModelLine = request.rawImpressionUploadModelLine.cmmsModelLine,
            createRequestId = request.requestId,
          )

          request.rawImpressionUploadModelLine.copy {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
            rawImpressionUploadModelLineResourceId = resourceId
            state = State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED
            clearCreateTime()
            clearUpdateTime()
          }
        }
      } catch (e: RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS.withDescription("RawImpressionUploadModelLine already exists")
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
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    }
  }

  override suspend fun batchCreateRawImpressionUploadModelLines(
    request: BatchCreateRawImpressionUploadModelLinesRequest
  ): BatchCreateRawImpressionUploadModelLinesResponse {
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
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
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!cmmsModelLineSet.add(subRequest.rawImpressionUploadModelLine.cmmsModelLine)) {
        throw InvalidFieldValueException(
            "requests.$index.raw_impression_upload_model_line.cmms_model_line"
          ) {
            "cmms_model_line is duplicate in the batch of requests"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.request_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
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

    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=batchCreateRawImpressionUploadModelLines")
      )

    val results: List<RawImpressionUploadModelLine> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadId(dataProviderResourceId, rawImpressionUploadResourceId)

          val existingByRequestId: Map<String, RawImpressionUploadModelLineResult> =
            txn.findRawImpressionUploadModelLinesByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.map { it.requestId },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              if (
                existing.rawImpressionUploadModelLine.cmmsModelLine !=
                  subRequest.rawImpressionUploadModelLine.cmmsModelLine
              ) {
                throw Status.ALREADY_EXISTS.withDescription(
                    "RawImpressionUploadModelLine already exists for request_id " +
                      "${subRequest.requestId} with a different cmms_model_line"
                  )
                  .asRuntimeException()
              }
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

              txn.insertRawImpressionUploadModelLine(
                rawImpressionUploadId = rawImpressionUploadId,
                rawImpressionUploadModelLineId = rawImpressionUploadModelLineId,
                rawImpressionUploadModelLineResourceId = resourceId,
                dataProviderResourceId = dataProviderResourceId,
                cmmsModelLine = subRequest.rawImpressionUploadModelLine.cmmsModelLine,
                createRequestId = subRequest.requestId,
              )

              subRequest.rawImpressionUploadModelLine.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                rawImpressionUploadModelLineResourceId = resourceId
                state = State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED
                clearCreateTime()
                clearUpdateTime()
              }
            }
          }
        }
      } catch (e: RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS.withDescription("RawImpressionUploadModelLine already exists")
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
              etag = ETags.computeETag(commitTimestamp.toInstant())
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
    if (request.rawImpressionUploadModelLineResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getRawImpressionUploadModelLineByResourceIds(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.rawImpressionUploadModelLineResourceId,
        )
        ?.rawImpressionUploadModelLine
        ?: throw RawImpressionUploadModelLineNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.rawImpressionUploadModelLineResourceId,
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
      val rows: Flow<RawImpressionUploadModelLine> =
        txn
          .readRawImpressionUploadModelLines(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId.ifEmpty { null },
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
        request.rawImpressionUploadModelLineResourceId,
        request.etag,
        request.requestId,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
        validPreviousStates =
          setOf(
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED,
          ),
        markRequestIdColumn = "MarkPoolAssigningRequestId",
        currentMarkRequestId = { it.markPoolAssigningRequestId },
      )
      .modelLine
  }

  override suspend fun markRawImpressionUploadModelLineRanking(
    request: MarkRawImpressionUploadModelLineRankingRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.rawImpressionUploadModelLineResourceId,
        request.etag,
        request.requestId,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
        validPreviousStates =
          setOf(
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED,
          ),
        markRequestIdColumn = "MarkRankingRequestId",
        currentMarkRequestId = { it.markRankingRequestId },
      )
      .modelLine
  }

  override suspend fun markRawImpressionUploadModelLineLabeling(
    request: MarkRawImpressionUploadModelLineLabelingRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.rawImpressionUploadModelLineResourceId,
        request.etag,
        request.requestId,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
        validPreviousStates =
          setOf(
            // Non-memoized uploads skip Phase 0/1 and go straight CREATED -> LABELING.
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED,
          ),
        markRequestIdColumn = "MarkLabelingRequestId",
        currentMarkRequestId = { it.markLabelingRequestId },
      )
      .modelLine
  }

  override suspend fun markRawImpressionUploadModelLineCompleted(
    request: MarkRawImpressionUploadModelLineCompletedRequest
  ): RawImpressionUploadModelLine {
    return transitionState(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.rawImpressionUploadModelLineResourceId,
        request.etag,
        request.requestId,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED,
        validPreviousStates = setOf(State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING),
        markRequestIdColumn = "MarkCompletedRequestId",
        currentMarkRequestId = { it.markCompletedRequestId },
      )
      .modelLine
  }

  override suspend fun markRawImpressionUploadModelLineFailed(
    request: MarkRawImpressionUploadModelLineFailedRequest
  ): RawImpressionUploadModelLine {

    val transitionResult =
      transitionState(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.rawImpressionUploadModelLineResourceId,
        request.etag,
        request.requestId,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED,
        validPreviousStates =
          setOf(
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
            State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
          ),
        markRequestIdColumn = "MarkFailedRequestId",
        currentMarkRequestId = { it.markFailedRequestId },
      ) {
        set("ErrorMessage").to(request.errorMessage)
      }
    // On an AIP-155 replay the stored (first) error_message is authoritative and returned as-is;
    // only a fresh transition adopts this request's error_message (the row read before the write
    // does not yet reflect it).
    return if (transitionResult.isReplay) {
      transitionResult.modelLine
    } else {
      transitionResult.modelLine.copy { errorMessage = request.errorMessage }
    }
  }

  /**
   * The outcome of a [transitionState] call.
   *
   * @property modelLine the resulting row
   * @property isReplay true if this was an AIP-155 idempotent replay (this mark's request_id was
   *   already stamped on the row), so [modelLine] is the current committed row, not a re-transition
   */
  private data class TransactionResult(
    val modelLine: RawImpressionUploadModelLine,
    val isReplay: Boolean = false,
  )

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
    rawImpressionUploadModelLineResourceId: String,
    expectedEtag: String,
    requestId: String,
    nextState: State,
    validPreviousStates: Set<State>,
    markRequestIdColumn: String,
    currentMarkRequestId: (RawImpressionUploadModelLineResult) -> String,
    block: (com.google.cloud.spanner.Mutation.WriteBuilder.() -> Unit)? = null,
  ): TransactionResult {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadModelLineResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_model_line_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=transitionState"))
    val txnResult =
      transactionRunner.run { txn ->
        val result =
          txn.getRawImpressionUploadModelLineByResourceIds(
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            rawImpressionUploadModelLineResourceId,
          )
            ?: throw RawImpressionUploadModelLineNotFoundException(
                dataProviderResourceId,
                rawImpressionUploadResourceId,
                rawImpressionUploadModelLineResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        // AIP-155 idempotent replay: if this exact mark already ran (this mark's request_id is
        // already stamped on the row), return the resource as-is instead of re-transitioning or
        // throwing FAILED_PRECONDITION on a Pub/Sub redelivery. Matching the request_id is
        // sufficient per AIP-155, even if the resource has since advanced to a later state.
        if (currentMarkRequestId(result) == requestId) {
          return@run TransactionResult(result.rawImpressionUploadModelLine, isReplay = true)
        }

        if (expectedEtag.isEmpty()) {
          throw RequiredFieldNotSetException("etag")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
        if (expectedEtag != result.rawImpressionUploadModelLine.etag) {
          throw EtagMismatchException(expectedEtag, result.rawImpressionUploadModelLine.etag)
            .asStatusRuntimeException(Status.Code.ABORTED)
        }

        val currentState = result.rawImpressionUploadModelLine.state
        if (currentState !in validPreviousStates) {
          throw RawImpressionUploadModelLineStateInvalidException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              rawImpressionUploadModelLineResourceId,
              result.rawImpressionUploadModelLine.state,
              validPreviousStates,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }

        // One upload in-flight per (DataProvider, cmms_model_line): concurrent Phase-1 rankers
        // would corrupt the shared cumulative rank index.
        if (nextState in PROCESSING_STATES) {
          val conflicts =
            txn.findInProgressModelLinesForModelLine(
              dataProviderResourceId,
              result.rawImpressionUploadModelLine.cmmsModelLine,
              excludeRawImpressionUploadId = result.rawImpressionUploadId,
            )
          if (conflicts.isNotEmpty()) {
            throw RawImpressionUploadModelLineConcurrentException(
                dataProviderResourceId,
                rawImpressionUploadModelLineResourceId,
                result.rawImpressionUploadModelLine.cmmsModelLine,
                nextState,
                conflicts.map { it.rawImpressionUploadResourceId to it.state },
              )
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          }
        }

        val clearError = nextState != State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED

        txn.updateRawImpressionUploadModelLineState(
          dataProviderResourceId,
          result.rawImpressionUploadId,
          result.rawImpressionUploadModelLineId,
          nextState,
        ) {
          if (clearError) set("ErrorMessage").to(null as String?)
          set(markRequestIdColumn).to(requestId)
          block?.invoke(this)
        }

        // Cascade the child transition up to the parent RawImpressionUpload in this same
        // (interleaved) transaction — no separate parent Mark RPC. First child to (re)start
        // processing flips CREATED -> ACTIVE; any child failure flips -> FAILED (non-terminal);
        // -> COMPLETED only once every child is COMPLETED.
        when (nextState) {
          State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED ->
            txn.updateRawImpressionUploadState(
              dataProviderResourceId,
              result.rawImpressionUploadId,
              RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED,
            )
          State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED -> {
            // This line's COMPLETED write is buffered (not visible to the read), so exclude it.
            if (
              txn.countNonCompletedRawImpressionUploadModelLines(
                dataProviderResourceId,
                result.rawImpressionUploadId,
                result.rawImpressionUploadModelLineId,
              ) == 0L
            ) {
              txn.updateRawImpressionUploadState(
                dataProviderResourceId,
                result.rawImpressionUploadId,
                RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED,
              )
            }
          }
          State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
          State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
          State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING -> {
            // Guard on CREATED|FAILED so a retry re-activates a FAILED upload, but a processing
            // transition never resurrects a just-set COMPLETED.
            val parentState =
              txn.getRawImpressionUploadState(dataProviderResourceId, result.rawImpressionUploadId)
            if (
              parentState == RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED ||
                parentState == RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED
            ) {
              txn.updateRawImpressionUploadState(
                dataProviderResourceId,
                result.rawImpressionUploadId,
                RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE,
              )
            }
          }
          else -> {}
        }
        TransactionResult(
          result.rawImpressionUploadModelLine.copy { if (clearError) clearErrorMessage() }
        )
      }

    // Carry the replay flag in the transaction's return value (not a var mutated inside the lambda)
    // so it stays correct across Spanner ABORTED retries, matching SpannerRankerJobService /
    // SpannerVidLabelingJobService. isReplay=true returns the row as already committed (possibly in
    // a later state); otherwise stamp the commit timestamp and etag onto the just-transitioned row.
    if (txnResult.isReplay) {
      return txnResult
    }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return TransactionResult(
      txnResult.modelLine.copy {
        state = nextState
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    )
  }

  /**
   * Validates a [CreateRawImpressionUploadModelLineRequest].
   *
   * @throws RequiredFieldNotSetException if required fields are missing
   * @throws InvalidFieldValueException if field values are invalid
   */
  private fun validateCreateRequest(request: CreateRawImpressionUploadModelLineRequest) {
    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
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
    private const val MAX_BATCH_SIZE = 50
    private const val DEFAULT_PAGE_SIZE = 50

    private val PROCESSING_STATES =
      setOf(
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING,
        State.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING,
      )
  }
}
