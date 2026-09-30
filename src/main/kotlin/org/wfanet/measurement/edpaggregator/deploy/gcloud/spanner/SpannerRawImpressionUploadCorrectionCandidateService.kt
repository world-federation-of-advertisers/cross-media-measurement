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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.security.MessageDigest
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionUploadCorrectionCandidateResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidateByAdvanceRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidateByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploadCorrectionCandidates
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AdvanceRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate

/** Persists VID-labeling raw-impression upload correction candidates. */
class SpannerRawImpressionUploadCorrectionCandidateService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUploadCorrectionCandidate(
    request: CreateRawImpressionUploadCorrectionCandidateRequest
  ): RawImpressionUploadCorrectionCandidate {
    validateCreateRequest(request)
    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=createRawImpressionUploadCorrectionCandidate")
      )
    try {
      transactionRunner.run { txn ->
        val existingById =
          txn.findRawImpressionUploadCorrectionCandidate(
            request.dataProviderResourceId,
            request.rawImpressionUploadCorrectionCandidateId,
          )
        if (existingById != null) {
          if (isIdempotentCreate(existingById, request)) return@run
          throw alreadyExists(request.rawImpressionUploadCorrectionCandidateId)
        }
        val existingByRequestId =
          txn.findRawImpressionUploadCorrectionCandidateByCreateRequestId(
            request.dataProviderResourceId,
            request.requestId,
          )
        if (existingByRequestId != null) {
          if (isIdempotentCreate(existingByRequestId, request)) return@run
          throw alreadyExists(
            existingByRequestId.rawImpressionUploadCorrectionCandidate
              .rawImpressionUploadCorrectionCandidateId
          )
        }
        val upload =
          txn
            .getRawImpressionUploadByResourceId(
              request.dataProviderResourceId,
              request.rawImpressionUploadCorrectionCandidate.rawImpressionUploadResourceId,
            )
            .rawImpressionUpload
        if (
          upload.state !=
            RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED ||
            !upload.registrationComplete ||
            !upload.hasDoneBlobCreateTime()
        ) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "RawImpressionUpload ${upload.rawImpressionUploadResourceId} is not a finalized quarantined manifest"
            )
            .asRuntimeException()
        }
        txn.insertRawImpressionUploadCorrectionCandidate(
          request.rawImpressionUploadCorrectionCandidate.copy {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadCorrectionCandidateId =
              request.rawImpressionUploadCorrectionCandidateId
          },
          request.requestId,
        )
      }
    } catch (e: RawImpressionUploadNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        val existing =
          databaseClient.singleUse().use { txn ->
            txn.findRawImpressionUploadCorrectionCandidate(
              request.dataProviderResourceId,
              request.rawImpressionUploadCorrectionCandidateId,
            )
              ?: txn.findRawImpressionUploadCorrectionCandidateByCreateRequestId(
                request.dataProviderResourceId,
                request.requestId,
              )
          }
        if (existing != null && isIdempotentCreate(existing, request)) {
          return existing.rawImpressionUploadCorrectionCandidate
        }
        throw alreadyExists(request.rawImpressionUploadCorrectionCandidateId)
      }
      throw e
    }
    return getCandidate(
      request.dataProviderResourceId,
      request.rawImpressionUploadCorrectionCandidateId,
    )
  }

  override suspend fun getRawImpressionUploadCorrectionCandidate(
    request: GetRawImpressionUploadCorrectionCandidateRequest
  ): RawImpressionUploadCorrectionCandidate {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireField(
      request.rawImpressionUploadCorrectionCandidateId,
      "raw_impression_upload_correction_candidate_id",
    )
    return getCandidate(
      request.dataProviderResourceId,
      request.rawImpressionUploadCorrectionCandidateId,
    )
  }

  override suspend fun listRawImpressionUploadCorrectionCandidates(
    request: ListRawImpressionUploadCorrectionCandidatesRequest
  ): ListRawImpressionUploadCorrectionCandidatesResponse {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    if (request.pageSize < 0) invalid("page_size must be non-negative")
    request.filter.stateInList.forEach { state ->
      if (
        state == RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED ||
          state == RawImpressionUploadCorrectionCandidate.State.UNRECOGNIZED
      ) {
        invalid("filter.state_in contains an unspecified state")
      }
    }
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val after = if (request.hasPageToken()) request.pageToken.after else null
    databaseClient.singleUse().use { txn ->
      val candidates: Flow<RawImpressionUploadCorrectionCandidate> =
        txn
          .readRawImpressionUploadCorrectionCandidates(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            after,
          )
          .map { it.rawImpressionUploadCorrectionCandidate }
      return listRawImpressionUploadCorrectionCandidatesResponse {
        candidates.collectIndexed { index, candidate ->
          if (index == pageSize) {
            nextPageToken = listRawImpressionUploadCorrectionCandidatesPageToken {
              this.after =
                ListRawImpressionUploadCorrectionCandidatesPageTokenKt.after {
                  createTime =
                    this@listRawImpressionUploadCorrectionCandidatesResponse
                      .rawImpressionUploadCorrectionCandidates
                      .last()
                      .createTime
                  rawImpressionUploadCorrectionCandidateId =
                    this@listRawImpressionUploadCorrectionCandidatesResponse
                      .rawImpressionUploadCorrectionCandidates
                      .last()
                      .rawImpressionUploadCorrectionCandidateId
                }
            }
          } else {
            rawImpressionUploadCorrectionCandidates += candidate
          }
        }
      }
    }
  }

  override suspend fun advanceRawImpressionUploadCorrectionCandidate(
    request: AdvanceRawImpressionUploadCorrectionCandidateRequest
  ): RawImpressionUploadCorrectionCandidate {
    validateAdvanceRequest(request)
    val requestFingerprint = request.fingerprint()
    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=advanceRawImpressionUploadCorrectionCandidate")
      )
    try {
      transactionRunner.run { txn ->
        val existingByRequestId =
          txn.findRawImpressionUploadCorrectionCandidateByAdvanceRequestId(
            request.dataProviderResourceId,
            request.requestId,
          )
        if (existingByRequestId != null) {
          if (isIdempotentAdvance(existingByRequestId, request, requestFingerprint)) return@run
          throw requestIdAlreadyUsed()
        }
        val result =
          txn.findRawImpressionUploadCorrectionCandidate(
            request.dataProviderResourceId,
            request.rawImpressionUploadCorrectionCandidateId,
          ) ?: throw notFound(request.rawImpressionUploadCorrectionCandidateId)
        val current = result.rawImpressionUploadCorrectionCandidate
        if (current.etag != request.etag) {
          throw Status.ABORTED.withDescription(
              "raw_impression_upload_correction_candidate etag mismatch"
            )
            .asRuntimeException()
        }
        val updated = transitionCandidate(txn, current, request)
        txn.updateRawImpressionUploadCorrectionCandidate(
          updated,
          result.advanceRequestIds + request.requestId,
          result.advanceRequestFingerprints + listOf(requestFingerprint),
        )
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw requestIdAlreadyUsed()
      }
      throw e
    }
    return getCandidate(
      request.dataProviderResourceId,
      request.rawImpressionUploadCorrectionCandidateId,
    )
  }

  private suspend fun transitionCandidate(
    txn: AsyncDatabaseClient.TransactionContext,
    current: RawImpressionUploadCorrectionCandidate,
    request: AdvanceRawImpressionUploadCorrectionCandidateRequest,
  ): RawImpressionUploadCorrectionCandidate {
    val allowedActions = allowedActions(current.state)
    if (request.action !in allowedActions) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "Action ${request.action} is not valid from ${current.state}"
        )
        .asRuntimeException()
    }
    val supersedingCandidate =
      if (request.action == AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE) {
        txn
          .findRawImpressionUploadCorrectionCandidate(
            request.dataProviderResourceId,
            request.supersedingRawImpressionUploadCorrectionCandidateId,
          )
          ?.rawImpressionUploadCorrectionCandidate
          ?: throw notFound(request.supersedingRawImpressionUploadCorrectionCandidateId)
      } else {
        null
      }
    if (supersedingCandidate != null) {
      val currentUpload =
        txn
          .getRawImpressionUploadByResourceId(
            request.dataProviderResourceId,
            current.rawImpressionUploadResourceId,
          )
          .rawImpressionUpload
      val supersedingUpload =
        txn
          .getRawImpressionUploadByResourceId(
            request.dataProviderResourceId,
            supersedingCandidate.rawImpressionUploadResourceId,
          )
          .rawImpressionUpload
      if (
        supersedingUpload.doneBlobUri != currentUpload.doneBlobUri ||
          !supersedingUpload.hasDoneBlobCreateTime() ||
          !currentUpload.hasDoneBlobCreateTime() ||
          Timestamps.compare(
            supersedingUpload.doneBlobCreateTime,
            currentUpload.doneBlobCreateTime,
          ) <= 0
      ) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Superseding candidate must be a newer revision of the same done object"
          )
          .asRuntimeException()
      }
      if (
        supersedingCandidate.state !in
          setOf(
            RawImpressionUploadCorrectionCandidate.State.STATE_PENDING,
            RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED,
          )
      ) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Superseding candidate must still be pending approval"
          )
          .asRuntimeException()
      }
    }
    return current.copy {
      when (request.action) {
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN -> {
          val operation =
            txn
              .findUploadHealingOperation(
                request.dataProviderResourceId,
                request.uploadHealingOperationId,
              )
              ?.uploadHealingOperation
              ?: throw Status.FAILED_PRECONDITION.withDescription(
                  "The assigned upload-healing operation does not exist"
                )
                .asRuntimeException()
          if (
            operation.state !=
              UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_IN_PROGRESS
          ) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "The assigned upload-healing operation is not active"
              )
              .asRuntimeException()
          }
          state = RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED
          uploadHealingOperationId = request.uploadHealingOperationId
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT -> {
          state = RawImpressionUploadCorrectionCandidate.State.STATE_APPROVED
          decision = RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT -> {
          state = RawImpressionUploadCorrectionCandidate.State.STATE_APPROVED
          decision = RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING -> {
          state = RawImpressionUploadCorrectionCandidate.State.STATE_HEALING
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE -> {
          val operation =
            txn
              .findUploadHealingOperation(
                request.dataProviderResourceId,
                current.uploadHealingOperationId,
              )
              ?.uploadHealingOperation
              ?: throw Status.FAILED_PRECONDITION.withDescription(
                  "The assigned upload-healing operation does not exist"
                )
                .asRuntimeException()
          if (
            operation.state != UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE
          ) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "The assigned upload-healing operation is not complete"
              )
              .asRuntimeException()
          }
          state =
            if (
              current.decision ==
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
            ) {
              RawImpressionUploadCorrectionCandidate.State.STATE_NO_REPLACEMENT
            } else {
              RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE
            }
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT -> {
          state = RawImpressionUploadCorrectionCandidate.State.STATE_REJECTED
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE -> {
          val superseding = checkNotNull(supersedingCandidate)
          if (
            superseding.rawImpressionUploadCorrectionCandidateId ==
              current.rawImpressionUploadCorrectionCandidateId
          ) {
            invalid("a candidate cannot supersede itself")
          }
          state = RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED
          supersedingRawImpressionUploadCorrectionCandidateId =
            request.supersedingRawImpressionUploadCorrectionCandidateId
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION -> {
          state = RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
        }
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED,
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.UNRECOGNIZED ->
          error("action was validated")
      }
    }
  }

  private suspend fun getCandidate(
    dataProviderResourceId: String,
    rawImpressionUploadCorrectionCandidateId: String,
  ): RawImpressionUploadCorrectionCandidate =
    databaseClient.singleUse().use { txn ->
      txn
        .findRawImpressionUploadCorrectionCandidate(
          dataProviderResourceId,
          rawImpressionUploadCorrectionCandidateId,
        )
        ?.rawImpressionUploadCorrectionCandidate
        ?: throw notFound(rawImpressionUploadCorrectionCandidateId)
    }

  private fun validateCreateRequest(request: CreateRawImpressionUploadCorrectionCandidateRequest) {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(
      request.rawImpressionUploadCorrectionCandidateId,
      "raw_impression_upload_correction_candidate_id",
    )
    requireUuid(request.requestId, "request_id")
    if (!request.hasRawImpressionUploadCorrectionCandidate())
      invalid("raw_impression_upload_correction_candidate is required")
    val candidate = request.rawImpressionUploadCorrectionCandidate
    requireField(candidate.rawImpressionUploadResourceId, "raw_impression_upload_resource_id")
    if (
      candidate.classification ==
        RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED ||
        candidate.classification ==
          RawImpressionUploadCorrectionCandidate.Classification.UNRECOGNIZED
    ) {
      invalid("classification is required")
    }
    if (candidate.priorManifestDigest.size() != MANIFEST_DIGEST_SIZE_BYTES) {
      invalid("prior_manifest_digest must be a SHA-256 digest")
    }
    if (candidate.currentManifestDigest.size() != MANIFEST_DIGEST_SIZE_BYTES) {
      invalid("current_manifest_digest must be a SHA-256 digest")
    }
    if (!candidate.hasExpireTime() || !Timestamps.isValid(candidate.expireTime)) {
      invalid("expire_time is required and must be valid")
    }
    if (
      candidate.dataProviderResourceId.isNotEmpty() ||
        candidate.rawImpressionUploadCorrectionCandidateId.isNotEmpty() ||
        candidate.state != RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED ||
        candidate.decision !=
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED ||
        candidate.supersedingRawImpressionUploadCorrectionCandidateId.isNotEmpty() ||
        candidate.uploadHealingOperationId.isNotEmpty() ||
        candidate.hasCreateTime() ||
        candidate.hasUpdateTime() ||
        candidate.etag.isNotEmpty()
    ) {
      invalid("output-only candidate fields must be unset")
    }
  }

  private fun validateAdvanceRequest(
    request: AdvanceRawImpressionUploadCorrectionCandidateRequest
  ) {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(
      request.rawImpressionUploadCorrectionCandidateId,
      "raw_impression_upload_correction_candidate_id",
    )
    requireField(request.etag, "etag")
    requireUuid(request.requestId, "request_id")
    if (
      request.action ==
        AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED ||
        request.action == AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.UNRECOGNIZED
    ) {
      invalid("action is required")
    }
    when (request.action) {
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN -> {
        requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
        if (request.supersedingRawImpressionUploadCorrectionCandidateId.isNotEmpty())
          invalid("superseding_raw_impression_upload_correction_candidate_id must be empty")
      }
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE -> {
        requireUuid(
          request.supersedingRawImpressionUploadCorrectionCandidateId,
          "superseding_raw_impression_upload_correction_candidate_id",
        )
        if (request.uploadHealingOperationId.isNotEmpty())
          invalid("upload_healing_operation_id must be empty")
      }
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION -> {
        if (request.uploadHealingOperationId.isNotEmpty())
          invalid("upload_healing_operation_id must be empty")
        if (request.supersedingRawImpressionUploadCorrectionCandidateId.isNotEmpty())
          invalid("superseding_raw_impression_upload_correction_candidate_id must be empty")
      }
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED,
      AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.UNRECOGNIZED ->
        error("action was validated")
    }
  }

  private fun allowedActions(
    state: RawImpressionUploadCorrectionCandidate.State
  ): Set<AdvanceRawImpressionUploadCorrectionCandidateRequest.Action> =
    when (state) {
      RawImpressionUploadCorrectionCandidate.State.STATE_PENDING ->
        setOf(
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      RawImpressionUploadCorrectionCandidate.State.STATE_PLANNED ->
        setOf(
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_CORRECT,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REJECT,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.SUPERSEDE,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      RawImpressionUploadCorrectionCandidate.State.STATE_APPROVED ->
        setOf(
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.START_HEALING,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      RawImpressionUploadCorrectionCandidate.State.STATE_HEALING ->
        setOf(
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.COMPLETE,
          AdvanceRawImpressionUploadCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      RawImpressionUploadCorrectionCandidate.State.STATE_COMPLETE,
      RawImpressionUploadCorrectionCandidate.State.STATE_NO_REPLACEMENT,
      RawImpressionUploadCorrectionCandidate.State.STATE_REJECTED,
      RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED,
      RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED -> emptySet()
      RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED,
      RawImpressionUploadCorrectionCandidate.State.UNRECOGNIZED ->
        error("Invalid stored raw-impression-upload-correction-candidate state")
    }

  private fun isIdempotentCreate(
    result: RawImpressionUploadCorrectionCandidateResult,
    request: CreateRawImpressionUploadCorrectionCandidateRequest,
  ): Boolean {
    val existing = result.rawImpressionUploadCorrectionCandidate
    val requested = request.rawImpressionUploadCorrectionCandidate
    return result.createRequestId == request.requestId &&
      existing.rawImpressionUploadCorrectionCandidateId ==
        request.rawImpressionUploadCorrectionCandidateId &&
      existing.rawImpressionUploadResourceId == requested.rawImpressionUploadResourceId &&
      existing.classification == requested.classification &&
      existing.priorManifestDigest == requested.priorManifestDigest &&
      existing.currentManifestDigest == requested.currentManifestDigest &&
      existing.expireTime == requested.expireTime
  }

  private fun isIdempotentAdvance(
    result: RawImpressionUploadCorrectionCandidateResult,
    request: AdvanceRawImpressionUploadCorrectionCandidateRequest,
    requestFingerprint: ByteString,
  ): Boolean {
    if (
      result.rawImpressionUploadCorrectionCandidate.rawImpressionUploadCorrectionCandidateId !=
        request.rawImpressionUploadCorrectionCandidateId
    ) {
      return false
    }
    val requestIndex = result.advanceRequestIds.indexOf(request.requestId)
    return requestIndex >= 0 &&
      result.advanceRequestFingerprints.getOrNull(requestIndex) == requestFingerprint
  }

  private fun AdvanceRawImpressionUploadCorrectionCandidateRequest.fingerprint(): ByteString =
    MessageDigest.getInstance("SHA-256").digest(toByteArray()).toByteString()

  private fun requireField(value: String, field: String) {
    if (value.isEmpty()) invalid("$field is required")
  }

  private fun requireUuid(value: String, field: String) {
    if (runCatching { UUID.fromString(value) }.isFailure) invalid("$field must be a UUID")
  }

  private fun invalid(description: String): Nothing =
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException()

  private fun notFound(candidateId: String) =
    Status.NOT_FOUND.withDescription(
        "RawImpressionUploadCorrectionCandidate $candidateId not found"
      )
      .asRuntimeException()

  private fun alreadyExists(candidateId: String) =
    Status.ALREADY_EXISTS.withDescription(
        "RawImpressionUploadCorrectionCandidate $candidateId already exists"
      )
      .asRuntimeException()

  private fun requestIdAlreadyUsed() =
    Status.ALREADY_EXISTS.withDescription("request_id was already used").asRuntimeException()

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MANIFEST_DIGEST_SIZE_BYTES = 32
    private const val MAX_PAGE_SIZE = 100
  }
}
