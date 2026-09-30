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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.CorrectionCandidateResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findCorrectionCandidateByAdvanceRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findCorrectionCandidateByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadHealingOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readCorrectionCandidates
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateCorrectionCandidate
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AdvanceCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.CorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.CorrectionCandidateServiceGrpcKt.CorrectionCandidateServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.CreateCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.GetCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ListCorrectionCandidatesPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.ListCorrectionCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.correctionCandidate
import org.wfanet.measurement.internal.edpaggregator.listCorrectionCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.listCorrectionCandidatesResponse

/** Persists VID-labeling correction candidates. */
class SpannerCorrectionCandidateService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : CorrectionCandidateServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createCorrectionCandidate(
    request: CreateCorrectionCandidateRequest
  ): CorrectionCandidate {
    validateCreateRequest(request)
    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createCorrectionCandidate"))
    try {
      transactionRunner.run { txn ->
        val existingById =
          txn.findCorrectionCandidate(request.dataProviderResourceId, request.correctionCandidateId)
        if (existingById != null) {
          if (isIdempotentCreate(existingById, request)) return@run
          throw alreadyExists(request.correctionCandidateId)
        }
        val existingByRequestId =
          txn.findCorrectionCandidateByCreateRequestId(
            request.dataProviderResourceId,
            request.requestId,
          )
        if (existingByRequestId != null) {
          if (isIdempotentCreate(existingByRequestId, request)) return@run
          throw alreadyExists(existingByRequestId.correctionCandidate.correctionCandidateId)
        }
        val upload =
          txn
            .getRawImpressionUploadByResourceId(
              request.dataProviderResourceId,
              request.correctionCandidate.rawImpressionUploadResourceId,
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
        txn.insertCorrectionCandidate(
          request.correctionCandidate.copy {
            dataProviderResourceId = request.dataProviderResourceId
            correctionCandidateId = request.correctionCandidateId
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
            txn.findCorrectionCandidate(
              request.dataProviderResourceId,
              request.correctionCandidateId,
            )
              ?: txn.findCorrectionCandidateByCreateRequestId(
                request.dataProviderResourceId,
                request.requestId,
              )
          }
        if (existing != null && isIdempotentCreate(existing, request)) {
          return existing.correctionCandidate
        }
        throw alreadyExists(request.correctionCandidateId)
      }
      throw e
    }
    return getCandidate(request.dataProviderResourceId, request.correctionCandidateId)
  }

  override suspend fun getCorrectionCandidate(
    request: GetCorrectionCandidateRequest
  ): CorrectionCandidate {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireField(request.correctionCandidateId, "correction_candidate_id")
    return getCandidate(request.dataProviderResourceId, request.correctionCandidateId)
  }

  override suspend fun listCorrectionCandidates(
    request: ListCorrectionCandidatesRequest
  ): ListCorrectionCandidatesResponse {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    if (request.pageSize < 0) invalid("page_size must be non-negative")
    request.filter.stateInList.forEach { state ->
      if (
        state == CorrectionCandidate.State.STATE_UNSPECIFIED ||
          state == CorrectionCandidate.State.UNRECOGNIZED
      ) {
        invalid("filter.state_in contains an unspecified state")
      }
    }
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val after = if (request.hasPageToken()) request.pageToken.after else null
    databaseClient.singleUse().use { txn ->
      val candidates: Flow<CorrectionCandidate> =
        txn
          .readCorrectionCandidates(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            after,
          )
          .map { it.correctionCandidate }
      return listCorrectionCandidatesResponse {
        candidates.collectIndexed { index, candidate ->
          if (index == pageSize) {
            nextPageToken = listCorrectionCandidatesPageToken {
              this.after =
                ListCorrectionCandidatesPageTokenKt.after {
                  createTime =
                    this@listCorrectionCandidatesResponse.correctionCandidates.last().createTime
                  correctionCandidateId =
                    this@listCorrectionCandidatesResponse.correctionCandidates
                      .last()
                      .correctionCandidateId
                }
            }
          } else {
            correctionCandidates += candidate
          }
        }
      }
    }
  }

  override suspend fun advanceCorrectionCandidate(
    request: AdvanceCorrectionCandidateRequest
  ): CorrectionCandidate {
    validateAdvanceRequest(request)
    val requestFingerprint = request.fingerprint()
    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=advanceCorrectionCandidate"))
    try {
      transactionRunner.run { txn ->
        val existingByRequestId =
          txn.findCorrectionCandidateByAdvanceRequestId(
            request.dataProviderResourceId,
            request.requestId,
          )
        if (existingByRequestId != null) {
          if (isIdempotentAdvance(existingByRequestId, request, requestFingerprint)) return@run
          throw requestIdAlreadyUsed()
        }
        val result =
          txn.findCorrectionCandidate(request.dataProviderResourceId, request.correctionCandidateId)
            ?: throw notFound(request.correctionCandidateId)
        val current = result.correctionCandidate
        if (current.etag != request.etag) {
          throw Status.ABORTED.withDescription("correction_candidate etag mismatch")
            .asRuntimeException()
        }
        val updated = transitionCandidate(txn, current, request)
        txn.updateCorrectionCandidate(
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
    return getCandidate(request.dataProviderResourceId, request.correctionCandidateId)
  }

  private suspend fun transitionCandidate(
    txn: AsyncDatabaseClient.TransactionContext,
    current: CorrectionCandidate,
    request: AdvanceCorrectionCandidateRequest,
  ): CorrectionCandidate {
    val allowedActions = allowedActions(current.state)
    if (request.action !in allowedActions) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "Action ${request.action} is not valid from ${current.state}"
        )
        .asRuntimeException()
    }
    val supersedingCandidate =
      if (request.action == AdvanceCorrectionCandidateRequest.Action.SUPERSEDE) {
        txn
          .findCorrectionCandidate(
            request.dataProviderResourceId,
            request.supersedingCorrectionCandidateId,
          )
          ?.correctionCandidate ?: throw notFound(request.supersedingCorrectionCandidateId)
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
          setOf(CorrectionCandidate.State.STATE_PENDING, CorrectionCandidate.State.STATE_PLANNED)
      ) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Superseding candidate must still be pending approval"
          )
          .asRuntimeException()
      }
    }
    return current.copy {
      when (request.action) {
        AdvanceCorrectionCandidateRequest.Action.ASSIGN_PLAN -> {
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
          state = CorrectionCandidate.State.STATE_PLANNED
          uploadHealingOperationId = request.uploadHealingOperationId
        }
        AdvanceCorrectionCandidateRequest.Action.APPROVE_CORRECT -> {
          state = CorrectionCandidate.State.STATE_APPROVED
          decision = CorrectionCandidate.Decision.DECISION_CORRECT
        }
        AdvanceCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT -> {
          state = CorrectionCandidate.State.STATE_APPROVED
          decision = CorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
        }
        AdvanceCorrectionCandidateRequest.Action.START_HEALING -> {
          state = CorrectionCandidate.State.STATE_HEALING
        }
        AdvanceCorrectionCandidateRequest.Action.COMPLETE -> {
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
            if (current.decision == CorrectionCandidate.Decision.DECISION_NO_REPLACEMENT) {
              CorrectionCandidate.State.STATE_NO_REPLACEMENT
            } else {
              CorrectionCandidate.State.STATE_COMPLETE
            }
        }
        AdvanceCorrectionCandidateRequest.Action.REJECT -> {
          state = CorrectionCandidate.State.STATE_REJECTED
        }
        AdvanceCorrectionCandidateRequest.Action.SUPERSEDE -> {
          val superseding = checkNotNull(supersedingCandidate)
          if (superseding.correctionCandidateId == current.correctionCandidateId) {
            invalid("a candidate cannot supersede itself")
          }
          state = CorrectionCandidate.State.STATE_SUPERSEDED
          supersedingCorrectionCandidateId = request.supersedingCorrectionCandidateId
        }
        AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION -> {
          state = CorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
        }
        AdvanceCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED,
        AdvanceCorrectionCandidateRequest.Action.UNRECOGNIZED -> error("action was validated")
      }
    }
  }

  private suspend fun getCandidate(
    dataProviderResourceId: String,
    correctionCandidateId: String,
  ): CorrectionCandidate =
    databaseClient.singleUse().use { txn ->
      txn
        .findCorrectionCandidate(dataProviderResourceId, correctionCandidateId)
        ?.correctionCandidate ?: throw notFound(correctionCandidateId)
    }

  private fun validateCreateRequest(request: CreateCorrectionCandidateRequest) {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.correctionCandidateId, "correction_candidate_id")
    requireUuid(request.requestId, "request_id")
    if (!request.hasCorrectionCandidate()) invalid("correction_candidate is required")
    val candidate = request.correctionCandidate
    requireField(candidate.rawImpressionUploadResourceId, "raw_impression_upload_resource_id")
    if (
      candidate.classification == CorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED ||
        candidate.classification == CorrectionCandidate.Classification.UNRECOGNIZED
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
        candidate.correctionCandidateId.isNotEmpty() ||
        candidate.state != CorrectionCandidate.State.STATE_UNSPECIFIED ||
        candidate.decision != CorrectionCandidate.Decision.DECISION_UNSPECIFIED ||
        candidate.supersedingCorrectionCandidateId.isNotEmpty() ||
        candidate.uploadHealingOperationId.isNotEmpty() ||
        candidate.hasCreateTime() ||
        candidate.hasUpdateTime() ||
        candidate.etag.isNotEmpty()
    ) {
      invalid("output-only candidate fields must be unset")
    }
  }

  private fun validateAdvanceRequest(request: AdvanceCorrectionCandidateRequest) {
    requireField(request.dataProviderResourceId, "data_provider_resource_id")
    requireUuid(request.correctionCandidateId, "correction_candidate_id")
    requireField(request.etag, "etag")
    requireUuid(request.requestId, "request_id")
    if (
      request.action == AdvanceCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED ||
        request.action == AdvanceCorrectionCandidateRequest.Action.UNRECOGNIZED
    ) {
      invalid("action is required")
    }
    when (request.action) {
      AdvanceCorrectionCandidateRequest.Action.ASSIGN_PLAN -> {
        requireUuid(request.uploadHealingOperationId, "upload_healing_operation_id")
        if (request.supersedingCorrectionCandidateId.isNotEmpty())
          invalid("superseding_correction_candidate_id must be empty")
      }
      AdvanceCorrectionCandidateRequest.Action.SUPERSEDE -> {
        requireUuid(request.supersedingCorrectionCandidateId, "superseding_correction_candidate_id")
        if (request.uploadHealingOperationId.isNotEmpty())
          invalid("upload_healing_operation_id must be empty")
      }
      AdvanceCorrectionCandidateRequest.Action.APPROVE_CORRECT,
      AdvanceCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT,
      AdvanceCorrectionCandidateRequest.Action.START_HEALING,
      AdvanceCorrectionCandidateRequest.Action.COMPLETE,
      AdvanceCorrectionCandidateRequest.Action.REJECT,
      AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION -> {
        if (request.uploadHealingOperationId.isNotEmpty())
          invalid("upload_healing_operation_id must be empty")
        if (request.supersedingCorrectionCandidateId.isNotEmpty())
          invalid("superseding_correction_candidate_id must be empty")
      }
      AdvanceCorrectionCandidateRequest.Action.ACTION_UNSPECIFIED,
      AdvanceCorrectionCandidateRequest.Action.UNRECOGNIZED -> error("action was validated")
    }
  }

  private fun allowedActions(
    state: CorrectionCandidate.State
  ): Set<AdvanceCorrectionCandidateRequest.Action> =
    when (state) {
      CorrectionCandidate.State.STATE_PENDING ->
        setOf(
          AdvanceCorrectionCandidateRequest.Action.ASSIGN_PLAN,
          AdvanceCorrectionCandidateRequest.Action.REJECT,
          AdvanceCorrectionCandidateRequest.Action.SUPERSEDE,
          AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      CorrectionCandidate.State.STATE_PLANNED ->
        setOf(
          AdvanceCorrectionCandidateRequest.Action.APPROVE_CORRECT,
          AdvanceCorrectionCandidateRequest.Action.APPROVE_NO_REPLACEMENT,
          AdvanceCorrectionCandidateRequest.Action.REJECT,
          AdvanceCorrectionCandidateRequest.Action.SUPERSEDE,
          AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      CorrectionCandidate.State.STATE_APPROVED ->
        setOf(
          AdvanceCorrectionCandidateRequest.Action.START_HEALING,
          AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      CorrectionCandidate.State.STATE_HEALING ->
        setOf(
          AdvanceCorrectionCandidateRequest.Action.COMPLETE,
          AdvanceCorrectionCandidateRequest.Action.REQUIRE_MANUAL_INTERVENTION,
        )
      CorrectionCandidate.State.STATE_COMPLETE,
      CorrectionCandidate.State.STATE_NO_REPLACEMENT,
      CorrectionCandidate.State.STATE_REJECTED,
      CorrectionCandidate.State.STATE_SUPERSEDED,
      CorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED -> emptySet()
      CorrectionCandidate.State.STATE_UNSPECIFIED,
      CorrectionCandidate.State.UNRECOGNIZED -> error("Invalid stored correction-candidate state")
    }

  private fun isIdempotentCreate(
    result: CorrectionCandidateResult,
    request: CreateCorrectionCandidateRequest,
  ): Boolean {
    val existing = result.correctionCandidate
    val requested = request.correctionCandidate
    return result.createRequestId == request.requestId &&
      existing.correctionCandidateId == request.correctionCandidateId &&
      existing.rawImpressionUploadResourceId == requested.rawImpressionUploadResourceId &&
      existing.classification == requested.classification &&
      existing.priorManifestDigest == requested.priorManifestDigest &&
      existing.currentManifestDigest == requested.currentManifestDigest &&
      existing.expireTime == requested.expireTime
  }

  private fun isIdempotentAdvance(
    result: CorrectionCandidateResult,
    request: AdvanceCorrectionCandidateRequest,
    requestFingerprint: ByteString,
  ): Boolean {
    if (result.correctionCandidate.correctionCandidateId != request.correctionCandidateId) {
      return false
    }
    val requestIndex = result.advanceRequestIds.indexOf(request.requestId)
    return requestIndex >= 0 &&
      result.advanceRequestFingerprints.getOrNull(requestIndex) == requestFingerprint
  }

  private fun AdvanceCorrectionCandidateRequest.fingerprint(): ByteString =
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
    Status.NOT_FOUND.withDescription("CorrectionCandidate $candidateId not found")
      .asRuntimeException()

  private fun alreadyExists(candidateId: String) =
    Status.ALREADY_EXISTS.withDescription("CorrectionCandidate $candidateId already exists")
      .asRuntimeException()

  private fun requestIdAlreadyUsed() =
    Status.ALREADY_EXISTS.withDescription("request_id was already used").asRuntimeException()

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MANIFEST_DIGEST_SIZE_BYTES = 32
    private const val MAX_PAGE_SIZE = 100
  }
}
