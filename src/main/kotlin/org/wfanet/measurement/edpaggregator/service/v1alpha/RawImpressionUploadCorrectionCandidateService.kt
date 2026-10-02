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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesPageToken as InternalListCandidatesPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesRequestKt as InternalListCandidatesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadCorrectionCandidatesResponse as InternalListCandidatesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub as InternalCandidateStub
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadCorrectionCandidateRequest as internalGetCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadCorrectionCandidatesRequest as internalListCandidatesRequest

/** Public API adapter for raw-impression upload correction candidates. */
class RawImpressionUploadCorrectionCandidateService(
  private val internalCandidateStub: InternalCandidateStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase(coroutineContext) {

  override suspend fun getRawImpressionUploadCorrectionCandidate(
    request: GetRawImpressionUploadCorrectionCandidateRequest
  ): RawImpressionUploadCorrectionCandidate {
    if (request.name.isEmpty()) required("name")
    val key = RawImpressionUploadCorrectionCandidateKey.fromName(request.name) ?: invalid("name")
    val candidate =
      try {
        internalCandidateStub.getRawImpressionUploadCorrectionCandidate(
          internalGetCandidateRequest {
            dataProviderResourceId = key.dataProviderId
            rawImpressionUploadCorrectionCandidateId = key.rawImpressionUploadCorrectionCandidateId
          }
        )
      } catch (e: StatusException) {
        throw translateCandidateError(e)
      }
    return candidate.toPublic(includeDifferences = true)
  }

  override suspend fun listRawImpressionUploadCorrectionCandidates(
    request: ListRawImpressionUploadCorrectionCandidatesRequest
  ): ListRawImpressionUploadCorrectionCandidatesResponse {
    if (request.parent.isEmpty()) required("parent")
    val dataProviderKey = DataProviderKey.fromName(request.parent) ?: invalid("parent")
    if (request.pageSize < 0) invalid("page_size")
    request.filter.stateInList.forEach {
      if (
        it == RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED ||
          it == RawImpressionUploadCorrectionCandidate.State.UNRECOGNIZED
      ) {
        invalid("filter.state_in")
      }
    }
    request.filter.classificationInList.forEach {
      if (
        it == RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED ||
          it == RawImpressionUploadCorrectionCandidate.Classification.UNRECOGNIZED
      ) {
        invalid("filter.classification_in")
      }
    }
    val internalPageToken =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListCandidatesPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    val response: InternalListCandidatesResponse =
      try {
        internalCandidateStub.listRawImpressionUploadCorrectionCandidates(
          internalListCandidatesRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            pageSize =
              if (request.pageSize == 0) DEFAULT_PAGE_SIZE
              else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
            if (internalPageToken != null) pageToken = internalPageToken
            if (request.hasFilter()) {
              filter =
                InternalListCandidatesRequestKt.filter {
                  stateIn += request.filter.stateInList.map { it.toInternal() }
                  classificationIn += request.filter.classificationInList.map { it.toInternal() }
                }
            }
          }
        )
      } catch (e: StatusException) {
        throw translateCandidateError(e)
      }
    return listRawImpressionUploadCorrectionCandidatesResponse {
      rawImpressionUploadCorrectionCandidates +=
        response.rawImpressionUploadCorrectionCandidatesList.map { it.toPublic() }
      if (response.hasNextPageToken()) {
        nextPageToken = response.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  private fun InternalCandidate.toPublic(
    includeDifferences: Boolean = false
  ): RawImpressionUploadCorrectionCandidate = rawImpressionUploadCorrectionCandidate {
    name =
      RawImpressionUploadCorrectionCandidateKey(
          this@toPublic.dataProviderResourceId,
          this@toPublic.rawImpressionUploadCorrectionCandidateId,
        )
        .toName()
    rawImpressionUpload =
      RawImpressionUploadKey(
          this@toPublic.dataProviderResourceId,
          this@toPublic.rawImpressionUploadResourceId,
        )
        .toName()
    classification = this@toPublic.classification.toPublic()
    priorManifestDigest = this@toPublic.priorManifestDigest
    currentManifestDigest = this@toPublic.currentManifestDigest
    state = this@toPublic.state.toPublic()
    decision = this@toPublic.decision.toPublic()
    if (supersedingRawImpressionUploadCorrectionCandidateId.isNotEmpty()) {
      supersedingRawImpressionUploadCorrectionCandidate =
        RawImpressionUploadCorrectionCandidateKey(
            this@toPublic.dataProviderResourceId,
            this@toPublic.supersedingRawImpressionUploadCorrectionCandidateId,
          )
          .toName()
    }
    if (uploadHealingOperationId.isNotEmpty()) {
      uploadHealingOperation =
        UploadHealingOperationKey(
            this@toPublic.dataProviderResourceId,
            this@toPublic.uploadHealingOperationId,
          )
          .toName()
    }
    expireTime = this@toPublic.expireTime
    createTime = this@toPublic.createTime
    updateTime = this@toPublic.updateTime
    etag = this@toPublic.etag
    if (includeDifferences) {
      manifestDifferences +=
        this@toPublic.manifestComparison.differencesList.map { difference ->
          RawImpressionUploadCorrectionCandidateKt.manifestDifference {
            type = difference.type.toPublic()
            val prior = difference.prior.takeIf { difference.hasPrior() }
            val current = difference.current.takeIf { difference.hasCurrent() }
            blobUri = prior?.blobUri ?: checkNotNull(current).blobUri
            if (prior != null) {
              priorBlobGeneration = prior.blobGeneration
              priorEventDate = prior.eventDate
              historicalOwnerRawImpressionUpload =
                RawImpressionUploadKey(
                    this@toPublic.dataProviderResourceId,
                    prior.ownerRawImpressionUploadResourceId,
                  )
                  .toName()
            }
            if (current != null) {
              currentBlobGeneration = current.blobGeneration
              currentEventDate = current.eventDate
            }
          }
        }
    }
  }

  private fun translateCandidateError(e: StatusException) =
    when (e.status.code) {
      Status.Code.NOT_FOUND ->
        Status.NOT_FOUND.withDescription("RawImpressionUploadCorrectionCandidate not found")
          .withCause(e)
          .asRuntimeException()
      Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withCause(e).asRuntimeException()
      Status.Code.FAILED_PRECONDITION ->
        Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
      Status.Code.ABORTED -> Status.ABORTED.withCause(e).asRuntimeException()
      else -> Status.INTERNAL.withCause(e).asRuntimeException()
    }

  private fun required(field: String): Nothing =
    throw RequiredFieldNotSetException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

  private fun invalid(field: String): Nothing =
    throw InvalidFieldValueException(field).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

private fun InternalCandidate.Classification.toPublic() =
  when (this) {
    InternalCandidate.Classification.CLASSIFICATION_EDITED ->
      RawImpressionUploadCorrectionCandidate.Classification.EDITED
    InternalCandidate.Classification.CLASSIFICATION_REMOVED ->
      RawImpressionUploadCorrectionCandidate.Classification.REMOVED
    InternalCandidate.Classification.CLASSIFICATION_MIXED ->
      RawImpressionUploadCorrectionCandidate.Classification.MIXED
    InternalCandidate.Classification.CLASSIFICATION_UNSPECIFIED,
    InternalCandidate.Classification.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED
  }

private fun InternalCandidate.ManifestDifference.Type.toPublic() =
  when (this) {
    InternalCandidate.ManifestDifference.Type.TYPE_ADDED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED
    InternalCandidate.ManifestDifference.Type.TYPE_EDITED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED
    InternalCandidate.ManifestDifference.Type.TYPE_REMOVED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED
    InternalCandidate.ManifestDifference.Type.TYPE_UNSPECIFIED,
    InternalCandidate.ManifestDifference.Type.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_UNSPECIFIED
  }

private fun RawImpressionUploadCorrectionCandidate.Classification.toInternal() =
  when (this) {
    RawImpressionUploadCorrectionCandidate.Classification.EDITED ->
      InternalCandidate.Classification.CLASSIFICATION_EDITED
    RawImpressionUploadCorrectionCandidate.Classification.REMOVED ->
      InternalCandidate.Classification.CLASSIFICATION_REMOVED
    RawImpressionUploadCorrectionCandidate.Classification.MIXED ->
      InternalCandidate.Classification.CLASSIFICATION_MIXED
    RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_UNSPECIFIED,
    RawImpressionUploadCorrectionCandidate.Classification.UNRECOGNIZED ->
      InternalCandidate.Classification.CLASSIFICATION_UNSPECIFIED
  }

private fun InternalCandidate.State.toPublic() =
  when (this) {
    InternalCandidate.State.STATE_PENDING -> RawImpressionUploadCorrectionCandidate.State.PENDING
    InternalCandidate.State.STATE_PLANNED -> RawImpressionUploadCorrectionCandidate.State.PLANNED
    InternalCandidate.State.STATE_APPROVED -> RawImpressionUploadCorrectionCandidate.State.APPROVED
    InternalCandidate.State.STATE_HEALING -> RawImpressionUploadCorrectionCandidate.State.HEALING
    InternalCandidate.State.STATE_COMPLETE -> RawImpressionUploadCorrectionCandidate.State.COMPLETE
    InternalCandidate.State.STATE_NO_REPLACEMENT ->
      RawImpressionUploadCorrectionCandidate.State.NO_REPLACEMENT
    InternalCandidate.State.STATE_REJECTED -> RawImpressionUploadCorrectionCandidate.State.REJECTED
    InternalCandidate.State.STATE_SUPERSEDED ->
      RawImpressionUploadCorrectionCandidate.State.SUPERSEDED
    InternalCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED ->
      RawImpressionUploadCorrectionCandidate.State.MANUAL_INTERVENTION_REQUIRED
    InternalCandidate.State.STATE_UNSPECIFIED,
    InternalCandidate.State.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED
  }

private fun RawImpressionUploadCorrectionCandidate.State.toInternal() =
  when (this) {
    RawImpressionUploadCorrectionCandidate.State.PENDING -> InternalCandidate.State.STATE_PENDING
    RawImpressionUploadCorrectionCandidate.State.PLANNED -> InternalCandidate.State.STATE_PLANNED
    RawImpressionUploadCorrectionCandidate.State.APPROVED -> InternalCandidate.State.STATE_APPROVED
    RawImpressionUploadCorrectionCandidate.State.HEALING -> InternalCandidate.State.STATE_HEALING
    RawImpressionUploadCorrectionCandidate.State.COMPLETE -> InternalCandidate.State.STATE_COMPLETE
    RawImpressionUploadCorrectionCandidate.State.NO_REPLACEMENT ->
      InternalCandidate.State.STATE_NO_REPLACEMENT
    RawImpressionUploadCorrectionCandidate.State.REJECTED -> InternalCandidate.State.STATE_REJECTED
    RawImpressionUploadCorrectionCandidate.State.SUPERSEDED ->
      InternalCandidate.State.STATE_SUPERSEDED
    RawImpressionUploadCorrectionCandidate.State.MANUAL_INTERVENTION_REQUIRED ->
      InternalCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED
    RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED,
    RawImpressionUploadCorrectionCandidate.State.UNRECOGNIZED ->
      InternalCandidate.State.STATE_UNSPECIFIED
  }

private fun InternalCandidate.Decision.toPublic() =
  when (this) {
    InternalCandidate.Decision.DECISION_CORRECT ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT
    InternalCandidate.Decision.DECISION_NO_REPLACEMENT ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_NO_REPLACEMENT
    InternalCandidate.Decision.DECISION_UNSPECIFIED,
    InternalCandidate.Decision.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED
  }
