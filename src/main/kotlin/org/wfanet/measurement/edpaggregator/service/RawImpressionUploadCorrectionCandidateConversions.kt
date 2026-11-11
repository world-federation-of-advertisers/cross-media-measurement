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

package org.wfanet.measurement.edpaggregator.service

import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateKt
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate

fun InternalCandidate.toPublic(
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
            priorOutputSourceRawImpressionUpload =
              RawImpressionUploadKey(
                  this@toPublic.dataProviderResourceId,
                  prior.outputSourceRawImpressionUploadResourceId,
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
    InternalCandidate.ManifestDifference.Type.TYPE_EVENT_DATE_CHANGED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EVENT_DATE_CHANGED
    InternalCandidate.ManifestDifference.Type.TYPE_UNSPECIFIED,
    InternalCandidate.ManifestDifference.Type.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.TYPE_UNSPECIFIED
  }

private fun InternalCandidate.State.toPublic() =
  when (this) {
    InternalCandidate.State.STATE_PENDING -> RawImpressionUploadCorrectionCandidate.State.PENDING
    InternalCandidate.State.STATE_ASSIGNED -> RawImpressionUploadCorrectionCandidate.State.ASSIGNED
    InternalCandidate.State.STATE_COMPLETE -> RawImpressionUploadCorrectionCandidate.State.COMPLETE
    InternalCandidate.State.STATE_SUPERSEDED ->
      RawImpressionUploadCorrectionCandidate.State.SUPERSEDED
    InternalCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED ->
      RawImpressionUploadCorrectionCandidate.State.MANUAL_INTERVENTION_REQUIRED
    InternalCandidate.State.STATE_UNSPECIFIED,
    InternalCandidate.State.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.State.STATE_UNSPECIFIED
  }

private fun InternalCandidate.Decision.toPublic() =
  when (this) {
    InternalCandidate.Decision.DECISION_APPLY_CANDIDATE ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
    InternalCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
    InternalCandidate.Decision.DECISION_UNSPECIFIED,
    InternalCandidate.Decision.UNRECOGNIZED ->
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED
  }
