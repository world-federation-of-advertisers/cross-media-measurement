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

import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload as InternalRawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

/** Converts an internal [InternalRawImpressionUpload] to a public one. */
fun InternalRawImpressionUpload.toPublic(): RawImpressionUpload {
  val source = this
  return rawImpressionUpload {
    name =
      RawImpressionUploadKey(source.dataProviderResourceId, source.rawImpressionUploadResourceId)
        .toName()
    state = source.state.toPublic()
    doneBlobUri = source.doneBlobUri
    doneBlobGeneration = source.doneBlobGeneration
    if (source.hasDoneBlobCreateTime()) {
      doneBlobCreateTime = source.doneBlobCreateTime
    }
    registrationComplete = source.registrationComplete
    processingDeferred = source.processingDeferred
    if (source.evictionOperationId.isNotEmpty()) {
      uploadHealingOperation =
        UploadHealingOperationKey(source.dataProviderResourceId, source.evictionOperationId)
          .toName()
    }
    if (source.replacesRawImpressionUploadResourceId.isNotEmpty()) {
      replacesRawImpressionUpload =
        RawImpressionUploadKey(
            source.dataProviderResourceId,
            source.replacesRawImpressionUploadResourceId,
          )
          .toName()
    }
    createTime = source.createTime
    updateTime = source.updateTime
    etag = source.etag
  }
}

/** Converts an internal [RawImpressionUploadState] to a public [RawImpressionUpload.State]. */
fun RawImpressionUploadState.toPublic(): RawImpressionUpload.State {
  return when (this) {
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED ->
      RawImpressionUpload.State.CREATED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE -> RawImpressionUpload.State.ACTIVE
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED ->
      RawImpressionUpload.State.COMPLETED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED -> RawImpressionUpload.State.FAILED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED ->
      RawImpressionUpload.State.CORRECTION_REQUIRED
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_REMOVED_WITHOUT_REPLACEMENT ->
      RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT
    RawImpressionUploadState.UNRECOGNIZED,
    RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/** Converts a public [RawImpressionUpload.State] to an internal [RawImpressionUploadState]. */
fun RawImpressionUpload.State.toInternal(): RawImpressionUploadState {
  return when (this) {
    RawImpressionUpload.State.CREATED ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
    RawImpressionUpload.State.ACTIVE -> RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE
    RawImpressionUpload.State.COMPLETED ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
    RawImpressionUpload.State.FAILED -> RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED
    RawImpressionUpload.State.CORRECTION_REQUIRED ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CORRECTION_REQUIRED
    RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT ->
      RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_REMOVED_WITHOUT_REPLACEMENT
    RawImpressionUpload.State.UNRECOGNIZED,
    RawImpressionUpload.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
