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

import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.v1alpha.ActivateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QuarantineRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadManifestComparison
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadManifestComparisonKt
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ResolveRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.activateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadManifestComparison
import org.wfanet.measurement.edpaggregator.v1alpha.registerRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.resolveRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.CreateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload as InternalUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate as InternalCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateKt as InternalCandidateKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.activateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.createQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload as internalUpload
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadCorrectionCandidate as internalCandidate
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateRequest

/** Resource-name translations at the VID-labeling machine API boundary. */
object VidLabelingMachineApiConversions {
  fun quarantineToPublic(request: CreateQuarantinedRawImpressionUploadRequest) =
    quarantineRawImpressionUploadRequest {
      parent = DataProviderKey(request.dataProviderResourceId).toName()
      rawImpressionUploadCorrectionCandidateId = request.rawImpressionUploadCorrectionCandidateId
      doneBlobUri = request.rawImpressionUpload.doneBlobUri
      doneBlobGeneration = request.rawImpressionUpload.doneBlobGeneration
      if (request.rawImpressionUpload.hasDoneBlobCreateTime()) {
        doneBlobCreateTime = request.rawImpressionUpload.doneBlobCreateTime
      }
      requestId = request.requestId
    }

  fun quarantineToInternal(request: QuarantineRawImpressionUploadRequest) =
    createQuarantinedRawImpressionUploadRequest {
      dataProviderResourceId = parent(request.parent).dataProviderId
      rawImpressionUploadCorrectionCandidateId = request.rawImpressionUploadCorrectionCandidateId
      rawImpressionUpload = internalUpload {
        doneBlobUri = request.doneBlobUri
        doneBlobGeneration = request.doneBlobGeneration
        if (request.hasDoneBlobCreateTime()) doneBlobCreateTime = request.doneBlobCreateTime
      }
      requestId = request.requestId
    }

  fun activationToPublic(request: ActivateQuarantinedRawImpressionUploadRequest) =
    activateRawImpressionUploadRequest {
      name =
        RawImpressionUploadKey(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
          )
          .toName()
      sourceRawImpressionUpload =
        RawImpressionUploadKey(
            request.dataProviderResourceId,
            request.sourceRawImpressionUploadResourceId,
          )
          .toName()
      uploadHealingOperation =
        UploadHealingOperationKey(request.dataProviderResourceId, request.evictionOperationId)
          .toName()
      requestId = request.requestId
    }

  fun activationToInternal(
    request: ActivateRawImpressionUploadRequest
  ): ActivateQuarantinedRawImpressionUploadRequest {
    val uploadKey = upload(request.name)
    val source = upload(request.sourceRawImpressionUpload)
    val operation =
      requireNotNull(UploadHealingOperationKey.fromName(request.uploadHealingOperation)) {
        "upload_healing_operation must be a resource name"
      }
    require(source.parentKey == uploadKey.parentKey && operation.parentKey == uploadKey.parentKey) {
      "Activation resources must belong to one DataProvider"
    }
    return activateQuarantinedRawImpressionUploadRequest {
      dataProviderResourceId = uploadKey.dataProviderId
      rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
      sourceRawImpressionUploadResourceId = source.rawImpressionUploadId
      evictionOperationId = operation.uploadHealingOperationId
      requestId = request.requestId
    }
  }

  fun registrationToPublic(request: RegisterDetectedRawImpressionUploadCorrectionCandidateRequest) =
    registerRawImpressionUploadCorrectionCandidateRequest {
      val provider = DataProviderKey(request.dataProviderResourceId)
      val candidate = request.rawImpressionUploadCorrectionCandidate
      parent = provider.toName()
      rawImpressionUploadCorrectionCandidateId = request.rawImpressionUploadCorrectionCandidateId
      rawImpressionUpload =
        RawImpressionUploadKey(provider, candidate.rawImpressionUploadResourceId).toName()
      classification =
        RawImpressionUploadCorrectionCandidate.Classification.forNumber(
          candidate.classification.number
        )
      priorManifestDigest = candidate.priorManifestDigest
      currentManifestDigest = candidate.currentManifestDigest
      manifestComparison = comparisonToPublic(candidate.manifestComparison, provider)
      if (request.supersededRawImpressionUploadCorrectionCandidateId.isNotEmpty()) {
        supersededRawImpressionUploadCorrectionCandidate =
          RawImpressionUploadCorrectionCandidateKey(
              provider,
              request.supersededRawImpressionUploadCorrectionCandidateId,
            )
            .toName()
      }
      if (candidate.hasExpireTime()) expireTime = candidate.expireTime
      requestId = request.requestId
    }

  fun registrationToInternal(
    request: RegisterRawImpressionUploadCorrectionCandidateRequest
  ): RegisterDetectedRawImpressionUploadCorrectionCandidateRequest {
    val provider = parent(request.parent)
    val upload = upload(request.rawImpressionUpload)
    require(upload.parentKey == provider) {
      "raw_impression_upload belongs to another DataProvider"
    }
    val superseded =
      request.supersededRawImpressionUploadCorrectionCandidate.takeIf(String::isNotEmpty)?.let {
        candidate(it)
      }
    require(superseded == null || superseded.parentKey == provider) {
      "Superseded candidate belongs to another DataProvider"
    }
    return registerDetectedRawImpressionUploadCorrectionCandidateRequest {
      dataProviderResourceId = provider.dataProviderId
      rawImpressionUploadCorrectionCandidateId = request.rawImpressionUploadCorrectionCandidateId
      rawImpressionUploadCorrectionCandidate = internalCandidate {
        rawImpressionUploadResourceId = upload.rawImpressionUploadId
        classification = InternalCandidate.Classification.forNumber(request.classification.number)
        priorManifestDigest = request.priorManifestDigest
        currentManifestDigest = request.currentManifestDigest
        manifestComparison = comparisonToInternal(request.manifestComparison, provider)
        if (request.hasExpireTime()) expireTime = request.expireTime
      }
      supersededRawImpressionUploadCorrectionCandidateId =
        superseded?.rawImpressionUploadCorrectionCandidateId.orEmpty()
      requestId = request.requestId
    }
  }

  fun resolutionToPublic(request: ResolveDetectedRawImpressionUploadCorrectionCandidateRequest) =
    resolveRawImpressionUploadCorrectionCandidateRequest {
      name =
        RawImpressionUploadCorrectionCandidateKey(
            request.dataProviderResourceId,
            request.rawImpressionUploadCorrectionCandidateId,
          )
          .toName()
      requestId = request.requestId
    }

  fun resolutionToInternal(request: ResolveRawImpressionUploadCorrectionCandidateRequest) =
    resolveDetectedRawImpressionUploadCorrectionCandidateRequest {
      val key = candidate(request.name)
      dataProviderResourceId = key.dataProviderId
      rawImpressionUploadCorrectionCandidateId = key.rawImpressionUploadCorrectionCandidateId
      requestId = request.requestId
    }

  fun comparisonToPublic(
    comparison: InternalCandidate.ManifestComparison,
    provider: DataProviderKey,
  ): RawImpressionUploadManifestComparison = rawImpressionUploadManifestComparison {
    priorManifest += comparison.priorManifestList.map { entryToPublic(it, provider) }
    currentManifest += comparison.currentManifestList.map { entryToPublic(it, provider) }
    differences +=
      comparison.differencesList.map { difference ->
        RawImpressionUploadManifestComparisonKt.difference {
          type =
            RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.forNumber(
              difference.type.number
            )
          if (difference.hasPrior()) prior = entryToPublic(difference.prior, provider)
          if (difference.hasCurrent()) current = entryToPublic(difference.current, provider)
        }
      }
  }

  fun comparisonToInternal(
    comparison: RawImpressionUploadManifestComparison,
    provider: DataProviderKey,
  ): InternalCandidate.ManifestComparison =
    InternalCandidateKt.manifestComparison {
      priorManifest += comparison.priorManifestList.map { entryToInternal(it, provider) }
      currentManifest += comparison.currentManifestList.map { entryToInternal(it, provider) }
      differences +=
        comparison.differencesList.map { difference ->
          InternalCandidateKt.manifestDifference {
            type = InternalCandidate.ManifestDifference.Type.forNumber(difference.type.number)
            if (difference.hasPrior()) prior = entryToInternal(difference.prior, provider)
            if (difference.hasCurrent()) current = entryToInternal(difference.current, provider)
          }
        }
    }

  private fun entryToPublic(entry: InternalCandidate.ManifestEntry, provider: DataProviderKey) =
    RawImpressionUploadManifestComparisonKt.entry {
      blobUri = entry.blobUri
      blobGeneration = entry.blobGeneration
      if (entry.hasEventDate()) eventDate = entry.eventDate
      if (entry.outputSourceRawImpressionUploadResourceId.isNotEmpty()) {
        outputSourceRawImpressionUpload =
          RawImpressionUploadKey(provider, entry.outputSourceRawImpressionUploadResourceId).toName()
      }
    }

  private fun entryToInternal(
    entry: RawImpressionUploadManifestComparison.Entry,
    provider: DataProviderKey,
  ) =
    InternalCandidateKt.manifestEntry {
      blobUri = entry.blobUri
      blobGeneration = entry.blobGeneration
      if (entry.hasEventDate()) eventDate = entry.eventDate
      if (entry.outputSourceRawImpressionUpload.isNotEmpty()) {
        val owner = upload(entry.outputSourceRawImpressionUpload)
        require(owner.parentKey == provider) { "Manifest owner belongs to another DataProvider" }
        outputSourceRawImpressionUploadResourceId = owner.rawImpressionUploadId
      }
    }

  fun uploadToInternal(resource: RawImpressionUpload, candidateName: String): InternalUpload {
    val key = upload(resource.name)
    val candidate = candidateName.takeIf(String::isNotEmpty)?.let { candidate(it) }
    require(candidate == null || candidate.parentKey == key.parentKey)
    val operation =
      resource.uploadHealingOperation.takeIf(String::isNotEmpty)?.let {
        requireNotNull(UploadHealingOperationKey.fromName(it))
      }
    val predecessor =
      resource.replacesRawImpressionUpload.takeIf(String::isNotEmpty)?.let { upload(it) }
    require(operation == null || operation.parentKey == key.parentKey)
    require(predecessor == null || predecessor.parentKey == key.parentKey)
    return internalUpload {
      dataProviderResourceId = key.dataProviderId
      rawImpressionUploadResourceId = key.rawImpressionUploadId
      state = RawImpressionUploadState.forNumber(resource.state.number)
      doneBlobUri = resource.doneBlobUri
      doneBlobGeneration = resource.doneBlobGeneration
      if (resource.hasDoneBlobCreateTime()) doneBlobCreateTime = resource.doneBlobCreateTime
      registrationComplete = resource.registrationComplete
      processingDeferred = resource.processingDeferred
      correctionCandidateId = candidate?.rawImpressionUploadCorrectionCandidateId.orEmpty()
      evictionOperationId = operation?.uploadHealingOperationId.orEmpty()
      replacesRawImpressionUploadResourceId = predecessor?.rawImpressionUploadId.orEmpty()
      if (resource.hasCreateTime()) createTime = resource.createTime
      if (resource.hasUpdateTime()) updateTime = resource.updateTime
      etag = resource.etag
    }
  }

  fun candidateToInternal(
    resource: RawImpressionUploadCorrectionCandidate,
    comparison: RawImpressionUploadManifestComparison,
  ): InternalCandidate {
    val key = candidate(resource.name)
    val upload = upload(resource.rawImpressionUpload)
    require(upload.parentKey == key.parentKey)
    val operation =
      resource.uploadHealingOperation.takeIf(String::isNotEmpty)?.let {
        requireNotNull(UploadHealingOperationKey.fromName(it))
      }
    val superseding =
      resource.supersedingRawImpressionUploadCorrectionCandidate.takeIf(String::isNotEmpty)?.let {
        candidate(it)
      }
    require(operation == null || operation.parentKey == key.parentKey)
    require(superseding == null || superseding.parentKey == key.parentKey)
    return internalCandidate {
      dataProviderResourceId = key.dataProviderId
      rawImpressionUploadCorrectionCandidateId = key.rawImpressionUploadCorrectionCandidateId
      rawImpressionUploadResourceId = upload.rawImpressionUploadId
      classification = InternalCandidate.Classification.forNumber(resource.classification.number)
      priorManifestDigest = resource.priorManifestDigest
      currentManifestDigest = resource.currentManifestDigest
      state = InternalCandidate.State.forNumber(resource.state.number)
      decision = InternalCandidate.Decision.forNumber(resource.decision.number)
      supersedingRawImpressionUploadCorrectionCandidateId =
        superseding?.rawImpressionUploadCorrectionCandidateId.orEmpty()
      uploadHealingOperationId = operation?.uploadHealingOperationId.orEmpty()
      if (resource.hasExpireTime()) expireTime = resource.expireTime
      if (resource.hasCreateTime()) createTime = resource.createTime
      if (resource.hasUpdateTime()) updateTime = resource.updateTime
      etag = resource.etag
      manifestComparison = comparisonToInternal(comparison, key.parentKey)
    }
  }

  fun parent(name: String): DataProviderKey =
    requireNotNull(DataProviderKey.fromName(name)) { "parent must be a DataProvider resource name" }

  fun upload(name: String): RawImpressionUploadKey =
    requireNotNull(RawImpressionUploadKey.fromName(name)) {
      "Upload must be a RawImpressionUpload resource name"
    }

  fun candidate(name: String): RawImpressionUploadCorrectionCandidateKey =
    requireNotNull(RawImpressionUploadCorrectionCandidateKey.fromName(name)) {
      "Candidate must be a correction-candidate resource name"
    }
}
