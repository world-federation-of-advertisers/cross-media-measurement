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

package org.wfanet.measurement.edpaggregator.vidlabeling

import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineApiConversions
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingCorrectionDetectionServiceGrpcKt.VidLabelingCorrectionDetectionServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.ActivateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.CreateQuarantinedRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.RegisterDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.internal.edpaggregator.ResolveDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.registerDetectedRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.resolveDetectedRawImpressionUploadCorrectionCandidateResponse

/** Correction detection through the system gateway or an in-cluster internal channel. */
class CorrectionDetectionClient
private constructor(
  private val systemStub: VidLabelingCorrectionDetectionServiceCoroutineStub?,
  private val internalStub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub?,
) {
  constructor(stub: VidLabelingCorrectionDetectionServiceCoroutineStub) : this(stub, null)

  constructor(stub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub) : this(null, stub)

  suspend fun createQuarantinedRawImpressionUpload(
    request: CreateQuarantinedRawImpressionUploadRequest
  ): RawImpressionUpload {
    internalStub?.let {
      return it.createQuarantinedRawImpressionUpload(request)
    }
    val response =
      checkNotNull(systemStub)
        .quarantineRawImpressionUpload(VidLabelingMachineApiConversions.quarantineToPublic(request))
    return VidLabelingMachineApiConversions.uploadToInternal(
      response.rawImpressionUpload,
      response.rawImpressionUploadCorrectionCandidate,
    )
  }

  suspend fun activateQuarantinedRawImpressionUpload(
    request: ActivateQuarantinedRawImpressionUploadRequest
  ): RawImpressionUpload {
    internalStub?.let {
      return it.activateQuarantinedRawImpressionUpload(request)
    }
    val response =
      checkNotNull(systemStub)
        .activateRawImpressionUpload(VidLabelingMachineApiConversions.activationToPublic(request))
    return VidLabelingMachineApiConversions.uploadToInternal(
      response.rawImpressionUpload,
      response.rawImpressionUploadCorrectionCandidate,
    )
  }

  suspend fun registerDetectedRawImpressionUploadCorrectionCandidate(
    request: RegisterDetectedRawImpressionUploadCorrectionCandidateRequest
  ): RegisterDetectedRawImpressionUploadCorrectionCandidateResponse {
    internalStub?.let {
      return it.registerDetectedRawImpressionUploadCorrectionCandidate(request)
    }
    val response =
      checkNotNull(systemStub)
        .registerRawImpressionUploadCorrectionCandidate(
          VidLabelingMachineApiConversions.registrationToPublic(request)
        )
    return registerDetectedRawImpressionUploadCorrectionCandidateResponse {
      rawImpressionUploadCorrectionCandidate =
        VidLabelingMachineApiConversions.candidateToInternal(
          response.rawImpressionUploadCorrectionCandidate,
          response.manifestComparison,
        )
      newlyCreated = response.newlyCreated
    }
  }

  suspend fun resolveDetectedRawImpressionUploadCorrectionCandidate(
    request: ResolveDetectedRawImpressionUploadCorrectionCandidateRequest
  ): ResolveDetectedRawImpressionUploadCorrectionCandidateResponse {
    internalStub?.let {
      return it.resolveDetectedRawImpressionUploadCorrectionCandidate(request)
    }
    val response =
      checkNotNull(systemStub)
        .resolveRawImpressionUploadCorrectionCandidate(
          VidLabelingMachineApiConversions.resolutionToPublic(request)
        )
    return resolveDetectedRawImpressionUploadCorrectionCandidateResponse {
      newlyResolved = response.newlyResolved
    }
  }
}
