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

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineApiConversions
import org.wfanet.measurement.edpaggregator.service.VidLabelingMachineAuthorizer
import org.wfanet.measurement.edpaggregator.service.toPublic as toPublicResource
import org.wfanet.measurement.edpaggregator.v1alpha.ActivateRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ActivateRawImpressionUploadResponse
import org.wfanet.measurement.edpaggregator.v1alpha.QuarantineRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QuarantineRawImpressionUploadResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ResolveRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ResolveRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingCorrectionDetectionServiceGrpcKt.VidLabelingCorrectionDetectionServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.activateRawImpressionUploadResponse
import org.wfanet.measurement.edpaggregator.v1alpha.quarantineRawImpressionUploadResponse
import org.wfanet.measurement.edpaggregator.v1alpha.registerRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.edpaggregator.v1alpha.resolveRawImpressionUploadCorrectionCandidateResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub

/** System API adapter for authenticated dispatcher correction detection. */
class VidLabelingCorrectionDetectionService(
  private val internalStub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub,
  private val authorizer: VidLabelingMachineAuthorizer,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : VidLabelingCorrectionDetectionServiceCoroutineImplBase(coroutineContext) {
  override suspend fun quarantineRawImpressionUpload(
    request: QuarantineRawImpressionUploadRequest
  ): QuarantineRawImpressionUploadResponse =
    VidLabelingMachineApiServices.authorized(
      authorizer,
      VidLabelingMachineAuthorizer.Role.DISPATCHER,
    ) {
      val internalRequest = VidLabelingMachineApiConversions.quarantineToInternal(request)
      val upload =
        rpcThrottlers.metadataWrite.onReady {
          internalStub.createQuarantinedRawImpressionUpload(internalRequest)
        }
      quarantineRawImpressionUploadResponse {
        rawImpressionUpload = upload.toPublicResource()
        rawImpressionUploadCorrectionCandidate =
          RawImpressionUploadCorrectionCandidateKey(
              upload.dataProviderResourceId,
              upload.correctionCandidateId,
            )
            .toName()
      }
    }

  override suspend fun activateRawImpressionUpload(
    request: ActivateRawImpressionUploadRequest
  ): ActivateRawImpressionUploadResponse =
    VidLabelingMachineApiServices.authorized(
      authorizer,
      VidLabelingMachineAuthorizer.Role.DISPATCHER,
    ) {
      val internalRequest = VidLabelingMachineApiConversions.activationToInternal(request)
      val upload =
        rpcThrottlers.metadataWrite.onReady {
          internalStub.activateQuarantinedRawImpressionUpload(internalRequest)
        }
      activateRawImpressionUploadResponse {
        rawImpressionUpload = upload.toPublicResource()
        rawImpressionUploadCorrectionCandidate =
          RawImpressionUploadCorrectionCandidateKey(
              upload.dataProviderResourceId,
              upload.correctionCandidateId,
            )
            .toName()
      }
    }

  override suspend fun registerRawImpressionUploadCorrectionCandidate(
    request: RegisterRawImpressionUploadCorrectionCandidateRequest
  ): RegisterRawImpressionUploadCorrectionCandidateResponse =
    VidLabelingMachineApiServices.authorized(
      authorizer,
      VidLabelingMachineAuthorizer.Role.DISPATCHER,
    ) {
      val internalRequest = VidLabelingMachineApiConversions.registrationToInternal(request)
      val result =
        rpcThrottlers.metadataWrite.onReady {
          internalStub.registerDetectedRawImpressionUploadCorrectionCandidate(internalRequest)
        }
      val candidate = result.rawImpressionUploadCorrectionCandidate
      registerRawImpressionUploadCorrectionCandidateResponse {
        rawImpressionUploadCorrectionCandidate = candidate.toPublicResource()
        manifestComparison =
          VidLabelingMachineApiConversions.comparisonToPublic(
            candidate.manifestComparison,
            VidLabelingMachineApiConversions.parent(request.parent),
          )
        newlyCreated = result.newlyCreated
      }
    }

  override suspend fun resolveRawImpressionUploadCorrectionCandidate(
    request: ResolveRawImpressionUploadCorrectionCandidateRequest
  ): ResolveRawImpressionUploadCorrectionCandidateResponse =
    VidLabelingMachineApiServices.authorized(
      authorizer,
      VidLabelingMachineAuthorizer.Role.DISPATCHER,
    ) {
      val internalRequest = VidLabelingMachineApiConversions.resolutionToInternal(request)
      val result =
        rpcThrottlers.metadataWrite.onReady {
          internalStub.resolveDetectedRawImpressionUploadCorrectionCandidate(internalRequest)
        }
      resolveRawImpressionUploadCorrectionCandidateResponse { newlyResolved = result.newlyResolved }
    }
}
