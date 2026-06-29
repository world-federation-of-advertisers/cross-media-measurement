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

import io.grpc.BindableService
import io.grpc.Channel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt as InternalImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt as InternalRankerJobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt as InternalRankIndexBlobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt as InternalRawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt as InternalRequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt as InternalVidLabelingJobServiceGrpcKt

data class Services(
  val requisitionMetadata: RequisitionMetadataServiceCoroutineImplBase,
  val impressionMetadata: ImpressionMetadataServiceCoroutineImplBase,
  val rawImpressionUpload: RawImpressionUploadServiceCoroutineImplBase,
  val vidLabelingJob: VidLabelingJobServiceCoroutineImplBase,
  val rankerJob: RankerJobServiceCoroutineImplBase,
  val rankIndexBlob: RankIndexBlobServiceCoroutineImplBase,
) {
  fun toList(): List<BindableService> =
    listOf(
      requisitionMetadata,
      impressionMetadata,
      rawImpressionUpload,
      vidLabelingJob,
      rankerJob,
      rankIndexBlob,
    )

  companion object {
    fun build(
      internalApiChannel: Channel,
      coroutineContext: CoroutineContext = EmptyCoroutineContext,
    ): Services {
      val internalRequisitionMetadataStub =
        InternalRequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
          internalApiChannel
        )
      val internalImpressionMetadataStub =
        InternalImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
          internalApiChannel
        )
      val internalUploadStub =
        InternalRawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
          internalApiChannel
        )
      val internalVidLabelingJobStub =
        InternalVidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(internalApiChannel)
      val internalRankerJobStub =
        InternalRankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(internalApiChannel)
      val internalRankIndexBlobStub =
        InternalRankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(internalApiChannel)

      return Services(
        RequisitionMetadataService(internalRequisitionMetadataStub, coroutineContext),
        ImpressionMetadataService(internalImpressionMetadataStub, coroutineContext),
        RawImpressionUploadService(internalUploadStub, coroutineContext),
        VidLabelingJobService(internalVidLabelingJobStub, coroutineContext),
        RankerJobService(internalRankerJobStub, coroutineContext),
        RankIndexBlobService(internalRankIndexBlobStub, coroutineContext),
      )
    }
  }
}
