/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.internal

import io.grpc.BindableService
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt

/** Edp Aggregator internal API services. */
data class Services(
  val requisitionMetadata:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase,
  val impressionMetadata:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase,
  val rawImpressionUpload:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase,
  val rawImpressionUploadFile:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase,
  val rawImpressionUploadModelLine:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase,
  val vidLabelingJob: VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase,
  val rankerJob: RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase,
  val rankIndexBlob: RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase,
  val poolAssignmentJob: PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase,
) {
  fun toList(): List<BindableService> =
    listOf(
      requisitionMetadata,
      impressionMetadata,
      rawImpressionUpload,
      rawImpressionUploadFile,
      rawImpressionUploadModelLine,
      vidLabelingJob,
      rankerJob,
      rankIndexBlob,
      poolAssignmentJob,
    )
}
