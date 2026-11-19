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

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt as PublicPools
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt as PublicModels
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt as PublicLabels
import org.wfanet.measurement.edpaggregator.v1alpha.commitRawImpressionUploadModelLineMergedOutputRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.outputBlobReference
import org.wfanet.measurement.internal.edpaggregator.CommitRawImpressionUploadModelLineMergedOutputRequest
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt as InternalPools
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt as InternalModels
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt as InternalLabels
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.vidLabelingJob

class CommittedOutputServiceTest {
  private val poolService =
    object : InternalPools.PoolAssignmentJobServiceCoroutineImplBase() {
      var request: MarkPoolAssignmentJobSucceededRequest? = null

      override suspend fun markPoolAssignmentJobSucceeded(
        request: MarkPoolAssignmentJobSucceededRequest
      ): MarkPoolAssignmentJobSucceededResponse {
        this.request = request
        return markPoolAssignmentJobSucceededResponse {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
            poolAssignmentJobResourceId = request.poolAssignmentJobResourceId
            cmmsModelLine = "modelProviders/provider/modelSuites/suite/modelLines/line"
            state =
              org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState
                .POOL_ASSIGNMENT_STATE_SUCCEEDED
            outputManifest = request.outputManifest
            lastShard = true
          }
        }
      }
    }
  private val labelService =
    object : InternalLabels.VidLabelingJobServiceCoroutineImplBase() {
      var request: MarkVidLabelingJobSucceededRequest? = null

      override suspend fun markVidLabelingJobSucceeded(
        request: MarkVidLabelingJobSucceededRequest
      ): MarkVidLabelingJobSucceededResponse {
        this.request = request
        return markVidLabelingJobSucceededResponse {
          vidLabelingJob = vidLabelingJob {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
            vidLabelingJobResourceId = request.vidLabelingJobResourceId
            cmmsModelLines += "modelProviders/provider/modelSuites/suite/modelLines/line"
            rawImpressionUploadFiles +=
              "dataProviders/provider/rawImpressionUploads/upload/files/file"
            state =
              org.wfanet.measurement.internal.edpaggregator.VidLabelingState
                .VID_LABELING_STATE_SUCCEEDED
            outputManifest = request.outputManifest
          }
        }
      }
    }
  private val modelService =
    object : InternalModels.RawImpressionUploadModelLineServiceCoroutineImplBase() {
      var request: CommitRawImpressionUploadModelLineMergedOutputRequest? = null

      override suspend fun commitRawImpressionUploadModelLineMergedOutput(
        request: CommitRawImpressionUploadModelLineMergedOutputRequest
      ): RawImpressionUploadModelLine {
        this.request = request
        return rawImpressionUploadModelLine {
          dataProviderResourceId = request.dataProviderResourceId
          rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
          rawImpressionUploadModelLineResourceId = request.rawImpressionUploadModelLineResourceId
          cmmsModelLine = "modelProviders/provider/modelSuites/suite/modelLines/line"
          state =
            org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
          mergedOutputManifest = request.outputManifest
        }
      }
    }
  private val internalServer = GrpcTestServerRule {
    addService(poolService)
    addService(labelService)
    addService(modelService)
  }
  private val publicServer = GrpcTestServerRule {
    addService(
      PoolAssignmentJobService(
        InternalPools.PoolAssignmentJobServiceCoroutineStub(internalServer.channel),
        EmptyCoroutineContext,
      )
    )
    addService(
      VidLabelingJobService(
        InternalLabels.VidLabelingJobServiceCoroutineStub(internalServer.channel),
        EmptyCoroutineContext,
      )
    )
    addService(
      RawImpressionUploadModelLineService(
        InternalModels.RawImpressionUploadModelLineServiceCoroutineStub(internalServer.channel),
        EmptyCoroutineContext,
      )
    )
  }
  @get:Rule val servers = RuleChain.outerRule(internalServer).around(publicServer)
  private val upload = "dataProviders/provider/rawImpressionUploads/upload"
  private val reference = outputBlobReference {
    blobUri = "gs://bucket/attempt/manifest"
    blobGeneration = 3L
  }

  @Test
  fun `public empty pool completion forwards the manifest without fabricating a DEK`() =
    runBlocking<Unit> {
      val response =
        PublicPools.PoolAssignmentJobServiceCoroutineStub(publicServer.channel)
          .markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
              name = "$upload/poolAssignmentJobs/job"
              etag = "etag"
              requestId = UUID.randomUUID().toString()
              outputManifest = reference
            }
          )
      assertThat(poolService.request?.hasEncryptedDek()).isFalse()
      assertThat(poolService.request?.outputManifest?.blobGeneration).isEqualTo(3L)
      assertThat(response.poolAssignmentJob.outputManifest).isEqualTo(reference)
      assertThat(response.poolAssignmentJob.lastShard).isTrue()
    }

  @Test
  fun `public labeling completion forwards and returns the selected manifest`() =
    runBlocking<Unit> {
      val response =
        PublicLabels.VidLabelingJobServiceCoroutineStub(publicServer.channel)
          .markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest {
              name = "$upload/vidLabelingJobs/job"
              etag = "etag"
              requestId = UUID.randomUUID().toString()
              outputManifest = reference
            }
          )
      assertThat(labelService.request?.outputManifest?.blobUri).isEqualTo(reference.blobUri)
      assertThat(response.vidLabelingJob.outputManifest).isEqualTo(reference)
    }

  @Test
  fun `public merged output commit forwards exact ownership and generation`() =
    runBlocking<Unit> {
      val response =
        PublicModels.RawImpressionUploadModelLineServiceCoroutineStub(publicServer.channel)
          .commitRawImpressionUploadModelLineMergedOutput(
            commitRawImpressionUploadModelLineMergedOutputRequest {
              name = "$upload/rawImpressionUploadModelLines/line"
              etag = "etag"
              requestId = UUID.randomUUID().toString()
              outputManifest = reference
            }
          )
      assertThat(modelService.request?.rawImpressionUploadResourceId).isEqualTo("upload")
      assertThat(modelService.request?.outputManifest?.blobGeneration).isEqualTo(3L)
      assertThat(response.mergedOutputManifest).isEqualTo(reference)
    }

  @Test
  fun `public labeling completion rejects invalid generations before invoking the internal service`() =
    runBlocking<Unit> {
      val e =
        assertFailsWith<StatusException> {
          PublicLabels.VidLabelingJobServiceCoroutineStub(publicServer.channel)
            .markVidLabelingJobSucceeded(
              markVidLabelingJobSucceededRequest {
                name = "$upload/vidLabelingJobs/job"
                etag = "etag"
                requestId = UUID.randomUUID().toString()
                outputManifest = outputBlobReference { blobUri = reference.blobUri }
              }
            )
        }
      assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(labelService.request).isNull()
    }
}
