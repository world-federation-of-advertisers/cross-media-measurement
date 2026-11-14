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
package org.wfanet.measurement.edpaggregator.tools

import com.google.type.date
import io.grpc.netty.NettyServerBuilder
import java.time.LocalDate
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys

/** Registered raw inputs and owner rows backing managed completion markers in CLI tests. */
internal class ManagedDoneObjectTestServices(
  private val dataProvider: String,
  private val modelLines: List<String>,
  private val rawEventDate: LocalDate,
  private val inputUri: String,
) {
  val rawUpload = "$dataProvider/rawImpressionUploads/managed"
  val doneFileName =
    LabeledImpressionsBlobKeys.forDone(rawUpload, modelLines.first(), rawEventDate)
      .substringAfterLast('/')
  var ownerState = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING

  private val uploads: RawImpressionUploadServiceCoroutineImplBase = mockService {
    onBlocking { getRawImpressionUpload(any<GetRawImpressionUploadRequest>()) }
      .thenAnswer {
        check(it.getArgument<GetRawImpressionUploadRequest>(0).name == rawUpload)
        rawImpressionUpload {
          name = rawUpload
          state =
            if (ownerState == RawImpressionUploadModelLine.State.FAILED)
              RawImpressionUpload.State.FAILED
            else RawImpressionUpload.State.ACTIVE
          registrationComplete = true
        }
      }
  }
  private val models: RawImpressionUploadModelLineServiceCoroutineImplBase = mockService {
    onBlocking {
        listRawImpressionUploadModelLines(any<ListRawImpressionUploadModelLinesRequest>())
      }
      .thenAnswer {
        val request = it.getArgument<ListRawImpressionUploadModelLinesRequest>(0)
        check(request.parent == "$dataProvider/rawImpressionUploads/-")
        check(request.pageToken.isEmpty())
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines +=
            modelLines
              .filter {
                request.filter.cmmsModelLine.isEmpty() || it == request.filter.cmmsModelLine
              }
              .map { model ->
                rawImpressionUploadModelLine {
                  name = "$rawUpload/rawImpressionUploadModelLines/${model.substringAfterLast('/')}"
                  cmmsModelLine = model
                  state = ownerState
                  if (ownerState == RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING) {
                    pendingAvailabilityDates += date {
                      year = rawEventDate.year
                      month = rawEventDate.monthValue
                      day = rawEventDate.dayOfMonth
                    }
                  }
                  if (ownerState == RawImpressionUploadModelLine.State.FAILED) {
                    failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
                  }
                }
              }
        }
      }
  }
  private val files: RawImpressionUploadFileServiceCoroutineImplBase = mockService {
    onBlocking { listRawImpressionUploadFiles(any<ListRawImpressionUploadFilesRequest>()) }
      .thenAnswer {
        val request = it.getArgument<ListRawImpressionUploadFilesRequest>(0)
        val wildcard = request.parent == "$dataProvider/rawImpressionUploads/-"
        check(wildcard || request.parent == rawUpload)
        check(!wildcard || request.filter.blobUriInList.isNotEmpty())
        listRawImpressionUploadFilesResponse {
          if (!wildcard || inputUri in request.filter.blobUriInList) {
            rawImpressionUploadFiles += rawImpressionUploadFile {
              name = "$rawUpload/files/input"
              blobUri = inputUri
              sizeBytes = 1L
              blobGeneration = 1L
              this.eventDate = date {
                year = rawEventDate.year
                month = rawEventDate.monthValue
                day = rawEventDate.dayOfMonth
              }
            }
          }
        }
      }
  }
  private val jobs: VidLabelingJobServiceCoroutineImplBase = mockService {
    onBlocking { listVidLabelingJobs(any<ListVidLabelingJobsRequest>()) }
      .thenReturn(listVidLabelingJobsResponse {})
  }

  fun addTo(builder: NettyServerBuilder): NettyServerBuilder =
    builder.addService(uploads).addService(models).addService(files).addService(jobs)
}
