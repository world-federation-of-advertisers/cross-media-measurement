// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.rawimpressions

import io.grpc.StatusException
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadFileKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.OutputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest

/** Resolves only complete database-selected attempts, retaining explicit legacy input scopes. */
class CommittedOutputResolver(
  private val jobs: VidLabelingJobServiceCoroutineStub,
  private val files: RawImpressionUploadFileServiceCoroutineStub,
  private val store: OutputManifestStore,
  private val throttler: Throttler,
) {
  data class Result(
    val labeledBlobs: List<OutputManifestRecord.LabeledBlob>,
    val legacyFiles: List<RawImpressionUploadFile>,
    val manifests: List<OutputBlobReference>,
    val hasCommittedManifests: Boolean,
    val rawImpressionUpload: String,
  )

  /** Resolves all successful job scopes for one upload/model, including empty publications. */
  suspend fun resolve(upload: String, modelLine: String, requireCompleted: Boolean = true): Result {
    val selected = mutableListOf<VidLabelingJob>()
    var token = ""
    do {
      val page = callMetadata {
        jobs.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = upload
            filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = modelLine }
            pageSize = 100
            pageToken = token
          }
        )
      }
      selected += page.vidLabelingJobsList
      token = page.nextPageToken
    } while (token.isNotEmpty())
    if (requireCompleted)
      check(selected.isNotEmpty() && selected.all { it.state == VidLabelingJob.State.SUCCEEDED }) {
        "Labeled output still has incomplete jobs"
      }
    val blobs = mutableListOf<OutputManifestRecord.LabeledBlob>()
    val legacy = linkedMapOf<String, RawImpressionUploadFile>()
    val manifests = mutableListOf<OutputBlobReference>()
    for (job in selected.filter { it.state == VidLabelingJob.State.SUCCEEDED }) {
      val inputs = linkedMapOf<String, RawImpressionUploadFile>()
      for (name in job.rawImpressionUploadFilesList) {
        check(RawImpressionUploadFileKey.fromName(name)?.parentKey?.toName() == upload) {
          "Job input belongs to another upload"
        }
        inputs[name] = callMetadata {
          files.getRawImpressionUploadFile(getRawImpressionUploadFileRequest { this.name = name })
        }
      }
      if (!job.hasOutputManifest()) {
        legacy.putAll(inputs)
        continue
      }
      val completed = mutableSetOf<Pair<String, String>>()
      val emitted = mutableSetOf<Pair<String, String>>()
      val jobBlobs = mutableListOf<OutputManifestRecord.LabeledBlob>()
      store.read(job.outputManifest).collect { record ->
        when {
          record.hasHeader() ->
            check(
              record.header.phase == OutputManifestRecord.Header.Phase.VID_LABELING &&
                record.header.owner == job.name &&
                record.header.rawImpressionUpload == upload &&
                record.header.modelLinesList.toSet() == job.cmmsModelLinesList.toSet()
            ) {
              "Manifest scope does not match its job"
            }
          record.hasFileCompleted() -> {
            val scope =
              record.fileCompleted.rawImpressionUploadFile to record.fileCompleted.modelLine
            check(scope.first in inputs && completed.add(scope)) {
              "Duplicate or foreign completed input scope"
            }
          }
          record.hasLabeledBlob() -> {
            val blob = record.labeledBlob
            val input =
              checkNotNull(inputs[blob.rawImpressionUploadFile]) {
                "Manifest input was not registered in this job"
              }
            check(
              blob.eventDate == input.eventDate &&
                emitted.add(blob.rawImpressionUploadFile to blob.modelLine)
            ) {
              "Duplicate or mismatched labeled output"
            }
            jobBlobs += blob
          }
          else -> error("Unexpected labeled-output manifest record")
        }
      }
      check(
        completed ==
          inputs.keys.flatMap { name -> job.cmmsModelLinesList.map { name to it } }.toSet()
      ) {
        "Manifest omits a completed input/model scope"
      }
      check(emitted.all { it in completed })
      blobs += jobBlobs.filter { it.modelLine == modelLine }
      manifests += job.outputManifest
    }
    return Result(blobs, legacy.values.toList(), manifests, manifests.isNotEmpty(), upload)
  }

  private suspend fun <T> callMetadata(block: suspend () -> T): T =
    throttler.onReady {
      try {
        block()
      } catch (e: StatusException) {
        throw IllegalStateException("Cannot resolve committed output metadata", e)
      }
    }
}
