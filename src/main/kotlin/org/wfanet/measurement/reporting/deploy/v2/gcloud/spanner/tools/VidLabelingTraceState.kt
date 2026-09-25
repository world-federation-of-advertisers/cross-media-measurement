// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

internal enum class ExpectedNodeDisposition {
  REQUIRED,
  AUTHORITATIVE_ONLY,
  NOT_APPLICABLE,
}

internal data class ExpectedTraceNode(
  val id: String,
  val modelLine: String?,
  val stage: String,
  val authoritativeState: String,
  val identifiers: Map<String, String>,
  val disposition: ExpectedNodeDisposition = ExpectedNodeDisposition.REQUIRED,
)

internal data class VidLabelingAuthoritativeGraph(
  val upload: RawImpressionUpload,
  val modelLines: List<RawImpressionUploadModelLine>,
  val nodes: List<ExpectedTraceNode>,
) {
  val correlationValues: Set<String>
    get() =
      (nodes.flatMap { it.identifiers.values } + upload.name)
        .filter { it.contains('/') || it.length >= 16 }
        .toSet()
}

internal fun interface VidLabelingStateResolver {
  suspend fun resolve(rawImpressionUpload: String): VidLabelingAuthoritativeGraph
}

internal fun interface VidLabelingFinalStateResolver {
  suspend fun resolve(
    upload: RawImpressionUpload,
    modelLines: List<RawImpressionUploadModelLine>,
    boundaryIdentities: Set<VidLabelingTraceAttributes.GcsObjectIdentity>,
  ): List<ExpectedTraceNode>
}

internal data class StoredObjectMetadata(val generation: Long)

internal class GcsKingdomFinalStateResolver(
  private val impressionMetadata: ImpressionMetadataServiceCoroutineStub,
  private val dataProviders: DataProvidersCoroutineStub,
  private val readObjectMetadata: suspend (String) -> StoredObjectMetadata?,
) : VidLabelingFinalStateResolver {
  override suspend fun resolve(
    upload: RawImpressionUpload,
    modelLines: List<RawImpressionUploadModelLine>,
    boundaryIdentities: Set<VidLabelingTraceAttributes.GcsObjectIdentity>,
  ): List<ExpectedTraceNode> {
    val dataProviderName = upload.name.substringBefore("/rawImpressionUploads/")
    val dataProvider =
      dataProviders.getDataProvider(
        GetDataProviderRequest.newBuilder().setName(dataProviderName).build()
      )
    val availabilityByModelLine =
      dataProvider.dataAvailabilityIntervalsList.associate { it.key to it.value }
    return buildList {
      val rootDone = upload.doneBlobUri.takeIf { it.isNotEmpty() }?.let { readObjectMetadata(it) }
      add(
        ExpectedTraceNode(
          "gcs:" + hash(upload.doneBlobUri),
          null,
          "upload_registration",
          if (rootDone?.generation == upload.doneBlobGeneration) "PUBLISHED" else "MISSING",
          mapOf(
            RAW_UPLOAD to upload.name,
            GCS_GENERATION to (rootDone?.generation?.toString() ?: "0"),
            GCS_PATH_HASH to hash(upload.doneBlobUri),
          ),
        )
      )
      for (modelLine in modelLines) {
        val metadataRows = listMetadata(dataProviderName, modelLine.cmmsModelLine)
        val rowsForUpload =
          metadataRows.filter { row ->
            val uri = doneUri(row.blobUri)
            val done = readObjectMetadata(uri)
            done != null &&
              VidLabelingTraceAttributes.gcsObjectIdentity(uri, done.generation) in
                boundaryIdentities
          }
        if (
          modelLine.state == RawImpressionUploadModelLine.State.COMPLETED && rowsForUpload.isEmpty()
        ) {
          add(
            ExpectedTraceNode(
              modelLine.name + ":impression_metadata",
              modelLine.cmmsModelLine,
              "data_availability_metadata",
              "MISSING",
              mapOf(MODEL_LINE to modelLine.cmmsModelLine),
            )
          )
          add(
            ExpectedTraceNode(
              modelLine.name + ":data_watcher",
              modelLine.cmmsModelLine,
              "data_watcher",
              "MISSING",
              mapOf(MODEL_LINE to modelLine.cmmsModelLine),
            )
          )
        }
        for (row in rowsForUpload) {
          val sidecarUri = row.blobUri
          val labeledUri = sidecarUri.removeSuffix(".metadata.binpb")
          val doneUri = doneUri(sidecarUri)
          val doneBlob = readObjectMetadata(doneUri)
          add(
            ExpectedTraceNode(
              row.name,
              modelLine.cmmsModelLine,
              "data_availability_metadata",
              row.state.name,
              mapOf(
                MODEL_LINE to modelLine.cmmsModelLine,
                IMPRESSION_METADATA to row.name,
                GCS_PATH_HASH to hash(sidecarUri),
              ),
            )
          )
          add(
            objectNode(
              labeledUri,
              modelLine.cmmsModelLine,
              "label",
              readObjectMetadata(labeledUri) != null,
            )
          )
          add(
            objectNode(
              sidecarUri,
              modelLine.cmmsModelLine,
              "label_finalize",
              readObjectMetadata(sidecarUri) != null,
            )
          )
          add(
            ExpectedTraceNode(
              "gcs:" + hash(doneUri),
              modelLine.cmmsModelLine,
              "data_watcher",
              if (doneBlob == null) "MISSING" else "PUBLISHED",
              mapOf(
                RAW_UPLOAD to upload.name,
                MODEL_LINE to modelLine.cmmsModelLine,
                GCS_GENERATION to (doneBlob?.generation?.toString() ?: "0"),
                GCS_PATH_HASH to hash(doneUri),
              ),
            )
          )
        }
        val availability = availabilityByModelLine[modelLine.cmmsModelLine]
        add(
          ExpectedTraceNode(
            modelLine.name + ":kingdom_availability",
            modelLine.cmmsModelLine,
            "data_availability_publish",
            if (availability == null) "MISSING" else "PUBLISHED",
            mapOf(MODEL_LINE to modelLine.cmmsModelLine),
            if (modelLine.state == RawImpressionUploadModelLine.State.COMPLETED) {
              ExpectedNodeDisposition.REQUIRED
            } else {
              ExpectedNodeDisposition.NOT_APPLICABLE
            },
          )
        )
      }
    }
  }

  private fun objectNode(uri: String, modelLine: String, stage: String, exists: Boolean) =
    ExpectedTraceNode(
      "gcs:" + hash(uri),
      modelLine,
      stage,
      if (exists) "PUBLISHED" else "MISSING",
      mapOf(MODEL_LINE to modelLine, GCS_PATH_HASH to hash(uri)),
    )

  private suspend fun listMetadata(parent: String, modelLine: String): List<ImpressionMetadata> {
    val result = mutableListOf<ImpressionMetadata>()
    var token = ""
    do {
      val filter = ListImpressionMetadataRequest.Filter.newBuilder().setModelLine(modelLine)
      val response =
        impressionMetadata.listImpressionMetadata(
          ListImpressionMetadataRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      result += response.impressionMetadataList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private fun doneUri(sidecarUri: String): String = sidecarUri.substringBeforeLast('/') + "/done"

  private fun hash(value: String): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString(
      ""
    ) {
      (it.toInt() and 0xff).toString(16).padStart(2, '0')
    }

  companion object {
    private const val RAW_UPLOAD = "xmm.edpa.raw_impression_upload.name"
    private const val MODEL_LINE = "xmm.model_line.name"
    private const val IMPRESSION_METADATA = "xmm.edpa.impression_metadata.name"
    private const val GCS_GENERATION = "xmm.gcs.object.generation"
    private const val GCS_PATH_HASH = "xmm.gcs.object.path_hash"
  }
}

internal class GrpcVidLabelingStateResolver(
  private val uploads: RawImpressionUploadServiceCoroutineStub,
  private val rawFiles: RawImpressionUploadFileServiceCoroutineStub,
  private val modelLines: RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolJobs: PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobs: RankerJobServiceCoroutineStub,
  private val labelingJobs: VidLabelingJobServiceCoroutineStub,
  private val rankBlobs: RankIndexBlobServiceCoroutineStub,
  private val workItems: WorkItemsCoroutineStub,
  private val workItemAttempts: WorkItemAttemptsCoroutineStub,
) : VidLabelingStateResolver {
  override suspend fun resolve(rawImpressionUpload: String): VidLabelingAuthoritativeGraph {
    val upload =
      uploads.getRawImpressionUpload(
        GetRawImpressionUploadRequest.newBuilder().setName(rawImpressionUpload).build()
      )
    val fileRows = listRawFiles(rawImpressionUpload)
    val modelLineRows = listModelLines(rawImpressionUpload)
    val allWorkItems = listWorkItems()
    val nodes = mutableListOf<ExpectedTraceNode>()
    nodes +=
      ExpectedTraceNode(
        upload.name,
        null,
        "upload_registration",
        upload.state.name,
        buildMap {
          put(RAW_UPLOAD, upload.name)
          put(GCS_GENERATION, upload.doneBlobGeneration.toString())
          if (upload.replacesRawImpressionUpload.isNotEmpty()) {
            put(REPLACES_UPLOAD, upload.replacesRawImpressionUpload)
          }
          if (upload.uploadHealingOperation.isNotEmpty()) {
            put(HEALING_OPERATION, upload.uploadHealingOperation)
          }
        },
      )
    for (file in fileRows) {
      nodes +=
        ExpectedTraceNode(
          file.name,
          null,
          "upload_registration",
          "REGISTERED",
          mapOf(
            RAW_UPLOAD to upload.name,
            RAW_UPLOAD_FILE to file.name,
            GCS_GENERATION to file.blobGeneration.toString(),
            GCS_PATH_HASH to hash(file.blobUri),
          ),
          ExpectedNodeDisposition.AUTHORITATIVE_ONLY,
        )
    }
    for (modelLine in modelLineRows) {
      val poolRows = listPoolJobs(rawImpressionUpload, modelLine.cmmsModelLine)
      val rankRows = listRankerJobs(rawImpressionUpload, modelLine.cmmsModelLine)
      val labelRows = listLabelingJobs(rawImpressionUpload, modelLine.cmmsModelLine)
      val blobRows = listRankBlobs(rawImpressionUpload, modelLine.cmmsModelLine)
      val memoized = poolRows.isNotEmpty() || rankRows.isNotEmpty() || blobRows.isNotEmpty()
      nodes +=
        ExpectedTraceNode(
          modelLine.name,
          modelLine.cmmsModelLine,
          "dispatch",
          modelLine.state.name,
          baseIdentifiers(upload.name, modelLine) +
            (LABEL_ROUTE to if (memoized) "memoized" else "non_memoized"),
        )
      for (job in poolRows) {
        nodes += jobNode(upload.name, modelLine, job)
        nodes +=
          workNodes(
            WorkItemIds.forSubpoolAssigner(upload.name, modelLine.cmmsModelLine, job.shardIndex),
            modelLine.cmmsModelLine,
            "pool_assignment",
            modelLine.failureAttemptId,
            allWorkItems,
          )
      }
      nodes +=
        finalizeNode(
          modelLine,
          "pool_assignment_finalize",
          poolRows.map { it.state.name },
          memoized,
        )
      for (job in rankRows) {
        nodes += jobNode(upload.name, modelLine, job)
        nodes +=
          workNodes(
            WorkItemIds.forVidRankBuilder(job.name),
            modelLine.cmmsModelLine,
            "rank",
            modelLine.failureAttemptId,
            allWorkItems,
          )
      }
      nodes += finalizeNode(modelLine, "rank_finalize", rankRows.map { it.state.name }, memoized)
      for (blob in blobRows) nodes += blobNode(upload.name, modelLine, blob)
      for (job in labelRows) {
        nodes += jobNode(upload.name, modelLine, job)
        nodes +=
          workNodes(
            WorkItemIds.forVidLabeler(job.name),
            modelLine.cmmsModelLine,
            "label",
            modelLine.failureAttemptId,
            allWorkItems,
          )
      }
      nodes +=
        finalizeNode(
          modelLine,
          "label_finalize",
          labelRows.map { it.state.name },
          labelRows.isNotEmpty(),
        )
    }
    return VidLabelingAuthoritativeGraph(upload, modelLineRows, nodes)
  }

  private fun baseIdentifiers(
    upload: String,
    modelLine: RawImpressionUploadModelLine,
  ): Map<String, String> = buildMap {
    put(RAW_UPLOAD, upload)
    put(RAW_UPLOAD_MODEL_LINE, modelLine.name)
    put(MODEL_LINE, modelLine.cmmsModelLine)
    if (modelLine.recoveryPredecessorRawImpressionUpload.isNotEmpty()) {
      put(RECOVERY_PREDECESSOR, modelLine.recoveryPredecessorRawImpressionUpload)
    }
  }

  private fun jobNode(
    upload: String,
    modelLine: RawImpressionUploadModelLine,
    job: PoolAssignmentJob,
  ) =
    ExpectedTraceNode(
      job.name,
      modelLine.cmmsModelLine,
      "pool_assignment",
      job.state.name,
      baseIdentifiers(upload, modelLine) +
        mapOf(POOL_JOB to job.name, SHARD_INDEX to job.shardIndex.toString()),
    )

  private fun jobNode(upload: String, modelLine: RawImpressionUploadModelLine, job: RankerJob) =
    ExpectedTraceNode(
      job.name,
      modelLine.cmmsModelLine,
      "rank",
      job.state.name,
      baseIdentifiers(upload, modelLine) + mapOf(RANKER_JOB to job.name),
    )

  private fun jobNode(
    upload: String,
    modelLine: RawImpressionUploadModelLine,
    job: VidLabelingJob,
  ) =
    ExpectedTraceNode(
      job.name + ":" + modelLine.cmmsModelLine,
      modelLine.cmmsModelLine,
      "label",
      job.state.name,
      baseIdentifiers(upload, modelLine) + mapOf(LABELING_JOB to job.name),
    )

  private fun blobNode(
    upload: String,
    modelLine: RawImpressionUploadModelLine,
    blob: RankIndexBlob,
  ) =
    ExpectedTraceNode(
      blob.name,
      modelLine.cmmsModelLine,
      "rank",
      "PUBLISHED",
      baseIdentifiers(upload, modelLine) + mapOf(RANK_BLOB to blob.name),
    )

  private fun finalizeNode(
    modelLine: RawImpressionUploadModelLine,
    stage: String,
    states: List<String>,
    applicable: Boolean,
  ) =
    ExpectedTraceNode(
      modelLine.name + ":" + stage,
      modelLine.cmmsModelLine,
      stage,
      states.joinToString(),
      mapOf(RAW_UPLOAD_MODEL_LINE to modelLine.name, MODEL_LINE to modelLine.cmmsModelLine),
      if (applicable && states.isNotEmpty() && states.all { it == "SUCCEEDED" }) {
        ExpectedNodeDisposition.REQUIRED
      } else {
        ExpectedNodeDisposition.NOT_APPLICABLE
      },
    )

  private suspend fun workNodes(
    originalId: String,
    modelLine: String,
    stage: String,
    failureAttemptId: String,
    allWorkItems: List<WorkItem>,
  ): List<ExpectedTraceNode> {
    val originalName = "workItems/$originalId"
    val retryName =
      failureAttemptId
        .takeIf { it.isNotEmpty() }
        ?.let { "workItems/" + RequestIds.forRetriedWorkItem(originalId, it) }
    val matching =
      allWorkItems.filter {
        it.name == originalName ||
          it.name.startsWith(originalName + "-monitor-recovery-") ||
          it.name == retryName
      }
    if (matching.isEmpty()) {
      return listOf(
        ExpectedTraceNode(
          originalName,
          modelLine,
          stage,
          "MISSING",
          mapOf(MODEL_LINE to modelLine, WORK_ITEM to originalName),
        )
      )
    }
    return matching.flatMap { workItem ->
      buildList {
        add(
          ExpectedTraceNode(
            workItem.name + ":generation:" + workItem.generation,
            modelLine,
            stage,
            workItem.state.name,
            mapOf(
              MODEL_LINE to modelLine,
              WORK_ITEM to workItem.name,
              WORK_ITEM_GENERATION to workItem.generation.toString(),
            ),
          )
        )
        val attempts = listAttempts(workItem.name)
        if (attempts.isEmpty() && workItem.state != WorkItem.State.QUEUED) {
          add(
            ExpectedTraceNode(
              workItem.name + ":attempt",
              modelLine,
              stage,
              "MISSING",
              mapOf(
                MODEL_LINE to modelLine,
                WORK_ITEM to workItem.name,
                WORK_ITEM_GENERATION to workItem.generation.toString(),
              ),
            )
          )
        }
        for (attempt in attempts) {
          add(
            ExpectedTraceNode(
              attempt.name,
              modelLine,
              stage,
              attempt.state.name,
              mapOf(
                MODEL_LINE to modelLine,
                WORK_ITEM to workItem.name,
                WORK_ITEM_ATTEMPT to attempt.name,
                WORK_ITEM_GENERATION to workItem.generation.toString(),
              ),
            )
          )
        }
      }
    }
  }

  private suspend fun listModelLines(parent: String): List<RawImpressionUploadModelLine> {
    val result = mutableListOf<RawImpressionUploadModelLine>()
    var token = ""
    do {
      val response =
        modelLines.listRawImpressionUploadModelLines(
          ListRawImpressionUploadModelLinesRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .build()
        )
      result += response.rawImpressionUploadModelLinesList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listRawFiles(parent: String): List<RawImpressionUploadFile> {
    val result = mutableListOf<RawImpressionUploadFile>()
    var token = ""
    do {
      val response =
        rawFiles.listRawImpressionUploadFiles(
          ListRawImpressionUploadFilesRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .build()
        )
      result += response.rawImpressionUploadFilesList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listPoolJobs(parent: String, modelLine: String): List<PoolAssignmentJob> {
    val result = mutableListOf<PoolAssignmentJob>()
    var token = ""
    do {
      val filter = ListPoolAssignmentJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
      val response =
        poolJobs.listPoolAssignmentJobs(
          ListPoolAssignmentJobsRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      result += response.poolAssignmentJobsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listRankerJobs(parent: String, modelLine: String): List<RankerJob> {
    val result = mutableListOf<RankerJob>()
    var token = ""
    do {
      val filter = ListRankerJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
      val response =
        rankerJobs.listRankerJobs(
          ListRankerJobsRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      result += response.rankerJobsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listLabelingJobs(parent: String, modelLine: String): List<VidLabelingJob> {
    val result = mutableListOf<VidLabelingJob>()
    var token = ""
    do {
      val filter = ListVidLabelingJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
      val response =
        labelingJobs.listVidLabelingJobs(
          ListVidLabelingJobsRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      result += response.vidLabelingJobsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listRankBlobs(parent: String, modelLine: String): List<RankIndexBlob> {
    val result = mutableListOf<RankIndexBlob>()
    var token = ""
    do {
      val filter = ListRankIndexBlobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
      val response =
        rankBlobs.listRankIndexBlobs(
          ListRankIndexBlobsRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      result += response.rankIndexBlobsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listWorkItems(): List<WorkItem> {
    val result = mutableListOf<WorkItem>()
    var token = ""
    do {
      val response =
        workItems.listWorkItems(
          ListWorkItemsRequest.newBuilder().setPageSize(1000).setPageToken(token).build()
        )
      result += response.workItemsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  private suspend fun listAttempts(parent: String): List<WorkItemAttempt> {
    val result = mutableListOf<WorkItemAttempt>()
    var token = ""
    do {
      val response =
        workItemAttempts.listWorkItemAttempts(
          ListWorkItemAttemptsRequest.newBuilder()
            .setParent(parent)
            .setPageSize(100)
            .setPageToken(token)
            .build()
        )
      result += response.workItemAttemptsList
      token = response.nextPageToken
    } while (token.isNotEmpty())
    return result
  }

  companion object {
    private const val RAW_UPLOAD = "xmm.edpa.raw_impression_upload.name"
    private const val RAW_UPLOAD_MODEL_LINE = "xmm.edpa.raw_impression_upload_model_line.name"
    private const val RAW_UPLOAD_FILE = "xmm.edpa.raw_impression_upload_file.name"
    private const val MODEL_LINE = "xmm.model_line.name"
    private const val POOL_JOB = "xmm.edpa.pool_assignment_job.name"
    private const val RANKER_JOB = "xmm.edpa.ranker_job.name"
    private const val LABELING_JOB = "xmm.edpa.vid_labeling_job.name"
    private const val RANK_BLOB = "xmm.edpa.rank_index_blob.name"
    private const val SHARD_INDEX = "xmm.edpa.shard_index"
    private const val LABEL_ROUTE = "xmm.edpa.label.route"
    private const val GCS_GENERATION = "xmm.gcs.object.generation"
    private const val GCS_PATH_HASH = "xmm.gcs.object.path_hash"
    private const val WORK_ITEM = "xmm.work_item.name"
    private const val WORK_ITEM_ATTEMPT = "xmm.work_item_attempt.name"
    private const val WORK_ITEM_GENERATION = "xmm.work_item.generation"
    private const val REPLACES_UPLOAD = "xmm.edpa.replaces_raw_impression_upload.name"
    private const val HEALING_OPERATION = "xmm.edpa.upload_healing_operation.name"
    private const val RECOVERY_PREDECESSOR =
      "xmm.edpa.recovery_predecessor_raw_impression_upload.name"
  }

  private fun hash(value: String): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString(
      ""
    ) {
      (it.toInt() and 0xff).toString(16).padStart(2, '0')
    }
}
