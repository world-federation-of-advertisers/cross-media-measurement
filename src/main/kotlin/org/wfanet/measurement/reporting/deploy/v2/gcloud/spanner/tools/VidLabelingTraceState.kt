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

import com.google.protobuf.util.Timestamps
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetModelLineRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.ListModelShardsRequest
import org.wfanet.measurement.api.v2alpha.ListModelShardsResponse
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemAttemptsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ListWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

internal enum class ExpectedNodeDisposition {
  REQUIRED,
  AUTHORITATIVE_ONLY,
  NOT_APPLICABLE,
  UNRESOLVED,
}

internal data class ExpectedTraceNode(
  val id: String,
  val modelLine: String?,
  val stage: String,
  val authoritativeState: String,
  val identifiers: Map<String, String>,
  val disposition: ExpectedNodeDisposition = ExpectedNodeDisposition.REQUIRED,
)

internal data class ExpectedWorkItemGroup(
  val originalName: String,
  val modelLine: String,
  val stage: String,
  val candidateNames: List<String>,
)

internal data class ResolvedWorkItem(
  val workItem: WorkItem,
  val attempts: List<WorkItemAttempt>,
  val attemptPages: Int,
)

internal data class VidLabelingWorkItemResolution(
  val nodes: List<ExpectedTraceNode>,
  val workItems: List<ResolvedWorkItem>,
  val truncated: Boolean,
  val counts: Map<String, Long>,
)

internal data class VidLabelingRouteResolution(
  val routes: Map<String, Boolean>,
  val counts: Map<String, Long>,
)

internal data class VidLabelingFinalStateResolution(
  val nodes: List<ExpectedTraceNode>,
  val counts: Map<String, Long>,
)

internal data class VidLabelingAuthoritativeGraph(
  val upload: RawImpressionUpload,
  val modelLines: List<RawImpressionUploadModelLine>,
  val nodes: List<ExpectedTraceNode>,
  val expectedWorkItems: List<ExpectedWorkItemGroup> = emptyList(),
  val counts: Map<String, Long> = emptyMap(),
) {
  val correlationValues: Set<String>
    get() =
      (nodes.flatMap { it.identifiers.values } + upload.name)
        .filter { it.contains('/') || it.length >= 16 }
        .toSet()

  fun withRoutes(resolution: VidLabelingRouteResolution): VidLabelingAuthoritativeGraph {
    return copy(
      nodes =
        nodes.map { node ->
          val memoized = node.modelLine?.let(resolution.routes::get)
          if (node.stage != "dispatch" || memoized == null) {
            node
          } else {
            node.copy(
              identifiers =
                node.identifiers + (LABEL_ROUTE to if (memoized) "memoized" else "non_memoized")
            )
          }
        }
    )
  }

  companion object {
    private const val LABEL_ROUTE = "xmm.edpa.label.route"
  }
}

internal fun interface VidLabelingStateResolver {
  suspend fun resolve(rawImpressionUpload: String): VidLabelingAuthoritativeGraph
}

internal fun interface VidLabelingWorkItemResolver {
  suspend fun resolve(
    rawImpressionUpload: String,
    expectedWorkItems: List<ExpectedWorkItemGroup>,
    pageLimit: Int,
  ): VidLabelingWorkItemResolution
}

internal fun interface RelevantWorkItemSelector {
  fun isRelevant(rawImpressionUpload: String, workItem: WorkItem): Boolean
}

internal fun interface VidLabelingRouteResolver {
  suspend fun resolve(dataProvider: String, modelLines: List<String>): VidLabelingRouteResolution
}

internal class KingdomVidLabelingRouteResolver
private constructor(
  private val getModelLine: suspend (GetModelLineRequest) -> ModelLine,
  private val listModelRolloutsRpc: suspend (ListModelRolloutsRequest) -> ListModelRolloutsResponse,
  private val listModelShardsRpc: suspend (ListModelShardsRequest) -> ListModelShardsResponse,
  private val maxConcurrency: Int = MODEL_LINE_CONCURRENCY,
) : VidLabelingRouteResolver {
  constructor(
    modelLines: ModelLinesCoroutineStub,
    modelRollouts: ModelRolloutsCoroutineStub,
    modelShards: ModelShardsCoroutineStub,
    maxConcurrency: Int = MODEL_LINE_CONCURRENCY,
  ) : this(
    { request -> modelLines.getModelLine(request) },
    { request -> modelRollouts.listModelRollouts(request) },
    { request -> modelShards.listModelShards(request) },
    maxConcurrency,
  )

  override suspend fun resolve(
    dataProvider: String,
    modelLines: List<String>,
  ): VidLabelingRouteResolution {
    require(maxConcurrency > 0) { "maxConcurrency must be positive" }
    val shardRows = listModelShards(dataProvider)
    val semaphore = Semaphore(maxConcurrency)
    val results = coroutineScope {
      modelLines
        .distinct()
        .sorted()
        .map { modelLine ->
          async {
            semaphore.withPermit {
              val resolvedModelLine =
                getModelLine(GetModelLineRequest.newBuilder().setName(modelLine).build())
              val rolloutRows = listModelRollouts(resolvedModelLine.name)
              val modelRelease =
                rolloutRows.rows.firstOrNull { it.modelRelease.isNotEmpty() }?.modelRelease
                  ?: error("No model rollout with a release found for $modelLine")
              val shard =
                shardRows.rows.firstOrNull { it.modelRelease == modelRelease && it.hasModelBlob() }
                  ?: error("No model shard found for $modelRelease on $dataProvider")
              RouteResult(modelLine, shard.memoizedVidAssignmentEnabled, rolloutRows.pages)
            }
          }
        }
        .awaitAll()
    }
    return VidLabelingRouteResolution(
      results.associate { it.modelLine to it.memoized },
      mapOf(
        "model_lines_resolved" to results.size.toLong(),
        "model_rollout_pages" to results.sumOf { it.rolloutPages }.toLong(),
        "model_shard_pages" to shardRows.pages.toLong(),
      ),
    )
  }

  private suspend fun listModelRollouts(parent: String) = pagedRows { token ->
    val response =
      listModelRolloutsRpc(
        ListModelRolloutsRequest.newBuilder().setParent(parent).setPageToken(token).build()
      )
    Page(response.modelRolloutsList, response.nextPageToken)
  }

  private suspend fun listModelShards(parent: String) = pagedRows { token ->
    val response =
      listModelShardsRpc(
        ListModelShardsRequest.newBuilder().setParent(parent).setPageToken(token).build()
      )
    Page(response.modelShardsList, response.nextPageToken)
  }

  private data class RouteResult(
    val modelLine: String,
    val memoized: Boolean,
    val rolloutPages: Int,
  )

  companion object {
    internal fun fromFunctions(
      maxConcurrency: Int = MODEL_LINE_CONCURRENCY,
      getModelLine: suspend (GetModelLineRequest) -> ModelLine,
      listModelRollouts: suspend (ListModelRolloutsRequest) -> ListModelRolloutsResponse,
      listModelShards: suspend (ListModelShardsRequest) -> ListModelShardsResponse,
    ): KingdomVidLabelingRouteResolver {
      return KingdomVidLabelingRouteResolver(
        getModelLine,
        listModelRollouts,
        listModelShards,
        maxConcurrency,
      )
    }
  }
}

internal fun interface VidLabelingFinalStateResolver {
  suspend fun resolve(
    upload: RawImpressionUpload,
    modelLines: List<RawImpressionUploadModelLine>,
  ): VidLabelingFinalStateResolution
}

internal data class StoredObjectMetadata(val generation: Long)

internal class GcsKingdomFinalStateResolver(
  private val impressionMetadata: ImpressionMetadataServiceCoroutineStub,
  private val dataProviders: DataProvidersCoroutineStub,
  private val modelLineConcurrency: Int = MODEL_LINE_CONCURRENCY,
  private val gcsConcurrency: Int = GCS_CONCURRENCY,
  private val readObjectMetadata: suspend (String, Long?) -> StoredObjectMetadata?,
) : VidLabelingFinalStateResolver {
  override suspend fun resolve(
    upload: RawImpressionUpload,
    modelLines: List<RawImpressionUploadModelLine>,
  ): VidLabelingFinalStateResolution = coroutineScope {
    require(modelLineConcurrency > 0) { "modelLineConcurrency must be positive" }
    require(gcsConcurrency > 0) { "gcsConcurrency must be positive" }
    val dataProviderName = upload.name.substringBefore("/rawImpressionUploads/")
    val gcsSemaphore = Semaphore(gcsConcurrency)
    val dataProviderDeferred = async {
      dataProviders.getDataProvider(
        GetDataProviderRequest.newBuilder().setName(dataProviderName).build()
      )
    }
    val rootDoneDeferred = async {
      upload.doneBlobUri
        .takeIf { it.isNotEmpty() }
        ?.let { uri ->
          gcsSemaphore.withPermit { readObjectMetadata(uri, upload.doneBlobGeneration) }
        }
    }
    val modelLineSemaphore = Semaphore(modelLineConcurrency)
    val modelLineResults =
      modelLines
        .sortedBy { it.name }
        .map { modelLine ->
          async {
            modelLineSemaphore.withPermit {
              resolveModelLine(dataProviderName, upload, modelLine, gcsSemaphore)
            }
          }
        }
        .awaitAll()
    val dataProvider = dataProviderDeferred.await()
    val rootDone = rootDoneDeferred.await()
    val availabilityByModelLine =
      dataProvider.dataAvailabilityIntervalsList.associate { it.key to it.value }
    val nodes = buildList {
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
      for (result in modelLineResults) {
        addAll(result.nodes)
        val availability = availabilityByModelLine[result.modelLine.cmmsModelLine]
        if (result.rows.isEmpty()) {
          add(
            ExpectedTraceNode(
              result.modelLine.name + ":kingdom_availability",
              result.modelLine.cmmsModelLine,
              "data_availability_publish",
              "MISSING",
              mapOf(MODEL_LINE to result.modelLine.cmmsModelLine),
              if (result.modelLine.state == RawImpressionUploadModelLine.State.COMPLETED) {
                ExpectedNodeDisposition.REQUIRED
              } else {
                ExpectedNodeDisposition.NOT_APPLICABLE
              },
            )
          )
        }
        for (row in result.rows) {
          val covered = availability?.let { contains(it, row.interval) } == true
          add(
            ExpectedTraceNode(
              row.name + ":kingdom_availability",
              result.modelLine.cmmsModelLine,
              "data_availability_publish",
              if (covered) "PUBLISHED" else "MISSING",
              mapOf(
                MODEL_LINE to result.modelLine.cmmsModelLine,
                AVAILABILITY_INTERVAL_START to Timestamps.toString(row.interval.startTime),
                AVAILABILITY_INTERVAL_END to Timestamps.toString(row.interval.endTime),
              ),
              if (result.modelLine.state == RawImpressionUploadModelLine.State.COMPLETED) {
                ExpectedNodeDisposition.REQUIRED
              } else {
                ExpectedNodeDisposition.NOT_APPLICABLE
              },
            )
          )
        }
      }
    }
    VidLabelingFinalStateResolution(
      nodes,
      mapOf(
        "kingdom_reads" to 1L,
        "impression_metadata_pages" to modelLineResults.sumOf { it.metadataPages }.toLong(),
        "impression_metadata_rows" to modelLineResults.sumOf { it.rows.size }.toLong(),
        "gcs_reads" to
          ((if (upload.doneBlobUri.isEmpty()) 0 else 1) + modelLineResults.sumOf { it.gcsReads })
            .toLong(),
      ),
    )
  }

  private suspend fun resolveModelLine(
    dataProviderName: String,
    upload: RawImpressionUpload,
    modelLine: RawImpressionUploadModelLine,
    gcsSemaphore: Semaphore,
  ): FinalModelLineResult {
    val metadataRows = listMetadata(dataProviderName, upload.name, modelLine.cmmsModelLine)
    val objectRows = coroutineScope {
      metadataRows.rows
        .sortedBy { it.name }
        .map { row ->
          async {
            val sidecarUri = row.blobUri
            val labeledUri = sidecarUri.removeSuffix(".metadata.binpb")
            val doneUri = doneUri(sidecarUri)
            val labeledDeferred = async {
              gcsSemaphore.withPermit { readObjectMetadata(labeledUri, null) != null }
            }
            val sidecarDeferred = async {
              gcsSemaphore.withPermit { readObjectMetadata(sidecarUri, null) != null }
            }
            val doneDeferred = async {
              gcsSemaphore.withPermit {
                readObjectMetadata(doneUri, row.outputDoneBlobGeneration.takeIf { it != 0L })
              }
            }
            FinalObjectRow(
              row,
              labeledUri,
              sidecarUri,
              doneUri,
              labeledDeferred.await(),
              sidecarDeferred.await(),
              doneDeferred.await(),
            )
          }
        }
        .awaitAll()
    }
    val nodes = buildList {
      if (modelLine.state == RawImpressionUploadModelLine.State.COMPLETED && objectRows.isEmpty()) {
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
      for (objectRow in objectRows) {
        val row = objectRow.row
        add(
          ExpectedTraceNode(
            row.name,
            modelLine.cmmsModelLine,
            "data_availability_metadata",
            row.state.name,
            mapOf(
              MODEL_LINE to modelLine.cmmsModelLine,
              IMPRESSION_METADATA to row.name,
              GCS_PATH_HASH to hash(objectRow.sidecarUri),
            ),
          )
        )
        add(
          objectNode(
            objectRow.labeledUri,
            modelLine.cmmsModelLine,
            "label",
            objectRow.labeledExists,
          )
        )
        add(
          objectNode(
            objectRow.sidecarUri,
            modelLine.cmmsModelLine,
            VidLabelingTraceLogging.LABEL_OUTPUT_STAGE,
            objectRow.sidecarExists,
          )
        )
        add(
          ExpectedTraceNode(
            "gcs:" + hash(objectRow.doneUri),
            modelLine.cmmsModelLine,
            "data_watcher",
            if (objectRow.doneBlob == null) "MISSING" else "PUBLISHED",
            mapOf(
              RAW_UPLOAD to upload.name,
              MODEL_LINE to modelLine.cmmsModelLine,
              GCS_GENERATION to (objectRow.doneBlob?.generation?.toString() ?: "0"),
              GCS_PATH_HASH to hash(objectRow.doneUri),
            ),
          )
        )
      }
    }
    return FinalModelLineResult(
      modelLine,
      objectRows.map { it.row },
      nodes,
      metadataRows.pages,
      objectRows.size * 3,
    )
  }

  private fun contains(container: com.google.type.Interval, contained: com.google.type.Interval) =
    Timestamps.compare(container.startTime, contained.startTime) <= 0 &&
      Timestamps.compare(container.endTime, contained.endTime) >= 0

  private fun objectNode(uri: String, modelLine: String, stage: String, exists: Boolean) =
    ExpectedTraceNode(
      "gcs:" + hash(uri),
      modelLine,
      stage,
      if (exists) "PUBLISHED" else "MISSING",
      mapOf(MODEL_LINE to modelLine, GCS_PATH_HASH to hash(uri)),
    )

  private suspend fun listMetadata(parent: String, rawImpressionUpload: String, modelLine: String) =
    pagedRows { token ->
      val filter =
        ListImpressionMetadataRequest.Filter.newBuilder()
          .setModelLine(modelLine)
          .setRawImpressionUpload(rawImpressionUpload)
      val response =
        impressionMetadata.listImpressionMetadata(
          ListImpressionMetadataRequest.newBuilder()
            .setParent(parent)
            .setPageToken(token)
            .setFilter(filter)
            .build()
        )
      Page(response.impressionMetadataList, response.nextPageToken)
    }

  private fun doneUri(sidecarUri: String): String = sidecarUri.substringBeforeLast('/') + "/done"

  private data class FinalObjectRow(
    val row: ImpressionMetadata,
    val labeledUri: String,
    val sidecarUri: String,
    val doneUri: String,
    val labeledExists: Boolean,
    val sidecarExists: Boolean,
    val doneBlob: StoredObjectMetadata?,
  )

  private data class FinalModelLineResult(
    val modelLine: RawImpressionUploadModelLine,
    val rows: List<ImpressionMetadata>,
    val nodes: List<ExpectedTraceNode>,
    val metadataPages: Int,
    val gcsReads: Int,
  )

  companion object {
    private const val RAW_UPLOAD = "xmm.edpa.raw_impression_upload.name"
    private const val MODEL_LINE = "xmm.model_line.name"
    private const val IMPRESSION_METADATA = "xmm.edpa.impression_metadata.name"
    private const val GCS_GENERATION = "xmm.gcs.object.generation"
    private const val GCS_PATH_HASH = "xmm.gcs.object.path_hash"
    private const val AVAILABILITY_INTERVAL_START = "xmm.edpa.availability.interval_start"
    private const val AVAILABILITY_INTERVAL_END = "xmm.edpa.availability.interval_end"
  }
}

internal class GrpcVidLabelingStateResolver(
  private val uploads: RawImpressionUploadServiceCoroutineStub,
  private val rawFiles: RawImpressionUploadFileServiceCoroutineStub,
  private val uploadModelLines: RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolJobs: PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobs: RankerJobServiceCoroutineStub,
  private val labelingJobs: VidLabelingJobServiceCoroutineStub,
  private val rankBlobs: RankIndexBlobServiceCoroutineStub,
  private val maxConcurrency: Int = MODEL_LINE_CONCURRENCY,
) : VidLabelingStateResolver {
  override suspend fun resolve(rawImpressionUpload: String): VidLabelingAuthoritativeGraph =
    coroutineScope {
      require(maxConcurrency > 0) { "maxConcurrency must be positive" }
      val upload =
        uploads.getRawImpressionUpload(
          GetRawImpressionUploadRequest.newBuilder().setName(rawImpressionUpload).build()
        )
      val fileRowsDeferred = async { listRawFiles(rawImpressionUpload) }
      val modelLineRows = listModelLines(rawImpressionUpload)
      val semaphore = Semaphore(maxConcurrency)
      val modelLineJobs =
        modelLineRows.rows
          .sortedBy { it.name }
          .map { modelLine ->
            async {
              val pool = async {
                semaphore.withPermit { listPoolJobs(rawImpressionUpload, modelLine.cmmsModelLine) }
              }
              val ranker = async {
                semaphore.withPermit {
                  listRankerJobs(rawImpressionUpload, modelLine.cmmsModelLine)
                }
              }
              val labeling = async {
                semaphore.withPermit {
                  listLabelingJobs(rawImpressionUpload, modelLine.cmmsModelLine)
                }
              }
              val blobs = async {
                semaphore.withPermit { listRankBlobs(rawImpressionUpload, modelLine.cmmsModelLine) }
              }
              ModelLineJobs(
                modelLine,
                pool.await(),
                ranker.await(),
                labeling.await(),
                blobs.await(),
              )
            }
          }
          .awaitAll()
      val fileRows = fileRowsDeferred.await()
      val nodes = mutableListOf<ExpectedTraceNode>()
      val expectedWorkItems = mutableListOf<ExpectedWorkItemGroup>()
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
      for (file in fileRows.rows.sortedBy { it.name }) {
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
      for (jobs in modelLineJobs) {
        val modelLine = jobs.modelLine
        nodes +=
          ExpectedTraceNode(
            modelLine.name,
            modelLine.cmmsModelLine,
            "dispatch",
            modelLine.state.name,
            baseIdentifiers(upload.name, modelLine),
          )
        for (job in jobs.pool.rows.sortedBy { it.name }) {
          nodes += jobNode(upload.name, modelLine, job)
          expectedWorkItems +=
            expectedWorkItemGroup(
              WorkItemIds.forSubpoolAssigner(upload.name, modelLine.cmmsModelLine, job.shardIndex),
              modelLine,
              "pool_assignment",
            )
        }
        nodes +=
          finalizeNode(
            modelLine,
            "pool_assignment_finalize",
            jobs.pool.rows.map { it.state.name },
            jobs.pool.rows.isNotEmpty(),
          )
        for (job in jobs.ranker.rows.sortedBy { it.name }) {
          nodes += jobNode(upload.name, modelLine, job)
          expectedWorkItems +=
            expectedWorkItemGroup(WorkItemIds.forVidRankBuilder(job.name), modelLine, "rank")
        }
        nodes +=
          finalizeNode(
            modelLine,
            "rank_finalize",
            jobs.ranker.rows.map { it.state.name },
            jobs.ranker.rows.isNotEmpty(),
          )
        for (blob in jobs.blobs.rows.sortedBy { it.name }) {
          nodes += blobNode(upload.name, modelLine, blob)
        }
        for (job in jobs.labeling.rows.sortedBy { it.name }) {
          nodes += jobNode(upload.name, modelLine, job)
          expectedWorkItems +=
            expectedWorkItemGroup(WorkItemIds.forVidLabeler(job.name), modelLine, "label")
        }
        nodes +=
          finalizeNode(
            modelLine,
            "label_finalize",
            jobs.labeling.rows.map { it.state.name },
            jobs.labeling.rows.isNotEmpty(),
          )
      }
      VidLabelingAuthoritativeGraph(
        upload,
        modelLineRows.rows.sortedBy { it.name },
        nodes,
        expectedWorkItems.sortedWith(
          compareBy<ExpectedWorkItemGroup> { it.modelLine }
            .thenBy { it.stage }
            .thenBy { it.originalName }
        ),
        mapOf(
          "uploads" to 1L,
          "raw_file_pages" to fileRows.pages.toLong(),
          "raw_files" to fileRows.rows.size.toLong(),
          "model_line_pages" to modelLineRows.pages.toLong(),
          "model_lines" to modelLineRows.rows.size.toLong(),
          "pool_job_pages" to modelLineJobs.sumOf { it.pool.pages }.toLong(),
          "pool_jobs" to modelLineJobs.sumOf { it.pool.rows.size }.toLong(),
          "ranker_job_pages" to modelLineJobs.sumOf { it.ranker.pages }.toLong(),
          "ranker_jobs" to modelLineJobs.sumOf { it.ranker.rows.size }.toLong(),
          "labeling_job_pages" to modelLineJobs.sumOf { it.labeling.pages }.toLong(),
          "labeling_jobs" to modelLineJobs.sumOf { it.labeling.rows.size }.toLong(),
          "rank_blob_pages" to modelLineJobs.sumOf { it.blobs.pages }.toLong(),
          "rank_blobs" to modelLineJobs.sumOf { it.blobs.rows.size }.toLong(),
        ),
      )
    }

  private fun expectedWorkItemGroup(
    originalId: String,
    modelLine: RawImpressionUploadModelLine,
    stage: String,
  ): ExpectedWorkItemGroup {
    val originalName = "workItems/$originalId"
    return ExpectedWorkItemGroup(
      originalName,
      modelLine.cmmsModelLine,
      stage,
      buildList {
        add(originalName)
        if (modelLine.failureAttemptId.isNotEmpty()) {
          add("workItems/" + RequestIds.forRetriedWorkItem(originalId, modelLine.failureAttemptId))
        }
        for (attempt in 1..WorkItemIds.MAX_MONITOR_RECOVERY_ATTEMPTS) {
          add("workItems/" + WorkItemIds.forMonitorRecovery(originalId, attempt))
        }
      },
    )
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

  private suspend fun listModelLines(parent: String) = pagedRows { token ->
    val response =
      uploadModelLines.listRawImpressionUploadModelLines(
        ListRawImpressionUploadModelLinesRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .build()
      )
    Page(response.rawImpressionUploadModelLinesList, response.nextPageToken)
  }

  private suspend fun listRawFiles(parent: String) = pagedRows { token ->
    val response =
      rawFiles.listRawImpressionUploadFiles(
        ListRawImpressionUploadFilesRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .build()
      )
    Page(response.rawImpressionUploadFilesList, response.nextPageToken)
  }

  private suspend fun listPoolJobs(parent: String, modelLine: String) = pagedRows { token ->
    val filter = ListPoolAssignmentJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
    val response =
      poolJobs.listPoolAssignmentJobs(
        ListPoolAssignmentJobsRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .setFilter(filter)
          .build()
      )
    Page(response.poolAssignmentJobsList, response.nextPageToken)
  }

  private suspend fun listRankerJobs(parent: String, modelLine: String) = pagedRows { token ->
    val filter = ListRankerJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
    val response =
      rankerJobs.listRankerJobs(
        ListRankerJobsRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .setFilter(filter)
          .build()
      )
    Page(response.rankerJobsList, response.nextPageToken)
  }

  private suspend fun listLabelingJobs(parent: String, modelLine: String) = pagedRows { token ->
    val filter = ListVidLabelingJobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
    val response =
      labelingJobs.listVidLabelingJobs(
        ListVidLabelingJobsRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .setFilter(filter)
          .build()
      )
    Page(response.vidLabelingJobsList, response.nextPageToken)
  }

  private suspend fun listRankBlobs(parent: String, modelLine: String) = pagedRows { token ->
    val filter = ListRankIndexBlobsRequest.Filter.newBuilder().setCmmsModelLine(modelLine)
    val response =
      rankBlobs.listRankIndexBlobs(
        ListRankIndexBlobsRequest.newBuilder()
          .setParent(parent)
          .setPageToken(token)
          .setFilter(filter)
          .build()
      )
    Page(response.rankIndexBlobsList, response.nextPageToken)
  }

  private data class ModelLineJobs(
    val modelLine: RawImpressionUploadModelLine,
    val pool: PagedRows<PoolAssignmentJob>,
    val ranker: PagedRows<RankerJob>,
    val labeling: PagedRows<VidLabelingJob>,
    val blobs: PagedRows<RankIndexBlob>,
  )

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
    private const val GCS_GENERATION = "xmm.gcs.object.generation"
    private const val GCS_PATH_HASH = "xmm.gcs.object.path_hash"
    private const val REPLACES_UPLOAD = "xmm.edpa.replaces_raw_impression_upload.name"
    private const val HEALING_OPERATION = "xmm.edpa.upload_healing_operation.name"
    private const val RECOVERY_PREDECESSOR =
      "xmm.edpa.recovery_predecessor_raw_impression_upload.name"
  }
}

internal class GrpcVidLabelingWorkItemResolver
private constructor(
  private val listWorkItems: suspend (ListWorkItemsRequest) -> ListWorkItemsResponse,
  private val listWorkItemAttempts:
    suspend (ListWorkItemAttemptsRequest) -> ListWorkItemAttemptsResponse,
  private val relevantWorkItemSelector: RelevantWorkItemSelector =
    RelevantWorkItemSelector { _, _ ->
      false
    },
  private val maxAttemptConcurrency: Int = ATTEMPT_CONCURRENCY,
) : VidLabelingWorkItemResolver {
  constructor(
    workItems: WorkItemsCoroutineStub,
    workItemAttempts: WorkItemAttemptsCoroutineStub,
    relevantWorkItemSelector: RelevantWorkItemSelector = RelevantWorkItemSelector { _, _ -> false },
    maxAttemptConcurrency: Int = ATTEMPT_CONCURRENCY,
  ) : this(
    { request -> workItems.listWorkItems(request) },
    { request -> workItemAttempts.listWorkItemAttempts(request) },
    relevantWorkItemSelector,
    maxAttemptConcurrency,
  )

  override suspend fun resolve(
    rawImpressionUpload: String,
    expectedWorkItems: List<ExpectedWorkItemGroup>,
    pageLimit: Int,
  ): VidLabelingWorkItemResolution {
    require(pageLimit > 0) { "pageLimit must be positive" }
    require(maxAttemptConcurrency > 0) { "maxAttemptConcurrency must be positive" }
    val expectedNames = expectedWorkItems.flatMap { it.candidateNames }.toSet()
    val retained = linkedMapOf<String, WorkItem>()
    var pagesFetched = 0
    var rowsExamined = 0
    var pageToken = ""
    val requestedPageTokens = mutableSetOf(pageToken)
    var truncated = false
    do {
      val response =
        listWorkItems(
          ListWorkItemsRequest.newBuilder()
            .setPageSize(WORK_ITEM_PAGE_SIZE)
            .setPageToken(pageToken)
            .build()
        )
      pagesFetched++
      rowsExamined += response.workItemsCount
      for (workItem in response.workItemsList) {
        if (
          workItem.name in expectedNames ||
            relevantWorkItemSelector.isRelevant(rawImpressionUpload, workItem)
        ) {
          retained.putIfAbsent(workItem.name, workItem)
        }
      }
      pageToken = response.nextPageToken
      check(pageToken.isEmpty() || requestedPageTokens.add(pageToken)) {
        "ListWorkItems returned a repeated page token"
      }
      if (pageToken.isNotEmpty() && pagesFetched == pageLimit) {
        truncated = true
        break
      }
    } while (pageToken.isNotEmpty())

    val semaphore = Semaphore(maxAttemptConcurrency)
    val resolved = coroutineScope {
      retained.values
        .sortedBy { it.name }
        .map { workItem ->
          async {
            semaphore.withPermit {
              val attempts = listAttempts(workItem.name)
              ResolvedWorkItem(
                workItem,
                attempts.rows.sortedWith(
                  compareBy<WorkItemAttempt> { it.attemptNumber }.thenBy { it.name }
                ),
                attempts.pages,
              )
            }
          }
        }
        .awaitAll()
    }
    val resolvedByName = resolved.associateBy { it.workItem.name }
    val nodes =
      expectedWorkItems
        .sortedWith(
          compareBy<ExpectedWorkItemGroup> { it.modelLine }
            .thenBy { it.stage }
            .thenBy { it.originalName }
        )
        .flatMap { group -> workNodes(group, resolvedByName, truncated) }
    val matchedExpectedNames = expectedNames.count(resolvedByName::containsKey)
    return VidLabelingWorkItemResolution(
      nodes,
      resolved,
      truncated,
      mapOf(
        "pages_fetched" to pagesFetched.toLong(),
        "rows_examined" to rowsExamined.toLong(),
        "rows_retained" to retained.size.toLong(),
        "expected_work_item_names" to expectedNames.size.toLong(),
        "work_items_matched" to matchedExpectedNames.toLong(),
        "attempt_pages" to resolved.sumOf { it.attemptPages }.toLong(),
      ),
    )
  }

  private fun workNodes(
    group: ExpectedWorkItemGroup,
    resolvedByName: Map<String, ResolvedWorkItem>,
    truncated: Boolean,
  ): List<ExpectedTraceNode> {
    val matching = group.candidateNames.mapNotNull(resolvedByName::get)
    if (matching.isEmpty()) {
      return listOf(
        ExpectedTraceNode(
          group.originalName,
          group.modelLine,
          group.stage,
          if (truncated) "UNRESOLVED" else "MISSING",
          mapOf(WORK_ITEM to group.originalName),
          if (truncated) {
            ExpectedNodeDisposition.UNRESOLVED
          } else {
            ExpectedNodeDisposition.REQUIRED
          },
        )
      )
    }
    return matching.flatMap { resolved ->
      val workItem = resolved.workItem
      buildList {
        add(
          ExpectedTraceNode(
            workItem.name + ":generation:" + workItem.generation,
            group.modelLine,
            group.stage,
            workItem.state.name,
            mapOf(
              WORK_ITEM to workItem.name,
              WORK_ITEM_GENERATION to workItem.generation.toString(),
            ),
          )
        )
        if (resolved.attempts.isEmpty() && workItem.state != WorkItem.State.QUEUED) {
          add(
            ExpectedTraceNode(
              workItem.name + ":attempt",
              group.modelLine,
              group.stage,
              "MISSING",
              mapOf(
                WORK_ITEM to workItem.name,
                WORK_ITEM_GENERATION to workItem.generation.toString(),
              ),
            )
          )
        }
        for (attempt in resolved.attempts) {
          add(
            ExpectedTraceNode(
              attempt.name,
              group.modelLine,
              group.stage,
              attempt.state.name,
              buildMap {
                put(WORK_ITEM, workItem.name)
                put(WORK_ITEM_ATTEMPT, attempt.name)
                if (attempt.workItemGeneration > 0L) {
                  put(WORK_ITEM_GENERATION, attempt.workItemGeneration.toString())
                }
              },
            )
          )
        }
      }
    }
  }

  private suspend fun listAttempts(parent: String) = pagedRows { token ->
    val response =
      listWorkItemAttempts(
        ListWorkItemAttemptsRequest.newBuilder()
          .setParent(parent)
          .setPageSize(WORK_ITEM_PAGE_SIZE)
          .setPageToken(token)
          .build()
      )
    Page(response.workItemAttemptsList, response.nextPageToken)
  }

  companion object {
    private const val WORK_ITEM = "xmm.work_item.name"
    private const val WORK_ITEM_ATTEMPT = "xmm.work_item_attempt.name"
    private const val WORK_ITEM_GENERATION = "xmm.work_item.generation"

    internal fun fromFunctions(
      maxAttemptConcurrency: Int = ATTEMPT_CONCURRENCY,
      listWorkItems: suspend (ListWorkItemsRequest) -> ListWorkItemsResponse,
      listWorkItemAttempts: suspend (ListWorkItemAttemptsRequest) -> ListWorkItemAttemptsResponse,
    ): GrpcVidLabelingWorkItemResolver {
      return GrpcVidLabelingWorkItemResolver(
        listWorkItems,
        listWorkItemAttempts,
        maxAttemptConcurrency = maxAttemptConcurrency,
      )
    }
  }
}

private data class Page<T>(val rows: List<T>, val nextPageToken: String)

private data class PagedRows<T>(val rows: List<T>, val pages: Int)

private suspend fun <T> pagedRows(read: suspend (String) -> Page<T>): PagedRows<T> {
  val rows = mutableListOf<T>()
  var pages = 0
  var token = ""
  do {
    val page = read(token)
    pages++
    rows += page.rows
    token = page.nextPageToken
  } while (token.isNotEmpty())
  return PagedRows(rows, pages)
}

private fun hash(value: String): String =
  java.security.MessageDigest.getInstance("SHA-256").digest(value.toByteArray()).joinToString("") {
    (it.toInt() and 0xff).toString(16).padStart(2, '0')
  }

private const val MODEL_LINE_CONCURRENCY = 4
private const val GCS_CONCURRENCY = 8
private const val ATTEMPT_CONCURRENCY = 8
private const val WORK_ITEM_PAGE_SIZE = 100
