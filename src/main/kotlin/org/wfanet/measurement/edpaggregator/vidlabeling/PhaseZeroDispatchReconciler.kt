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

import com.google.protobuf.kotlin.unpack
import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Reconstructs the complete Phase-0 child fan-out from its immutable parent snapshot. */
class PhaseZeroDispatchReconciler(
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val maxJobsPerBatchCreate: Int = DEFAULT_MAX_JOBS_PER_BATCH_CREATE,
) {
  /** The complete deterministic WorkItem set for one Phase-0 dispatch. */
  data class PreparedFanOut(val workItems: Map<String, WorkItem>)

  /** Creates missing job rows and returns every expected WorkItem without publishing it. */
  suspend fun prepare(
    uploadName: String,
    modelLineName: String,
    dispatch: RawImpressionUploadModelLine.PhaseZeroDispatch,
    existingJobs: List<PoolAssignmentJob>,
  ): PreparedFanOut {
    check(dispatch.workItemQueue.isNotEmpty()) { "Persisted Phase-0 WorkItem queue is empty" }
    check(!dispatch.workItemParams.isEmpty) { "Persisted Phase-0 WorkItem parameters are empty" }
    val workItemParams = WorkItemParams.parseFrom(dispatch.workItemParams)
    check(workItemParams.appParams.`is`(SubpoolAssignerParams::class.java)) {
      "Persisted Phase-0 WorkItem parameters are not SubpoolAssignerParams"
    }
    val templateParams = workItemParams.appParams.unpack(SubpoolAssignerParams::class.java)
    check(
      templateParams.rawImpressionUpload == uploadName &&
        templateParams.modelLine == modelLineName &&
        templateParams.totalShards > 0
    ) {
      "Persisted Phase-0 parameters do not match $modelLineName under $uploadName"
    }

    val jobsByShard =
      ensurePoolAssignmentJobs(uploadName, modelLineName, templateParams.totalShards, existingJobs)
    val workItems = linkedMapOf<String, WorkItem>()
    for (shardIndex in 0 until templateParams.totalShards) {
      val jobName = checkNotNull(jobsByShard[shardIndex])
      val workItemId = WorkItemIds.forSubpoolAssigner(uploadName, modelLineName, shardIndex)
      workItems[workItemId] = workItem {
        queue = dispatch.workItemQueue
        this.workItemParams =
          workItemParams
            .toBuilder()
            .setAppParams(
              templateParams
                .copy {
                  poolAssignmentJob = jobName
                  this.shardIndex = shardIndex
                }
                .pack()
            )
            .build()
            .pack()
      }
    }
    return PreparedFanOut(workItems)
  }

  /** Publishes every expected WorkItem, treating deterministic-ID collisions as success. */
  suspend fun publish(fanOut: PreparedFanOut): Int {
    var published = 0
    for ((workItemId, source) in fanOut.workItems) {
      try {
        rpcThrottlers.controlPlane.onReady {
          workItemsStub.createWorkItem(
            createWorkItemRequest {
              this.workItemId = workItemId
              workItem = source
            }
          )
        }
        published++
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
        logger.info("WorkItem $workItemId already exists; skipping")
      }
    }
    return published
  }

  private suspend fun ensurePoolAssignmentJobs(
    uploadName: String,
    modelLineName: String,
    totalShards: Int,
    existingJobs: List<PoolAssignmentJob>,
  ): Map<Int, String> {
    val existingByShard = existingJobs.associateBy { it.shardIndex }
    val jobsByShard =
      if (
        existingByShard.size == totalShards && existingByShard.keys == (0 until totalShards).toSet()
      ) {
        existingByShard.mapValues { it.value.name }
      } else {
        createPoolAssignmentJobs(uploadName, modelLineName, totalShards)
      }
    check(jobsByShard.size == totalShards && jobsByShard.keys == (0 until totalShards).toSet()) {
      "Expected $totalShards PoolAssignmentJobs for $modelLineName; found shards " +
        jobsByShard.keys.sorted()
    }
    return jobsByShard
  }

  private suspend fun createPoolAssignmentJobs(
    uploadName: String,
    modelLineName: String,
    totalShards: Int,
  ): Map<Int, String> {
    val jobsByShard = mutableMapOf<Int, String>()
    for (shardChunk in (0 until totalShards).chunked(maxJobsPerBatchCreate)) {
      val response =
        rpcThrottlers.metadataWrite.onReady {
          poolAssignmentJobsStub.batchCreatePoolAssignmentJobs(
            batchCreatePoolAssignmentJobsRequest {
              parent = uploadName
              for (shardIndex in shardChunk) {
                requests += createPoolAssignmentJobRequest {
                  parent = uploadName
                  poolAssignmentJob = poolAssignmentJob {
                    cmmsModelLine = modelLineName
                    this.shardIndex = shardIndex
                  }
                  requestId = RequestIds.forPoolAssignmentJob(uploadName, modelLineName, shardIndex)
                }
              }
            }
          )
        }
      for (job in response.poolAssignmentJobsList) {
        jobsByShard[job.shardIndex] = job.name
      }
    }
    return jobsByShard
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_MAX_JOBS_PER_BATCH_CREATE = 50
  }
}
