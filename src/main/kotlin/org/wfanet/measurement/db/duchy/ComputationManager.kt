// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import java.io.IOException
import java.nio.file.Paths
import kotlin.random.Random

/**
 * Manages status and stage transitions for ongoing computations.
 *
 * @param[StageT] enum of the stages of a computation.
 */
abstract class ComputationManager<StageT : Enum<StageT>>(
  private val relationalDatabase: ComputationsRelationalDb<StageT, ComputationStageDetails>,
  private val blobDatabase: ComputationsBlobDb<StageT>
) {

  /**
   * Creates a new computation.
   *
   * @throws [IOException] upon failure
   */
  suspend fun createComputation(globalId: Long, stage: StageT) {
    relationalDatabase.insertComputation(globalId, stage)
  }

  /**
   * Transitions the stage of an ongoing computation.
   *
   * This can be thought of as an atomic transaction, at least on the underlying
   * [ComputationsRelationalDb]. BLOBs are written first via a [ComputationsBlobDb], and then the
   * stage and work queue is updated in a single transaction. The new stage for the computation is
   * in a clean stage and is thus ready for whatever processing is required, by a worker. This means
   * all input BLOBs are present at the end of this operation and all of those BLOBS' references are
   * known for the stage.
   *
   * Because each task knows the input blobs required to complete it, the set of BLOBs required by
   * the task are all referenced here. The BLOBs should be present before calling this function.
   *
   * @param[token] The task currently being worked
   * @param[stageAfter] The stage to transition the computation to
   * @param[inputBlobsPaths] List of pathes ot all input BLOBs for the new stage. They must exist.
   * @param[outputBlobCount] The number of BLOBs which should be written as part of this stage,
   *    this may be useful when a stage is waiting on inputs from multiple other workers.
   * @param[afterTransition] What to do with the work after transitioning the stage.
   * @param[nextStageDetails] details specific to the next stage to include in the database.
   *
   * @throws [IOException] when stage stage transition fails
   */
  suspend fun transitionStage(
    token: ComputationStorageEditToken<StageT>,
    stageAfter: StageT,
    inputBlobsPaths: List<String> = listOf(),
    outputBlobCount: Int = 0,
    afterTransition: AfterTransition,
    nextStageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance()
  ) {
    return relationalDatabase.updateComputationStage(
      token = token,
      nextStage = stageAfter,
      inputBlobPaths = inputBlobsPaths,
      outputBlobs = outputBlobCount,
      afterTransition = afterTransition,
      nextStageDetails = nextStageDetails
    )
  }

  /**
   * Transitions the stage of an ongoing computation to an ending state.
   *
   * @param[token] The task currently being worked
   * @param[endingStage] The terminal stage to transition the computation to
   * @param [endComputationReason] The reason why the computation is ending
   */
  suspend fun endComputation(
    token: ComputationStorageEditToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  ) {
    relationalDatabase.endComputation(token, endingStage, endComputationReason)
  }

  /**
   * Enqueues a computation into the work queue.
   *
   * @throws [IOException] upon failure
   */
  suspend fun enqueue(token: ComputationStorageEditToken<StageT>) {
    relationalDatabase.enqueue(token)
  }

  /**
   * Pull for available work and claim a task for the worker.
   *
   * If the returned value is present, then the task has been claimed for the worker. When absent,
   * no task was claimed.
   */
  suspend fun claimWork(workerId: String): Long? {
    return relationalDatabase.claimTask(workerId)
  }

  /**
   * Write a BLOB to blob storage, and then update the reference to it in the relational data
   * base for a stage.
   *
   * @throws [IOException] upon failure
   */
  suspend fun writeAndRecordOutputBlob(
    token: ComputationStorageEditToken<StageT>,
    blobName: BlobRef,
    blob: ByteArray
  ) {
    blobDatabase.blockingWrite(blobName, blob)
    relationalDatabase.writeOutputBlobReference(token, blobName)
  }

  /**
   * Convenience function to get a path were a new blob may be written.
   * Returns a path to that can be used for writing a Blob of the form localId/stage/name/randomId.
   */
  fun newBlobPath(
    // TODO(fashing) This doesn't need the entire token. Refactor it to just the input it needs.
    token: ComputationStorageEditToken<StageT>,
    name: String,
    random: Random = Random
  ): String {
    val hexValue = random.nextLong(until = Long.MAX_VALUE).toString(16)
    return Paths.get(token.localId.toString(), token.stage.name, name, hexValue).toString()
  }
}
