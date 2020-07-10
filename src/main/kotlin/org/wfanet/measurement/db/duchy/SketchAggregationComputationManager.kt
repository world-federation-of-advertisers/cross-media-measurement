package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.SketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationStage.CREATED
import org.wfanet.measurement.internal.SketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_APPEND_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationStage.UNKNOWN
import org.wfanet.measurement.internal.SketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.ComputationStageDetails

/**
 * [ComputationManager] specific to running the Privacy-Preserving Secure Cardinality and
 * Frequency Estimation protocol using sparse representation of
 * Cascading Legions Cardinality Estimator sketches.
 */
class SketchAggregationComputationManager(
  relationalDatabase: ComputationsRelationalDb<SketchAggregationStage, ComputationStageDetails>,
  blobDatabase: ComputationsBlobDb<SketchAggregationStage>,
  private val duchiesInComputation: Int
) : ComputationManager<SketchAggregationStage, ComputationStageDetails>(
  relationalDatabase,
  blobDatabase
) {

  /**
   * Calls [transitionStage] to move to a new stage in a consistent way.
   *
   * The assumption is this will only be called by a job that is executing the stage of a
   * computation, which will have knowledge of all the data needed as input to the next stage.
   * Most of the time [inputsToNextStage] is the list of outputs of the currently running stage.
   */
  suspend fun transitionComputationToStage(
    token: ComputationToken<SketchAggregationStage>,
    inputsToNextStage: List<String> = listOf(),
    stage: SketchAggregationStage
  ): ComputationToken<SketchAggregationStage> {
    requireValidRoleForStage(stage, token.role)
    return when (stage) {
      // Stages of computation mapping some number of inputs to single output.
      TO_APPEND_SKETCHES,
      TO_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS -> transitionStage(
        token,
        stage,
        inputBlobsPaths = requireNotEmpty(inputsToNextStage),
        outputBlobCount = 1,
        afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
      )
      // The primary duchy is waiting for input from all the other duchies. This is a special case
      // of the other wait stages as it has n-1 inputs.
      WAIT_SKETCHES -> transitionStage(
        token,
        stage,
        // The output of current stage is the results of adding noise to locally stored sketches.
        inputBlobsPaths = requireNotEmpty(inputsToNextStage),
        outputBlobCount = duchiesInComputation - 1,
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
      // Stages were the duchy is waiting for a single input from the predecessor duchy.
      WAIT_CONCATENATED,
      WAIT_FLAG_COUNTS -> transitionStage(
        token,
        stage,
        // Keep a reference to the finished work artifact in case it needs to be resent.
        inputBlobsPaths = requireNotEmpty(inputsToNextStage),
        // Requires an output to be written e.g., the sketch sent by the predecessor duchy.
        outputBlobCount = 1,
        // Peasant have nothing to do for this stage.
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
      COMPLETED -> error("Computation should be ended with call to endComputation(...)")
      // Stages that we can't transition to ever.
      UNRECOGNIZED, UNKNOWN, CREATED -> error("Cannot make transition function to stage $stage")
    }
  }

  private fun requireValidRoleForStage(stage: SketchAggregationStage, role: DuchyRole) {
    when (stage) {
      WAIT_SKETCHES,
      TO_APPEND_SKETCHES,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS -> require(role == DuchyRole.PRIMARY) {
        "$stage may only be executed by the primary MPC worker."
      }
      TO_BLIND_POSITIONS, TO_DECRYPT_FLAG_COUNTS -> require(role == DuchyRole.SECONDARY) {
        "$stage may only be executed by a non-primary MPC worker."
      }
      else -> { /* Stage can be executed at either primary or non-primary */ }
    }
  }

  /**
   * Writes the concatenated sketch as a blob and update the stage's output blobref to point to
   * the sketch.
   *
   * When the concatenated sketch already exists, no blob is written, but the path to the blob
   * is returned.
   *
   * @return Pair of token after updating blob reference, and path to written blob.
   */
  suspend fun writeReceivedConcatenatedSketch(
    token: ComputationToken<SketchAggregationStage>,
    sketch: ByteArray
  ): Pair<ComputationToken<SketchAggregationStage>, String> {
    return writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_CONCATENATED,
      nameForBlob = "concatenated_sketch",
      token = token,
      bytes = sketch
    )
  }

  suspend fun writeReceivedFlagsAndCounts(
    token: ComputationToken<SketchAggregationStage>,
    encryptedFlagCounts: ByteArray
  ): Pair<ComputationToken<SketchAggregationStage>, String> {
    return writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_FLAG_COUNTS,
      nameForBlob = "encrypted_flag_counts",
      token = token,
      bytes = encryptedFlagCounts
    )
  }

  private suspend fun writeExpectedBlobIfNotPresent(
    requiredStage: SketchAggregationStage,
    nameForBlob: String,
    token: ComputationToken<SketchAggregationStage>,
    bytes: ByteArray
  ): Pair<ComputationToken<SketchAggregationStage>, String> {
    require(token.stage == requiredStage) {
      "Cannot accept $nameForBlob while in stage ${token.stage}"
    }
    val outputBlob =
      readBlobReferences(token, BlobDependencyType.OUTPUT).asSequence().single()
    val existingPath = outputBlob.value

    // Return the path to the already written blob if one exists.
    if (existingPath != null) return Pair(token, existingPath)

    // Write the blob to a new path if there is not already a reference saved for it in
    // the relational database.
    val newPath = newBlobPath(token, nameForBlob)
    writeAndRecordOutputBlob(token, BlobRef(outputBlob.key, newPath), bytes)
    val newToken =
      getToken(token.globalId) ?: error("Computation $token not found after writing output blob")
    return Pair(newToken, newPath)
  }
}

private fun requireNotEmpty(paths: List<String>): List<String> {
  require(paths.isNotEmpty()) { "Passed paths to input blobs is empty" }
  return paths
}
