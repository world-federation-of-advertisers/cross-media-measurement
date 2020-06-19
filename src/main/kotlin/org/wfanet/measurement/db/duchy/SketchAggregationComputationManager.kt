package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.SketchAggregationState.ADDING_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.CLEAN_UP
import org.wfanet.measurement.internal.SketchAggregationState.COMBINING_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.COMPUTING_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.DECRYPTING_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.FLAG_COUNTS_DECRYPTED
import org.wfanet.measurement.internal.SketchAggregationState.METRICS_COMPUTED
import org.wfanet.measurement.internal.SketchAggregationState.NOISE_ADDED
import org.wfanet.measurement.internal.SketchAggregationState.POSITIONS_BLINDED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.REGISTERS_COMBINED
import org.wfanet.measurement.internal.SketchAggregationState.TRANSMITTED_SKETCH
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.ComputationStageDetails

/**
 * [ComputationManager] specific to running the Privacy-Preserving Secure Cardinality and
 * Frequency Estimation protocol using sparse representation of
 * Cascading Legions Cardinality Estimator sketches.
 */
class SketchAggregationComputationManager(
  relationalDatabase: ComputationsRelationalDb<SketchAggregationState, ComputationStageDetails>,
  blobDatabase: ComputationsBlobDb<SketchAggregationState>,
  private val duchiesInComputation: Int
) : ComputationManager<SketchAggregationState, ComputationStageDetails>(
  relationalDatabase,
  blobDatabase
) {

  /**
   * Calls [transitionState] to move to a new stage in a consistent way.
   *
   * The assumption is this will only be called by a job that is executing the stage of a
   * computation, which is why the inputs and outputs of the stage are passed in and not retrieved
   * from storage.
   */
  fun transitionComputationToStage(
    token: ComputationToken<SketchAggregationState>,
    inputsToCurrentStage: List<String>,
    outputsToCurrentStage: List<String>,
    stage: SketchAggregationState
  ): ComputationToken<SketchAggregationState> {
    return when (stage) {
      // Stages which signal the start of a long running CPU intensive job which immediately
      // follow receiving a sketch from another duchy.
      ADDING_NOISE,
      BLINDING_POSITIONS,
      DECRYPTING_FLAG_COUNTS -> {
        transitionState(
          token,
          stage,
          // The current stage is a RECEIVED* stage which generate no outputs.
          inputBlobsPaths = inputsToCurrentStage,
          outputBlobCount = 1,
          // The peasant has more work to do, either send it to next duchy or continue working
          // if this is the primary.
          afterTransition = AfterTransition.CONTINUE_WORKING
        )
      }
      // Stages which signal the start of a long running CPU intensive job which immediately
      // follow the completion of another long running CPU intensive job at the same duchy.
      COMBINING_REGISTERS,
      COMPUTING_METRICS -> {
        transitionState(
          token,
          stage,
          // Continue with the outputs of the previous stage.
          inputBlobsPaths = outputsToCurrentStage,
          outputBlobCount = 1,
          // The peasant has more work to do, e.g., send it to next duchy.
          afterTransition = AfterTransition.CONTINUE_WORKING
        )
      }
      // Stages signifying the end of a long running computationally expensive job.
      NOISE_ADDED,
      POSITIONS_BLINDED,
      FLAG_COUNTS_DECRYPTED,
      REGISTERS_COMBINED,
      METRICS_COMPUTED -> {
        transitionState(
          token,
          stage,
          // The output of the last job is kept around as it is the finished work.
          inputBlobsPaths = outputsToCurrentStage,
          // The peasant has more work to do, either send it to next duchy or continue working
          // if this is the primary.
          afterTransition = AfterTransition.CONTINUE_WORKING
        )
      }
      // Stages were the duchy is waiting for a single input from the predecessor duchy.
      WAIT_CONCATENATED,
      WAIT_JOINED -> {
        transitionState(
          token,
          stage,
          // Keep a reference to the finished work artifact in case it needs to be resent.
          inputBlobsPaths = inputsToCurrentStage,
          // Requires an output to be written e.g., the sketch sent by the predecessor duchy.
          outputBlobCount = 1,
          // Peasant have nothing to do for this stage.
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
        )
      }
      WAIT_FINISHED -> {
        transitionState(
          token,
          stage,
          // Keep a reference to the finished work artifact in case it needs to be resent.
          inputBlobsPaths = inputsToCurrentStage,
          // Peasant have nothing to do for this stage.
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
        )
      }
      // Stages were the duchy finished receiving all inputs from other duchies.
      RECEIVED_SKETCHES,
      RECEIVED_CONCATENATED,
      RECEIVED_JOINED -> {
        transitionState(
          token,
          stage,
          // The sketch received from the last duchy.
          inputBlobsPaths = outputsToCurrentStage,
          // This will be called by an gRPC handler, so it gets added to the peasant work queue.
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        )
      }
      TRANSMITTED_SKETCH -> {
        transitionState(
          token,
          stage,
          // Save a reference to the noised sketches in case they need to tbe resent.
          inputBlobsPaths = inputsToCurrentStage,
          // Job will be moved into a Wait stage.
          afterTransition = AfterTransition.CONTINUE_WORKING
        )
      }
      // The primary duchy is waiting for input from all the other duchies. This is a special case
      // of the other wait stages as it has n-1 inputs.
      WAIT_SKETCHES -> {
        transitionState(
          token,
          stage,
          outputBlobCount = duchiesInComputation - 1,
          // Need to wait for input from other duchies.
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
        )
      }
      CLEAN_UP -> {
        transitionState(
          token,
          stage,
          // Some peasant needs to clean up after the computation.
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        )
      }
      FINISHED -> {
        transitionState(token, stage, afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE)
      }
      else -> error("Cannot make transition function to stage $stage")
    }
  }
}
