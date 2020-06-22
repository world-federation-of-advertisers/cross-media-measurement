package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.SketchAggregationState.ADDING_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_AND_JOINING_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.CLEAN_UP
import org.wfanet.measurement.internal.SketchAggregationState.COMPUTING_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.DECRYPTING_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.GATHERING_LOCAL_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.STARTING
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.UNKNOWN
import org.wfanet.measurement.internal.SketchAggregationState.UNRECOGNIZED
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
      // Runs after the initialization of the computation, there are not any inputs.
      GATHERING_LOCAL_SKETCHES -> transitionState(
        token,
        stage,
        outputBlobCount = 1,
        afterTransition = AfterTransition.CONTINUE_WORKING
      )
      // Stages which are run immediately after a stage producing output on the same peasant.
      ADDING_NOISE,
      COMPUTING_METRICS -> transitionState(
        token,
        stage,
        inputBlobsPaths = requireNotEmpty(outputsToCurrentStage),
        outputBlobCount = 1,
        afterTransition = AfterTransition.CONTINUE_WORKING
      )
      // Stages where part of the computation is run after receiving inputs from another duchy.
      // These stages follow a RECEIVED_* stage, which do not create output but carry forward the
      // output of the WAIT_* stage.
      //
      // For example, the input to BLINDING_POSITIONS is the concatenated sketch from the
      // predecessor duchy which is the output of WAIT_CONCATENATED. The output of WAIT_CONCATENATED
      // is the input to RECEIVED_CONCATENATED which doesn't produce an output. So the input to
      // BLINDING_POSITIONS is RECEIVED_CONCATENATED.input which is WAIT_CONCATENATED.output
      BLINDING_POSITIONS,
      BLINDING_AND_JOINING_REGISTERS,
      DECRYPTING_FLAG_COUNTS -> transitionState(
        token,
        stage,
        inputBlobsPaths = requireNotEmpty(inputsToCurrentStage),
        outputBlobCount = 1,
        afterTransition = AfterTransition.CONTINUE_WORKING
      )
      // The primary duchy is waiting for input from all the other duchies. This is a special case
      // of the other wait stages as it has n-1 inputs.
      WAIT_SKETCHES -> transitionState(
        token,
        stage,
        // The output of current stage is the results of adding noise to locally stored sketches.
        inputBlobsPaths = requireNotEmpty(outputsToCurrentStage),
        outputBlobCount = duchiesInComputation - 1,
        // Need to wait for input from other duchies.
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
      // Stages were the duchy is waiting for a single input from the predecessor duchy.
      WAIT_CONCATENATED,
      WAIT_JOINED -> transitionState(
        token,
        stage,
        // Keep a reference to the finished work artifact in case it needs to be resent.
        inputBlobsPaths = requireNotEmpty(outputsToCurrentStage),
        // Requires an output to be written e.g., the sketch sent by the predecessor duchy.
        outputBlobCount = 1,
        // Peasant have nothing to do for this stage.
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
      // Waits for a signal to cleanup the computation. The signal itself is not a required input
      // for the next stage, so it doesn't need to be stored.
      WAIT_FINISHED -> transitionState(
        token,
        stage,
        // Keep a reference to the finished work artifact in case it needs to be resent.
        inputBlobsPaths = requireNotEmpty(inputsToCurrentStage),
        // Peasant have nothing to do for this stage.
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
      // Stages were the duchy is making a single output from a collection of data from all duchies.
      RECEIVED_SKETCHES -> transitionState(
        token,
        stage,
        // The input to current stage is the set of locally stored sketches with added noise,
        // the outputs of the current stage are the set of sketches with added noise received
        // from the other duchies.
        inputBlobsPaths = requireNotEmpty(inputsToCurrentStage) + requireNotEmpty(
          outputsToCurrentStage
        ),
        // Creates the first concatenated sketch.
        outputBlobCount = 1,
        // This will be called by an gRPC handler, so it gets added to the peasant work queue.
        afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
      )
      // Stages where the duchy got input from another duchy. These stages make no output themselves
      // but are a way to track when a computation operates on the inputs from another duchy.
      RECEIVED_CONCATENATED,
      RECEIVED_JOINED -> transitionState(
        token,
        stage,
        // The sketch received from the last duchy.
        inputBlobsPaths = requireNotEmpty(outputsToCurrentStage),
        // This will be called by an gRPC handler, so it gets added to the peasant work queue.
        afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
      )
      CLEAN_UP -> // Some peasant needs to clean up after the computation.
        transitionState(token, stage, afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE)
      FINISHED ->
        transitionState(token, stage, afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE)
      // States that we can't transition to ever.
      UNRECOGNIZED, UNKNOWN, STARTING -> error("Cannot make transition function to stage $stage")
    }
  }
}

private fun requireNotEmpty(paths: List<String>): List<String> {
  require(paths.isNotEmpty()) { "Passed paths to input blobs is empty" }
  return paths
}
