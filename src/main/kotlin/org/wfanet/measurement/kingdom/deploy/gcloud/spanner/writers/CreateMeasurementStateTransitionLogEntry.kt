package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementStateLogEntry

private data class MeasurementStateTransition(
  val priorMeasurementState: Int,
  val currentMeasurementState: Int
)
internal suspend fun SpannerWriter.TransactionScope.createMeasurementStateTransitionLogEntry(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  nextState: Measurement.State,
) {

  val previousStateTransition = getPreviousState(measurementConsumerId, measurementId)
  val priorMeasurementState =
    previousStateTransition?.currentMeasurementState ?: Measurement.State.STATE_UNSPECIFIED.number

  insertMeasurementLogEntry(measurementId, measurementConsumerId)

  insertMeasurementStateTransitionLogEntry(
    measurementId,
    measurementConsumerId,
    priorMeasurementState.toLong(),
    nextState.number.toLong()
  )

  val pippo: Int = nextState.number

}

internal fun SpannerWriter.TransactionScope.insertMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
) {

  transactionContext.bufferInsertMutation("MeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
  }
}

private fun SpannerWriter.TransactionScope.insertMeasurementStateTransitionLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
  priorMeasurementState: Long,
  currentMeasurementState: Long
) {

  val pippo : Int = Measurement.State.STATE_UNSPECIFIED.number

  transactionContext.bufferInsertMutation("StateTransitionMeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("PriorMeasurementState" to InternalId(priorMeasurementState))
    set("CurrentMeasurementState" to InternalId(currentMeasurementState))
  }
}

private fun translateToInternalStates(struct: Struct): MeasurementStateTransition =
  MeasurementStateTransition(
    struct.getLong("PriorMeasurementState").toInt(),
    struct.getLong("CurrentMeasurementState").toInt()
  )

private suspend fun SpannerWriter.TransactionScope.getPreviousState(
  measurementId: InternalId,
  measurementConsumerId: InternalId
): MeasurementStateTransition? {

  return transactionContext
    .executeQuery(
      Statement.newBuilder(
        """
        SELECT
          StateTransitionMeasurementLogEntries.PriorMeasurementState,
          StateTransitionMeasurementLogEntries.CurrentMeasurementState,
        FROM StateTransitionMeasurementLogEntries
        WHERE StateTransitionMeasurementLogEntries.MeasurementConsumerId = $measurementConsumerId
        AND StateTransitionMeasurementLogEntries.MeasurementId = $measurementId
        ORDER BY ExchangeSteps.StepIndex DESC
        LIMIT 1
      """
          .trimIndent()
      )
        .build()
    )
    .map(::translateToInternalStates)
    .singleOrNull()
}

