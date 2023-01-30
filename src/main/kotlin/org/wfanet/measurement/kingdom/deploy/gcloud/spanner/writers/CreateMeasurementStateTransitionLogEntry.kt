package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Measurement

private const val DEFAULT_INITIAL_STATE = -1L
private data class MeasurementStateTransition(
  val priorMeasurementState: Long,
  val currentMeasurementState: Long
)
internal suspend fun SpannerWriter.TransactionScope.createMeasurementStateTransitionLogEntry(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  nextState: Measurement.State,
) {

  val previousStateTransition = getPreviousState(measurementConsumerId, measurementId)
  val priorMeasurementState = previousStateTransition?.currentMeasurementState ?: DEFAULT_INITIAL_STATE

  insertMeasurementLogEntry(measurementId, measurementConsumerId)

  insertMeasurementStateTransitionLogEntry(
    measurementId,
    measurementConsumerId,
    InternalId(priorMeasurementState),
    InternalId(nextState.number.toLong())
  )

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
  priorMeasurementState: InternalId,
  currentMeasurementState: InternalId
) {
  transactionContext.bufferInsertMutation("StateTransitionMeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("PriorMeasurementState" to priorMeasurementState)
    set("CurrentMeasurementState" to currentMeasurementState)
  }
}

private fun translateToInternalStates(struct: Struct): MeasurementStateTransition =
  MeasurementStateTransition(
    struct.getLong("PriorMeasurementState"),
    struct.getLong("CurrentMeasurementState")
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
        WHERE StateTransitionMeasurementLogEntries.MeasurementConsumerId = ${measurementConsumerId.value}
        AND StateTransitionMeasurementLogEntries.MeasurementId = ${measurementId.value}
        ORDER BY StateTransitionMeasurementLogEntries.CreateTime DESC
        LIMIT 1
      """
          .trimIndent()
      )
        .build()
    )
    .map(::translateToInternalStates)
    .singleOrNull()
}

