package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry.Details

private val DEFAULT_INITIAL_STATE by lazy { InternalId(-1L) }
private val MEASUREMENT_LOG_DETAILS by lazy { Details.getDefaultInstance() }


private data class MeasurementStateTransition(
  val priorMeasurementState: InternalId,
  val currentMeasurementState: InternalId
)
internal suspend fun SpannerWriter.TransactionScope.createMeasurementStateTransitionLogEntry(
  measurementConsumerId: InternalId,
  measurementId: InternalId,
  nextState: Measurement.State,
) {

  val previousStateTransition = getPreviousState(measurementId, measurementConsumerId)
  val priorMeasurementState = previousStateTransition?.currentMeasurementState ?: DEFAULT_INITIAL_STATE
  val nextMeasurementState = InternalId(nextState.numberAsLong)

  if (priorMeasurementState != nextMeasurementState) {

    insertMeasurementLogEntry(measurementId, measurementConsumerId)

    insertMeasurementStateTransitionLogEntry(
      measurementId,
      measurementConsumerId,
      priorMeasurementState,
      nextMeasurementState
    )

  }

}

internal fun SpannerWriter.TransactionScope.insertMeasurementLogEntry(
  measurementId: InternalId,
  measurementConsumerId: InternalId,
) {

  transactionContext.bufferInsertMutation("MeasurementLogEntries") {
    set("MeasurementConsumerId" to measurementConsumerId)
    set("MeasurementId" to measurementId)
    set("CreateTime" to Value.COMMIT_TIMESTAMP)
    set("MeasurementLogDetails" to MEASUREMENT_LOG_DETAILS)
    setJson("MeasurementLogDetailsJson" to MEASUREMENT_LOG_DETAILS)
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
    InternalId(struct.getLong("PriorMeasurementState")),
    InternalId(struct.getLong("CurrentMeasurementState"))
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

