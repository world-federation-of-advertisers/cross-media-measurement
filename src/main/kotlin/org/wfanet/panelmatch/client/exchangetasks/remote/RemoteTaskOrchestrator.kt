package org.wfanet.panelmatch.client.exchangetasks.remote

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey

interface RemoteTaskOrchestrator {
  suspend fun orchestrateTask(
    exchangeWorkflowId: String,
    exchangeStepName: String,
    exchangeStepIndex: Int,
    exchangeStepAttempt: CanonicalExchangeStepAttemptKey,
    exchangeDate: LocalDate,
  )
}
