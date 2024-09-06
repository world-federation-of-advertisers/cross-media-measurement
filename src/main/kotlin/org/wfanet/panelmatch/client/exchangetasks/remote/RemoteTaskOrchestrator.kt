package org.wfanet.panelmatch.client.exchangetasks.remote

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey

interface RemoteTaskOrchestrator {
  suspend fun orchestrateTask(
    exchangeWorkflowId: String,
    exchangeStepIndex: Int,
    exchangeStepAttempt: ExchangeStepAttemptKey,
    exchangeDate: LocalDate,
  )
}
