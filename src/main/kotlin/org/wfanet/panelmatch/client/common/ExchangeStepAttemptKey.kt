package org.wfanet.panelmatch.client.common

/** A key that uniquely identifies a [org.wfanet.panelmatch.client.internal.ExchangeStepAttempt]. */
data class ExchangeStepAttemptKey(
  val recurringExchangeId: String,
  val exchangeId: String,
  val stepId: String,
  val attemptId: String,
  val simpleName: String,
)
