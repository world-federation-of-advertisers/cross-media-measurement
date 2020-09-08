package org.wfanet.measurement.integration

/**
 * Implementation of [InProcessKingdomAndDuchyIntegrationTest] for GCP backends (Spanner, GCS).
 */
class GcpInProcessKingdomAndDuchyIntegrationTest : InProcessKingdomAndDuchyIntegrationTest() {
  override val kingdomRelationalDatabaseRule by lazy { GcpKingdomRelationalDatabaseProviderRule() }
  override val duchyDependenciesRule by lazy { GcpDuchyDependencyProviderRule(DUCHY_IDS) }
}
