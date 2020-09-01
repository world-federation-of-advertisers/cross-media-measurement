package org.wfanet.measurement.integration

/**
 * Implementation of [InProcessKingdomIntegrationTest] for GCP backends (Spanner, GCS).
 */
class GcpInProcessKingdomIntegrationTest : InProcessKingdomIntegrationTest() {
  override val kingdomRelationalDatabaseRule by lazy { GcpKingdomRelationalDatabaseProviderRule() }
}
