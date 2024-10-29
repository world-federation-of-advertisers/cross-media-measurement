package org.wfanet.measurement.integration.deploy.gcloud

import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.Timeout
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.InProcessMeasurementSystemProberIntegrationTest

/**
 * Implementation of [InProcessMeasurementSystemProberIntegrationTest] for GCloud backends with
 * Spanner database.
 */
class GCloudSpannerInProcessMeasurementSystemProberIntegrationTest :
  InProcessMeasurementSystemProberIntegrationTest(
    KingdomDataServicesProviderRule(spannerEmulator),
    SpannerDuchyDependencyProviderRule(spannerEmulator, ALL_DUCHY_NAMES),
  ) {
  /**
   * Rule to enforce test method timeout.
   *
   * TODO(Kotlin/kotlinx.coroutines#3865): Switch back to CoroutinesTimeout when fixed.
   */
  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
