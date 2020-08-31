package org.wfanet.measurement.common.testing

import org.junit.rules.TestRule

/**
 * JUnit rule to compute a value.
 */
interface ProviderRule<T : Any> : TestRule {
  /**
   * Value produced by the rule. This must not be accessed before the rule runs.
   */
  val value: T
}
