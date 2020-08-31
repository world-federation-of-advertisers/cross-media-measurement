package org.wfanet.measurement.common.identity.testing

import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.DuchyIds

/**
 * JUnit rule that sets the global list of all valid Duchy ids to [duchyIds].
 */
class DuchyIdSetter(private vararg val duchyIds: String) : TestRule {
  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        DuchyIds.setDuchyIdsForTest(duchyIds.toSet())
        base.evaluate()
      }
    }
  }
}
