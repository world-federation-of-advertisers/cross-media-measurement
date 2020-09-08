package org.wfanet.measurement.common.testing

import org.junit.rules.RuleChain
import org.junit.rules.TestRule

/**
 * Chains JUnit rules together so they are executed in the order passed.
 */
fun chainRulesSequentially(rules: List<TestRule>): RuleChain {
  return rules.fold(RuleChain.emptyRuleChain()) { chain, rule -> chain.around(rule) }
}

/**
 * Chains JUnit rules together so they are executed in the order passed.
 */
fun chainRulesSequentially(vararg rules: TestRule): RuleChain {
  return chainRulesSequentially(rules.toList())
}
