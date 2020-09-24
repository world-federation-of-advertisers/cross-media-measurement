// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
