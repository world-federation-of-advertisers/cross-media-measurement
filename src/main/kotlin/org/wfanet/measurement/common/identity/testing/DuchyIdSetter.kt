// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.identity.testing

import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.identity.DuchyIds

/**
 * JUnit rule that sets the global list of all valid Duchy ids to [duchyIds].
 */
class DuchyIdSetter(val duchyIds: Set<String>) : TestRule {
  constructor(duchyIds: Iterable<String>) : this(duchyIds.toSet())
  constructor(vararg duchyIds: String) : this(duchyIds.toSet())

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        DuchyIds.setDuchyIdsForTest(duchyIds)
        base.evaluate()
      }
    }
  }
}
