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

import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

/**
 * [TestRule] that ensures that the given [AutoCloseable] resource is closed after statement
 * evaluation.
 *
 * This can replace [ExternalResource][org.junit.rules.ExternalResource], which does not guarantee
 * that [after][org.junit.rules.ExternalResource.after] is invoked if
 * [before][org.junit.rules.ExternalResource.before] throws an exception.
 */
open class CloseableResource<T : AutoCloseable>(private val createResource: () -> T) : TestRule {
  protected lateinit var resource: T
    private set

  override fun apply(base: Statement, description: Description) = object : Statement() {
    override fun evaluate() {
      check(!::resource.isInitialized)

      resource = createResource()
      resource.use { base.evaluate() }
    }
  }
}
