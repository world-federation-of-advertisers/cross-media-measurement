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
open class CloseableResource<T : AutoCloseable>(lazyResource: () -> T) : TestRule {
  protected val resource by lazy { lazyResource() }

  override fun apply(base: Statement, description: Description) = object : Statement() {
    override fun evaluate() {
      try {
        resource.use {
          before()
          base.evaluate()
        }
      } finally {
        after()
      }
    }
  }

  open fun before() {}

  open fun after() {}
}
