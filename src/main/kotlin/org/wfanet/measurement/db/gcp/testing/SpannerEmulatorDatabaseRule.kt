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

package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import java.util.concurrent.atomic.AtomicInteger
import org.junit.rules.TestRule
import org.wfanet.measurement.common.testing.CloseableResource

/**
 * JUnit rule for [SpannerEmulator] instance.
 */
class SpannerEmulatorDatabaseRule(spannerInstance: Instance, schemaResourcePath: String) :
  DatabaseRule by DatabaseRuleImpl(spannerInstance, schemaResourcePath)

private interface DatabaseRule : TestRule {
  val databaseId: DatabaseId
}

private class DatabaseRuleImpl(spannerInstance: Instance, schemaResourcePath: String) :
  DatabaseRule,
  CloseableResource<TemporaryDatabase>({ TemporaryDatabase(spannerInstance, schemaResourcePath) }) {

  override val databaseId: DatabaseId
    get() = resource.databaseId
}

private class TemporaryDatabase(spannerInstance: Instance, schemaResourcePath: String) :
  AutoCloseable {

  private val database: Database
  init {
    val databaseName = "test-db-${instanceCounter.incrementAndGet()}"
    val ddl = TemporaryDatabase::class.java.getResource(schemaResourcePath).readText()
    database = createDatabase(spannerInstance, ddl, databaseName)
  }

  val databaseId: DatabaseId
    get() = database.id

  override fun close() {
    if (database.exists()) {
      database.drop()
    }
  }

  companion object {
    /** Atomic counter to ensure each instance has a unique name. */
    private val instanceCounter = AtomicInteger(0)
  }
}
