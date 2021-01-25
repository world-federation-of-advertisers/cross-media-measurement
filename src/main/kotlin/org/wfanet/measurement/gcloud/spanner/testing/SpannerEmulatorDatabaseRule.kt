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

package org.wfanet.measurement.gcloud.spanner.testing

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.DatabaseClient
import java.util.concurrent.atomic.AtomicInteger
import org.junit.rules.TestRule
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.asAsync
import org.wfanet.measurement.gcloud.spanner.createDatabase

/**
 * JUnit rule exposing a temporary Google Cloud Spanner database.
 *
 * All instances share a single [SpannerEmulator].
 */
class SpannerEmulatorDatabaseRule(schema: SpannerSchema) :
  DatabaseRule by DatabaseRuleImpl(schema)

private interface DatabaseRule : TestRule {
  val databaseClient: AsyncDatabaseClient
}

private class DatabaseRuleImpl(schema: SpannerSchema) :
  DatabaseRule,
  CloseableResource<TemporaryDatabase>({ TemporaryDatabase(schema) }) {

  override val databaseClient: AsyncDatabaseClient
    get() = resource.databaseClient.asAsync()
}

private class TemporaryDatabase(schema: SpannerSchema) : AutoCloseable {

  private val database: Database
  init {
    val databaseName = "test-db-${instanceCounter.incrementAndGet()}"
    database = schema.definitionReader().useLines { lines ->
      createDatabase(emulator.instance, lines, databaseName)
    }
  }

  val databaseClient: DatabaseClient by lazy {
    emulator.getDatabaseClient(database.id)
  }

  override fun close() {
    database.drop()
  }

  companion object {
    /** Atomic counter to ensure each instance has a unique name. */
    private val instanceCounter = AtomicInteger(0)

    private val emulator = EmulatorWithInstance()
  }
}
