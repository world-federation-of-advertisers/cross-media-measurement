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

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Instance
import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceId
import com.google.cloud.spanner.InstanceInfo
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import kotlinx.coroutines.runBlocking

/**
 * [AutoCloseable] wrapping a [SpannerEmulator] with a single test [Instance].
 */
internal class EmulatorWithInstance : AutoCloseable {
  private val spannerEmulator = SpannerEmulator().apply { start() }

  private val spanner: Spanner
  init {
    val emulatorHost = runBlocking { spannerEmulator.waitUntilReady() }

    val spannerOptions =
      SpannerOptions.newBuilder().setProjectId(PROJECT_ID).setEmulatorHost(emulatorHost).build()
    spanner = spannerOptions.service
  }

  val instance: Instance
  init {
    instance = spanner.instanceAdminClient.createInstance(
      InstanceInfo.newBuilder(InstanceId.of(PROJECT_ID, INSTANCE_NAME))
        .setDisplayName(INSTANCE_DISPLAY_NAME)
        .setInstanceConfigId(InstanceConfigId.of(PROJECT_ID, INSTANCE_CONFIG))
        .setNodeCount(1)
        .build()
    ).get()
  }

  override fun close() {
    instance.delete()
    spanner.close()
    spannerEmulator.close()
  }

  fun getDatabaseClient(databaseId: DatabaseId): DatabaseClient =
    spanner.getDatabaseClient(databaseId)

  companion object {
    private const val PROJECT_ID = "test-project"
    private const val INSTANCE_NAME = "test-instance"
    private const val INSTANCE_DISPLAY_NAME = "Test Instance"
    private const val INSTANCE_CONFIG = "emulator-config"
  }
}
