/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.testing

import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider

class InvokeTestDataWatcherFunctionTest : InvokeAbstractDataWatcherFunctionTest() {
  override val functionBinaryPath =
    Paths.get(
      "wfa_measurement_system",
      "src",
      "main",
      "kotlin",
      "org",
      "wfanet",
      "measurement",
      "securecomputation",
      "deploy",
      "gcloud",
      "datawatcher",
      "testing",
      "InvokeTestDataWatcherFunction_deploy.jar",
    )
  override val gcfTarget =
    "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.testing.TestDataWatcherFunction"
  override val additionalFlags =
    mapOf<String, String>(
      "GOOGLE_PUB_SUB_EMULATOR_HOST" to pubSubEmulatorProvider.host,
      "GOOGLE_PUB_SUB_EMULATOR_PORT" to pubSubEmulatorProvider.port.toString(),
    )
  override val projectId = "some-project-id"
  override val topicId = "some-topic-id"

  @Before
  fun initPubSub() {
    val googlePubSubClient =
      GooglePubSubEmulatorClient(pubSubEmulatorProvider.host, pubSubEmulatorProvider.port)
    runBlocking { googlePubSubClient.createTopic(projectId, topicId) }
  }

  companion object {

    @ClassRule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
  }
}
