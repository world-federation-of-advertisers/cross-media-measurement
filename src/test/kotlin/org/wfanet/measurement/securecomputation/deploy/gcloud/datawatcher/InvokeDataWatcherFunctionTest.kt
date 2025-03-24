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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.testing.InvokeAbstractDataWatcherFunctionTest

class InvokeDataWatcherFunctionTest : InvokeAbstractDataWatcherFunctionTest() {
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
      "InvokeDataWatcherFunction_deploy.jar",
    )
  // Note that this target is expected to return
  // INFO: Process output: io.grpc.StatusRuntimeException: UNKNOWN: An unknown error occurred: Your
  // default credentials were not found. To set up Application Default Credentials for your
  // environment, see https://cloud.google.com/docs/authentication/external/set-up-adc.
  //
  // The cloud function does not currently return a 500 as documented.
  override val gcfTarget =
    "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"
  override val additionalFlags = emptyMap<String, String>()
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
