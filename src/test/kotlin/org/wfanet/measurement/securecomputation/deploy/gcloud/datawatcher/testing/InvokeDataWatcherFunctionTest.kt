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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import io.netty.handler.ssl.ClientAuth
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.nio.file.Paths

@RunWith(JUnit4::class)
class InvokeDataWatcherFunctionTest() {

  val functionBinaryPath =
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
      "InvokeDataWatcherFunction_deploy.jar",
    )
  override val gcfTarget =
    "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"
  override val additionalFlags = emptyMap<String, String>()
  override val projectId = "some-project-id"
  override val topicId = "some-topic-id"
}
