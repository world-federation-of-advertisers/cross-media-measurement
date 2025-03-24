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

import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.getPropertyValue

/*
 * Initializes a DataWatcherFunction that calls a pub sub emulator.
 */
class TestDataWatcherFunction() :
  DataWatcherFunction(
    workItemsService =
      lazy {
        val host = getPropertyValue("GOOGLE_PUB_SUB_EMULATOR_HOST")
        val port = getPropertyValue("GOOGLE_PUB_SUB_EMULATOR_PORT").toInt()
        val googlePubSubClient = GooglePubSubEmulatorClient(host, port)
        val projectId = getPropertyValue("CONTROL_PLANE_PROJECT_ID")
        GooglePubSubWorkItemsService(projectId, googlePubSubClient)
      }
  )
