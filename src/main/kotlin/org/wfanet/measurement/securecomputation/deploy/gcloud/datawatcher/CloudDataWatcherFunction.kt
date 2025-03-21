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

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigs
import com.google.protobuf.TextFormat
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient

/*
 * The DataWatcher receives cloud events when data is written to storage.
 * It then launches sends work to the Control Plane or a Cloud Function to do that work.
 * Reads a config with regexes mapping to different work types and config.
 */
class CloudDataWatcherFunction : DataWatcherFunction() {

  override val schema = "gs://"
  override val workItemsService by lazy {
    val projectId = System.getenv("CONTROL_PLANE_PROJECT_ID")
    val googlePubSubClient = DefaultGooglePubSubClient()
    GooglePubSubWorkItemsService(projectId, googlePubSubClient)
  }

}
