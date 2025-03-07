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
import java.nio.charset.StandardCharsets
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import kotlin.text.matches

/*
 * The DataWatcher receives cloud events when data is written to storage.
 * It then launches sends work to the Control Plane or a Cloud Function to do that work.
 * Reads a config with regexes mapping to different work types and config.
 */
class DataWatcher(
  private val workItemsService: GooglePubSubWorkItemsService,
  private val dataWatcherConfigs: List<DataWatcherConfig>
) : CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    val cloudEventData = requireNotNull(event.getData()) { "event must have data" }.toBytes().decodeToString()
    val data = StorageObjectData.newBuilder().apply {
      JsonFormat.parser().merge(cloudEventData, this)
    }.build()
    val blobKey: String = data.getName()
    val bucket: String = data.getBucket()
    val path = "${bucket}://${blobKey}"
    for (config in dataWatcherConfigs) {
      val regex = config.sourcePathRegex.toRegex()
      if (regex.matches(path)) {
        when (config.sinkConfigCase) {
          DataWatcherConfig.SinkConfigCase.CONTROL_PLANE_CONFIG -> {
            val queueConfig = config.controlPlaneConfig
            val workItemId = UUID.randomUUID().toString()
            val workItemParams =
              workItemConfig {
                  this.config = queueConfig.appConfig
                  this.dataPath = path
                }
                .pack()
            val request = createWorkItemRequest {
              this.workItemId = workItemId
              this.workItem = workItem {
                queue = queueConfig.queueName
                this.workItemParams = workItemParams
              }
            }
            runBlocking { workItemsService.createWorkItem(request) }
          }
          DataWatcherConfig.SinkConfigCase.CLOUD_FUNCTION_CONFIG ->
            TODO("Cloud Function Sink not currently supported")
          DataWatcherConfig.SinkConfigCase.SINKCONFIG_NOT_SET ->
            error("Invalid sink config: ${config.sinkConfigCase}")
        }
      }
    }
  }
}
