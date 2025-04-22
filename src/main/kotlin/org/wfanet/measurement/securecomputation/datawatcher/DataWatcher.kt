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

package org.wfanet.measurement.securecomputation.datawatcher

import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import com.google.events.cloud.storage.v1.StorageObjectData
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import com.google.cloud.functions.CloudEventsFunction
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import java.nio.charset.StandardCharsets
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.common.pack
import com.google.protobuf.Any
import io.cloudevents.CloudEvent
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.UUID
import kotlinx.coroutines.runBlocking

class DataWatcher(
  private val workItemsService: GooglePubSubWorkItemsService,
  private val dataWatcherConfigs: List<DataWatcherConfig>
) : CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    val cloudEventData = String(event.getData().toBytes(), StandardCharsets.UTF_8)
    val builder = StorageObjectData.newBuilder()
    JsonFormat.parser().merge(cloudEventData, builder)
    val data = builder.build()
    val bucket = data.getBucket()
    val blobKey = data.getName()
    dataWatcherConfigs.forEach { config ->
      val regex = config.sourcePathRegex.toRegex()
      if (regex.containsMatchIn(blobKey)) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (config.sinkConfigCase) {
          DataWatcherConfig.SinkConfigCase.CONTROL_PLANE_CONFIG -> {
            val queueConfig = config.controlPlaneConfig
            val workItemId = UUID.randomUUID().toString()
            val workItemParams =
              triggeredApp {
                  this.config = queueConfig.appConfig
                  this.path = path
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
