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

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.util.UUID
import java.util.logging.Logger
import kotlin.text.matches
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/*
 * Watcher to observe blob creation events and take the appropriate action for each.
 * @param workItemsStub - the Google Pub Sub Sink to call
 * @param dataWatcherConfigs - a list of [DataWatcherConfig]
 */
class DataWatcher(
  private val workItemsStub: WorkItemsCoroutineStub,
  private val dataWatcherConfigs: List<DataWatcherConfig>,
) {
  suspend fun receivePath(path: String) {
    for (config in dataWatcherConfigs) {
      try {
        val regex = config.sourcePathRegex.toRegex()
        if (regex.matches(path)) {
          logger.info("${config.name}: Matched path: $path")
          when (config.sinkConfigCase) {
            DataWatcherConfig.SinkConfigCase.CONTROL_PLANE_QUEUE_SINK -> {
              send_to_control_plane(config, path)
            }
            DataWatcherConfig.SinkConfigCase.HTTP_ENDPOINTS_SINK -> {
              send_to_http_endpoint(config)
            }
            DataWatcherConfig.SinkConfigCase.SINKCONFIG_NOT_SET ->
              error("${config.name}: Invalid sink config: ${config.sinkConfigCase}")
          }
        }
      } catch (e: Exception) {
        logger.severe("${config.name}: Unable to process $path for $config: ${e.message}")
      }
    }
  }

  private suspend fun send_to_control_plane(config: DataWatcherConfig, path: String) {

    val queueConfig = config.controlPlaneQueueSink
    val workItemId = UUID.randomUUID().toString()
    val workItemParams =
      workItemParams {
          appParams = queueConfig.appParams
          this.dataPathParams = dataPathParams { this.dataPath = path }
        }
        .pack()
    val request = createWorkItemRequest {
      this.workItemId = workItemId
      this.workItem = workItem {
        queue = queueConfig.queue
        this.workItemParams = workItemParams
      }
    }
    workItemsStub.createWorkItem(request)
  }

  private suspend fun send_to_http_endpoint(config: DataWatcherConfig) {
    val httpEndpointConfig = config.httpEndpointsSink
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create(httpEndpointConfig.endpointUri))
        .POST(HttpRequest.BodyPublishers.ofString(httpEndpointConfig.appParams.toJson()))
        .build()
    val response = client.send(request, BodyHandlers.ofString())
    logger.info("${config.name}: Response status: ${response.statusCode()}")
    logger.info("${config.name}: Response body: ${response.body()}")
    check(response.statusCode() == 200)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
