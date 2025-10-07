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

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenProvider
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.time.Instant
import java.util.UUID
import java.util.logging.Logger
import kotlin.time.TimeSource
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
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
  private val dataWatcherConfigs: List<WatchedPath>,
  private val workItemIdGenerator: () -> String = { "work-item-" + UUID.randomUUID().toString() },
  private val idTokenProvider: IdTokenProvider =
    GoogleCredentials.getApplicationDefault() as? IdTokenProvider
      ?: throw IllegalArgumentException("Application Default Credentials do not provide ID token"),
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
  private val metrics: DataWatcherMetrics = DataWatcherMetrics.instance,

) {

  suspend fun receivePath(path: String, blobCreateTime: Instant? = null) {
    logger.info("Received Path: $path")
    for (config in dataWatcherConfigs) {
      try {
        val regex = config.sourcePathRegex.toRegex()
        if (regex.matches(path)) {
          logger.info("${config.identifier}: Matched path: $path")

          val sinkType = config.sinkConfigCase.name
          Tracing.trace(
            spanName = "data_watcher.process",
            attributes =
              mapOf(
                "blob_path" to path,
                "sink_type" to sinkType,
                "config_identifier" to config.identifier,
              ),
          ) {
            when (config.sinkConfigCase) {
              WatchedPath.SinkConfigCase.CONTROL_PLANE_QUEUE_SINK -> {
                sendToControlPlane(config, path)
              }

              WatchedPath.SinkConfigCase.HTTP_ENDPOINT_SINK -> {
                sendToHttpEndpoint(config, path, blobCreateTime)
              }

              WatchedPath.SinkConfigCase.SINKCONFIG_NOT_SET ->
                error("${config.identifier}: Invalid sink config: ${config.sinkConfigCase}")
            }
          }

          blobCreateTime?.let { doneTime ->
            val nowMillis = System.currentTimeMillis()
            val deltaMillis = nowMillis - doneTime.toEpochMilli()
            val latencySeconds = deltaMillis.coerceAtLeast(0).toDouble() / 1000.0
            metrics.recordMatchLatency(sinkType = sinkType, latencySeconds = latencySeconds)
          }
        } else {
          logger.info("$path does not match ${config.sourcePathRegex}")
        }
      } catch (e: Exception) {
        logger.severe("${config.identifier}: Unable to process $path for $config: ${e.message}")
      }
    }
  }

  private suspend fun sendToControlPlane(config: WatchedPath, path: String) {

    val queueConfig = config.controlPlaneQueueSink
    val workItemId = workItemIdGenerator()
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

    metrics.incrementQueueWrites(queueConfig.queue)
    logger.info("${config.identifier}: Created work item $workItemId for queue ${queueConfig.queue}")
  }

  private fun sendToHttpEndpoint(config: WatchedPath, path: String, blobCreateTime: Instant?) {

    val idToken: IdToken =
      idTokenProvider.idTokenWithAudience(config.httpEndpointSink.endpointUri, emptyList())
    val jwt = idToken.tokenValue

    val httpEndpointConfig = config.httpEndpointSink
    logger.info("${config.identifier}: Sending HTTP request to ${httpEndpointConfig.endpointUri}")
    val requestBuilder =
      HttpRequest.newBuilder()
        .uri(URI.create(httpEndpointConfig.endpointUri))
        .header("Authorization", "Bearer $jwt")
        .header(DATA_WATCHER_PATH_HEADER, path)
        .POST(HttpRequest.BodyPublishers.ofString(httpEndpointConfig.appParams.toJson()))
    if (blobCreateTime != null && path.isDoneMarker()) {
      requestBuilder.header(DONE_BLOB_TIMESTAMP_HEADER, blobCreateTime.toEpochMilli().toString())
    }
    val request = requestBuilder.build()
    val response = httpClient.send(request, BodyHandlers.ofString())
    logger.fine("${config.identifier}: Response status: ${response.statusCode()}")
    logger.fine("${config.identifier}: Response body: ${response.body()}")
    check(response.statusCode() == 200)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val DONE_BLOB_TIMESTAMP_HEADER: String = "X-DataWatcher-Done-Timestamp"
    private const val DONE_SUFFIX = "done"

    private fun String.isDoneMarker(): Boolean {
      return lowercase().endsWith(DONE_SUFFIX)
    }
  }
}
