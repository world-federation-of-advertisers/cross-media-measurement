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
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Span
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.time.TimeSource
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/*
 * Watcher to observe blob creation events and take the appropriate action for each.
 * @param workItemsStub - the Google Pub Sub Sink to call
 * @param dataWatcherConfigs - a list of [DataWatcherConfig]
 * @param meter - OpenTelemetry meter for instrumentation
 */
class DataWatcher(
  private val workItemsStub: WorkItemsCoroutineStub,
  private val dataWatcherConfigs: List<WatchedPath>,
  private val workItemIdGenerator: () -> String = { "work-item-" + UUID.randomUUID().toString() },
  private val idTokenProvider: IdTokenProvider =
    GoogleCredentials.getApplicationDefault() as? IdTokenProvider
      ?: throw IllegalArgumentException("Application Default Credentials do not provide ID token"),
  private val meter: Meter = Instrumentation.meter,
) {
  private val metrics =
    DataWatcherMetrics(meter = meter, sinkTypeKey = ATTR_SINK_TYPE_KEY, queueKey = ATTR_QUEUE_KEY)

  suspend fun receivePath(path: String) {
    logger.log(Level.INFO, "Received data path for evaluation: path=$path")
    for (config in dataWatcherConfigs) {
      processPathForConfig(config, path)
    }
  }

  private suspend fun processPathForConfig(config: WatchedPath, path: String) {
    val regex = config.sourcePathRegex.toRegex()
    if (!regex.matches(path)) {
      logger.log(
        Level.FINE,
        "Configuration ${config.identifier} did not match path: path=$path, regex=${config.sourcePathRegex}",
      )
      return
    }

    logger.log(Level.INFO, "Configuration matched path: config=${config.identifier}, path=$path")
    val processingStartTime = TimeSource.Monotonic.markNow()

    try {
      when (config.sinkConfigCase) {
        WatchedPath.SinkConfigCase.CONTROL_PLANE_QUEUE_SINK -> sendToControlPlane(config, path)
        WatchedPath.SinkConfigCase.HTTP_ENDPOINT_SINK -> sendToHttpEndpoint(config, path)
        WatchedPath.SinkConfigCase.SINKCONFIG_NOT_SET ->
          error("${config.identifier}: Invalid sink config: ${config.sinkConfigCase}")
      }

      val processingDurationSeconds = processingStartTime.elapsedNow().inWholeMilliseconds / 1000.0
      onProcessingCompleted(config, path, processingDurationSeconds)
    } catch (e: Exception) {
      val elapsedSeconds = processingStartTime.elapsedNow().inWholeMilliseconds / 1000.0
      onProcessingFailed(config, path, elapsedSeconds, e)
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

    onQueueWrite(config, path, queueConfig.queue, workItemId)
  }

  private fun sendToHttpEndpoint(config: WatchedPath, path: String) {

    val idToken: IdToken =
      idTokenProvider.idTokenWithAudience(config.httpEndpointSink.endpointUri, emptyList())
    val jwt = idToken.tokenValue

    val httpEndpointConfig = config.httpEndpointSink
    val client = HttpClient.newHttpClient()
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create(httpEndpointConfig.endpointUri))
        .header("Authorization", "Bearer $jwt")
        .header(DATA_WATCHER_PATH_HEADER, path)
        .POST(HttpRequest.BodyPublishers.ofString(httpEndpointConfig.appParams.toJson()))
        .build()
    val response = client.send(request, BodyHandlers.ofString())
    val statusCode = response.statusCode()
    check(statusCode == 200) {
      "${config.identifier}: HTTP endpoint ${httpEndpointConfig.endpointUri} returned $statusCode"
    }
    onHttpDispatch(config, path, statusCode)
  }

  private fun onProcessingCompleted(config: WatchedPath, path: String, durationSeconds: Double) {
    metrics.recordProcessingDuration(config, durationSeconds)
    Span.current()
      .addEvent(
        EVENT_PROCESSING_COMPLETED,
        Attributes.builder()
          .put(ATTR_SINK_TYPE_KEY, config.sinkConfigCase.name)
          .put(ATTR_DATA_PATH_KEY, path)
          .put(ATTR_STATUS_KEY, STATUS_SUCCESS)
          .put(ATTR_DURATION_SECONDS_KEY, durationSeconds)
          .build(),
      )
    logger.log(
      Level.INFO,
      "Successfully processed configuration: config=${config.identifier}, path=$path, durationSeconds=$durationSeconds",
    )
  }

  private fun onProcessingFailed(
    config: WatchedPath,
    path: String,
    durationSeconds: Double,
    throwable: Throwable,
  ) {
    Span.current()
      .addEvent(
        EVENT_PROCESSING_FAILED,
        Attributes.builder()
          .put(ATTR_SINK_TYPE_KEY, config.sinkConfigCase.name)
          .put(ATTR_DATA_PATH_KEY, path)
          .put(ATTR_STATUS_KEY, STATUS_FAILURE)
          .put(ATTR_DURATION_SECONDS_KEY, durationSeconds)
          .put(ATTR_ERROR_TYPE_KEY, errorTypeOf(throwable))
          .put(ATTR_ERROR_MESSAGE_KEY, throwable.message ?: "")
          .build(),
      )
    logger.log(
      Level.SEVERE,
      "Failed to process configuration: config=${config.identifier}, path=$path, durationSeconds=$durationSeconds, error=${throwable.message}",
      throwable,
    )
  }

  private fun onQueueWrite(
    config: WatchedPath,
    path: String,
    queueName: String,
    workItemId: String,
  ) {
    metrics.recordQueueWrite(config, queueName)
    Span.current()
      .addEvent(
        EVENT_QUEUE_WRITE,
        Attributes.builder()
          .put(ATTR_SINK_TYPE_KEY, config.sinkConfigCase.name)
          .put(ATTR_DATA_PATH_KEY, path)
          .put(ATTR_QUEUE_KEY, queueName)
          .put(ATTR_WORK_ITEM_ID_KEY, workItemId)
          .put(ATTR_STATUS_KEY, STATUS_SUCCESS)
          .build(),
      )
    logger.log(
      Level.INFO,
      "Submitted work item to control plane queue: config=${config.identifier}, path=$path, queue=$queueName, workItemId=$workItemId",
    )
  }

  private fun onHttpDispatch(config: WatchedPath, path: String, statusCode: Int) {
    Span.current()
      .addEvent(
        EVENT_HTTP_ENDPOINT_DISPATCH,
        Attributes.builder()
          .put(ATTR_SINK_TYPE_KEY, config.sinkConfigCase.name)
          .put(ATTR_DATA_PATH_KEY, path)
          .put(ATTR_STATUS_KEY, STATUS_SUCCESS)
          .put(ATTR_HTTP_STATUS_KEY, statusCode.toLong())
          .build(),
      )
    logger.log(
      Level.INFO,
      "Dispatched notification to HTTP endpoint: config=${config.identifier}, path=$path, statusCode=$statusCode",
    )
  }

  private fun errorTypeOf(throwable: Throwable): String {
    return throwable::class.simpleName ?: throwable.javaClass.simpleName ?: throwable.javaClass.name
  }

  companion object {
    private val logger: Logger = Logger.getLogger(DataWatcher::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"

    private val ATTR_SINK_TYPE_KEY = AttributeKey.stringKey("edpa.data_watcher.sink_type")
    private val ATTR_WORK_ITEM_ID_KEY = AttributeKey.stringKey("edpa.data_watcher.work_item_id")
    private val ATTR_QUEUE_KEY = AttributeKey.stringKey("edpa.data_watcher.queue")
    private val ATTR_DATA_PATH_KEY = AttributeKey.stringKey("edpa.data_watcher.data_path")
    private val ATTR_STATUS_KEY = AttributeKey.stringKey("edpa.data_watcher.status")
    private val ATTR_ERROR_TYPE_KEY = AttributeKey.stringKey("edpa.data_watcher.error_type")
    private val ATTR_ERROR_MESSAGE_KEY = AttributeKey.stringKey("edpa.data_watcher.error_message")
    private val ATTR_DURATION_SECONDS_KEY =
      AttributeKey.doubleKey("edpa.data_watcher.processing_duration_seconds")
    private val ATTR_SOURCE_PATH_REGEX_KEY =
      AttributeKey.stringKey("edpa.data_watcher.source_path_regex")
    private val ATTR_HTTP_STATUS_KEY = AttributeKey.longKey("http.status.code")

    private const val STATUS_SUCCESS = "success"
    private const val STATUS_FAILURE = "failure"

    private const val EVENT_PROCESSING_COMPLETED = "edpa.data_watcher.processing_completed"
    private const val EVENT_PROCESSING_FAILED = "edpa.data_watcher.processing_failed"
    private const val EVENT_QUEUE_WRITE = "edpa.data_watcher.queue_write"
    private const val EVENT_HTTP_ENDPOINT_DISPATCH = "edpa.data_watcher.http_dispatch_completed"
  }
}
