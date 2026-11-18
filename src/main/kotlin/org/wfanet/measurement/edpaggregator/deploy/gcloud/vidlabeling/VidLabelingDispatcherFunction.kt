/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.protobuf.util.JsonFormat
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatcherParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemCloudFunction
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * Cloud Function that registers VID labeling uploads in the EDP Aggregator metadata store.
 *
 * Invoked when an EDP finishes uploading raw impressions and writes a "done" blob. The function
 * looks up the matching [VidLabelingConfig] for the data provider, resolves active model lines via
 * the VID Repository API, crawls the directory for raw impression files, and creates
 * `RawImpressionUpload`, `RawImpressionUploadFile`, and `RawImpressionUploadModelLine` records. It
 * then calls the shared [VidLabelingDispatchSequencer] to aggressively start pipeline work (the
 * "fast path") rather than waiting for the next `VidLabelingMonitorFunction` tick.
 *
 * ## Environment Variables
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. URI prefix for the config storage bucket.
 * - `CONFIG_BLOB_KEY`: Required. Blob key for the [VidLabelingConfigs] textproto.
 * - `MODEL_LINES_TARGET`: Required. Target endpoint for the VID Repository ModelLines service.
 * - `MODEL_LINES_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_ROLLOUTS_TARGET`: Required. Target endpoint for the VID Repository ModelRollouts
 * - `MODEL_ROLLOUTS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_SHARDS_TARGET`: Required. Target endpoint for the VID Repository ModelShards service.
 * - `MODEL_SHARDS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `RAW_IMPRESSION_UPLOAD_TARGET`: Required. Target endpoint for the `RawImpressionUploadService`,
 *   `RawImpressionUploadModelLineService`, and `PoolAssignmentJobService`.
 * - `RAW_IMPRESSION_UPLOAD_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `CONTROL_PLANE_TARGET`: Required. Target endpoint for the Secure Computation control plane
 *   (`WorkItemsService`).
 * - `CONTROL_PLANE_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `VID_LABELER_QUEUE_NAME`: Required. Resource name of the Phase-2 VidLabeler Secure Computation
 *   queue.
 * - `POOL_ASSIGNER_QUEUE_NAME`: Required. Resource name of the Phase-0 SubpoolAssigner Secure
 *   Computation queue (memoized model lines).
 * - `CHANNEL_SHUTDOWN_DURATION_SECONDS`: Optional. gRPC channel shutdown timeout (default: 3s).
 * - `VID_LABELING_*_RPC_MIN_INTERVAL`: Optional. Minimum intervals for the four outbound RPC
 *   throttlers.
 * - `VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH`: Optional. Enables [FileSystemStorageClient] instead
 *   of GCS. Used only in testing.
 * - `VID_LABELING_DISPATCHER_FOOTER_READ_CONCURRENCY`: Optional. Maximum concurrent Parquet footer
 *   reads (default: 12).
 *
 * ## Request Headers
 * - `X-DataWatcher-Path`: Required. Full storage URI of the "done" blob.
 * - `X-DataWatcher-Generation`: Required. GCS object generation number of the "done" blob.
 * - `X-Override-Model-Lines`: Optional. Comma-separated list of model line resource names to use
 *   instead of querying the VID Repository API. Supports backfilling past data where the model line
 *   may no longer be in the active window.
 * - `X-Recovery-Source-Upload`: Optional. Evicted source upload that authorizes an override
 *   forwarded from done-object recovery metadata.
 * - `X-Eviction-Operation-Id`: Optional. Eviction operation that authorizes the recovery. Required
 *   together with `X-Recovery-Source-Upload`.
 */
class VidLabelingDispatcherFunction : WorkItemCloudFunction() {
  private val runtime = VidLabelingDispatcherRuntime()

  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun isWorkItemRequest(request: HttpRequest): Boolean =
    request.getFirstHeader("x-goog-pubsub-message-id").isPresent ||
      request.contentType.orElse("").substringBefore(';') == "application/octet-stream"

  override suspend fun createWorkItemInvocation(workItem: WorkItem): WorkItemInvocation {
    val provider =
      runCatching {
          val frame = workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
          frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java).dataProvider
        }
        .getOrDefault("")
    val (items, attempts) = runtime.controlPlaneClients(provider)
    return VidLabelingDispatcherWorkItemProcessor(
        items,
        attempts,
        { input, guard ->
          runtime.validateSource(input)
          runtime
            .createDispatcher(
              input.appParams.dataProvider,
              input.source.dataPath,
              input.appParams.recovery.overrideModelLinesList,
              input.appParams.recovery.sourceUpload.takeIf { it.isNotEmpty() },
              UploadHealingOperationKey.fromName(input.appParams.recovery.uploadHealingOperation)
                ?.uploadHealingOperationId,
              guard,
            )
            .upload(input.source.dataPath, input.source.generation)
        },
      )
      .createInvocation(workItem)
  }

  override fun handleFailure(error: Exception, response: HttpResponse) {
    response.setStatusCode(if (error is IllegalArgumentException) 400 else 503)
    response.writer.write("VID dispatch failed: " + error::class.java.simpleName)
  }

  override fun flushTelemetry() {
    EdpaTelemetry.flush()
  }

  override fun serviceLegacyRequest(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting VidLabelingDispatcherFunction")

      val dispatcherParams =
        VidLabelingDispatcherParams.newBuilder()
          .apply { JsonFormat.parser().merge(request.reader, this) }
          .build()

      val doneBlobPath =
        request.getFirstHeader(DATA_WATCHER_PATH_HEADER).orElseThrow {
          IllegalArgumentException("Missing required header: $DATA_WATCHER_PATH_HEADER")
        }

      val doneBlobGeneration: Long =
        request
          .getFirstHeader(DATA_WATCHER_GENERATION_HEADER)
          .map { it.toLong() }
          .orElseThrow {
            IllegalArgumentException("Missing required header: $DATA_WATCHER_GENERATION_HEADER")
          }

      val overrideModelLines: List<String> =
        request
          .getFirstHeader(OVERRIDE_MODEL_LINES_HEADER)
          .map { header -> header.split(",").map { it.trim() }.filter { it.isNotEmpty() } }
          .orElse(emptyList())
      val recoverySourceUpload = request.getFirstHeader(RECOVERY_SOURCE_UPLOAD_HEADER).orElse(null)
      val evictionOperationId = request.getFirstHeader(EVICTION_OPERATION_ID_HEADER).orElse(null)

      val dispatcher =
        runtime.createDispatcher(
          dispatcherParams.dataProvider,
          doneBlobPath,
          overrideModelLines,
          recoverySourceUpload,
          evictionOperationId,
        )
      Tracing.withW3CTraceContext(request) {
        val doneBlobPathHash = VidLabelingTraceLogging.sha256(doneBlobPath)
        Tracing.trace(
          spanName = "edpa.vid_labeling.upload.register",
          attributes =
            Attributes.builder()
              .put(VidLabelingTraceAttributes.DATA_PROVIDER_NAME, dispatcherParams.dataProvider)
              .put(VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH, doneBlobPathHash)
              .put(VidLabelingTraceAttributes.GCS_OBJECT_GENERATION, doneBlobGeneration)
              .put(XmmTraceAttributes.LIFECYCLE_STAGE, "upload_registration")
              .put(XmmTraceAttributes.OUTCOME, "started")
              .build(),
        ) {
          try {
            val outcome =
              runBlocking(Context.current().asContextElement()) {
                dispatcher.upload(doneBlobPath, doneBlobGeneration)
              }
            Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.telemetryValue)
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            Span.current()
              .setAttribute(XmmTraceAttributes.OUTCOME, "failed")
              .setAttribute(XmmTraceAttributes.ERROR_TYPE, XmmTraceAttributes.errorType(e))
            XmmTraceAttributes.errorCode(e)?.let {
              Span.current().setAttribute(XmmTraceAttributes.ERROR_CODE, it)
            }
            throw e
          }
        }
      }
    } catch (e: IllegalArgumentException) {
      // Bad request: missing/invalid headers, malformed params, or an unknown data provider. These
      // are caller errors, so return 4xx rather than letting the framework surface them as 500.
      logger.log(Level.WARNING, "Rejecting request as bad input", e)
      response.setStatusCode(400)
      response.writer.write(e.message ?: "Bad request")
    }
  }

  companion object {
    private val logger = Logger.getLogger(VidLabelingDispatcherFunction::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER = "X-DataWatcher-Path"
    private const val DATA_WATCHER_GENERATION_HEADER = "X-DataWatcher-Generation"
    private const val OVERRIDE_MODEL_LINES_HEADER = "X-Override-Model-Lines"
    private const val RECOVERY_SOURCE_UPLOAD_HEADER = "X-Recovery-Source-Upload"
    private const val EVICTION_OPERATION_ID_HEADER = "X-Eviction-Operation-Id"
  }
}
