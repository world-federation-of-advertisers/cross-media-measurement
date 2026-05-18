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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.edpaggregator.ConfigLoader
import org.wfanet.measurement.edpaggregator.dataavailability.BufferedDataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.dataavailability.DeleteEvent
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

/**
 * Cloud Function that handles cleanup of ImpressionMetadata records when GCS objects are deleted.
 *
 * This function is invoked by DataWatcher when an OBJECT_DELETE event is detected in GCS. It reads
 * the ImpressionMetadata resource name from the request header and calls DeleteImpressionMetadata
 * to soft-delete the corresponding Spanner record.
 *
 * ## Buffered Mode
 *
 * When `CLEANUP_BUFFER_ENABLED=true`, delete events are buffered in memory and flushed via a
 * single `BatchDeleteImpressionMetadata` RPC. This reduces N individual Spanner transactions to
 * one when GCS lifecycle rules delete many files in a short window.
 *
 * ## Headers
 * - `X-DataWatcher-Path`: Required. The GCS object path that was deleted (used as blob URI).
 * - `X-Impression-Metadata-Resource-Id`: Optional. The ImpressionMetadata resource name to delete.
 *   If not provided, the function will look up the record by blob URI.
 *
 * ## Environment Variables
 * - `IMPRESSION_METADATA_TARGET`: Required. Target endpoint for the Impression Metadata service.
 * - `IMPRESSION_METADATA_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `CLEANUP_BUFFER_ENABLED`: Optional. Set to "true" to enable buffered batch processing.
 * - `CLEANUP_BUFFER_BATCH_SIZE`: Optional. Flush threshold (default: 100).
 * - `CLEANUP_BUFFER_FLUSH_INTERVAL_SECONDS`: Optional. Periodic flush interval (default: 30).
 *
 * ## Configuration
 * - The request body contains versioned params (as a `google.protobuf.Any`) or a legacy
 *   `DataAvailabilitySyncConfig` JSON. The config is resolved via [ConfigLoader].
 */
class DataAvailabilityCleanupFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting DataAvailabilityCleanupFunction")

      val requestBody = request.reader.readText()
      val dataAvailabilitySyncConfig =
        ConfigLoader.buildDataAvailabilitySyncConfig(requestBody, runtimeConfigs.configsList)

      val deletedBlobPath =
        request.getFirstHeader(DATA_WATCHER_PATH_HEADER).orElseThrow {
          IllegalArgumentException("Missing required header: $DATA_WATCHER_PATH_HEADER")
        }

      val impressionMetadataResourceId =
        request.getFirstHeader(IMPRESSION_METADATA_RESOURCE_ID_HEADER).orElse(null)

      if (bufferEnabled) {
        val buffer = getOrCreateBuffer(dataAvailabilitySyncConfig)
        buffer.enqueue(DeleteEvent(deletedBlobPath, impressionMetadataResourceId))
        logger.info(
          "Buffered delete event for $deletedBlobPath " +
            "(pending: ${buffer.pendingCount()})"
        )
        response.setStatusCode(200)
        response.writer.write("Buffered (pending: ${buffer.pendingCount()})")
        return
      }

      val dataAvailabilityCleanup = createCleanup(dataAvailabilitySyncConfig)

      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) {
          dataAvailabilityCleanup.cleanup(deletedBlobPath, impressionMetadataResourceId)
        }
      }

      response.setStatusCode(200)
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in DataAvailabilityCleanupFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val IMPRESSION_METADATA_RESOURCE_ID_HEADER: String =
      "X-Impression-Metadata-Resource-Id"

    private val configBlobKey: String =
      requireNotNull(System.getenv("CONFIG_BLOB_KEY")) {
        "CONFIG_BLOB_KEY environment variable must be set"
      }
    private val runtimeConfigs: DataAvailabilitySyncConfigs = runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        DataAvailabilitySyncConfigs.getDefaultInstance(),
      )
    }

    val bufferEnabled: Boolean =
      System.getenv("CLEANUP_BUFFER_ENABLED")?.equals("true", ignoreCase = true) ?: false

    private val bufferBatchSize: Int =
      System.getenv("CLEANUP_BUFFER_BATCH_SIZE")?.toIntOrNull()
        ?: BufferedDataAvailabilityCleanup.DEFAULT_BATCH_SIZE

    private val bufferFlushIntervalSeconds: Long =
      System.getenv("CLEANUP_BUFFER_FLUSH_INTERVAL_SECONDS")?.toLongOrNull()
        ?: BufferedDataAvailabilityCleanup.DEFAULT_FLUSH_INTERVAL_SECONDS

    @Volatile private var sharedBuffer: BufferedDataAvailabilityCleanup? = null

    init {
      if (bufferEnabled) {
        Runtime.getRuntime().addShutdownHook(
          Thread {
            logger.info("Shutdown hook: flushing buffered delete events")
            sharedBuffer?.shutdown()
          }
        )
      }
    }

    private fun createInstrumentedStub(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): ImpressionMetadataServiceCoroutineStub {
      val grpcChannels =
        DataAvailabilitySyncFunction.getOrCreateSharedChannels(dataAvailabilitySyncConfig)

      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val instrumentedChannel =
        ClientInterceptors.intercept(
          grpcChannels.impressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )

      return ImpressionMetadataServiceCoroutineStub(instrumentedChannel)
    }

    private fun createCleanup(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): DataAvailabilityCleanup {
      return DataAvailabilityCleanup(
        createInstrumentedStub(dataAvailabilitySyncConfig),
        dataAvailabilitySyncConfig.dataProvider,
        createStorageClient(dataAvailabilitySyncConfig),
      )
    }

    private fun getOrCreateBuffer(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): BufferedDataAvailabilityCleanup {
      return sharedBuffer ?: synchronized(this) {
        sharedBuffer ?: run {
          BufferedDataAvailabilityCleanup(
            impressionMetadataServiceStub =
              createInstrumentedStub(dataAvailabilitySyncConfig),
            dataProviderName = dataAvailabilitySyncConfig.dataProvider,
            storageClient = createStorageClient(dataAvailabilitySyncConfig),
            batchSize = bufferBatchSize,
            flushIntervalSeconds = bufferFlushIntervalSeconds,
          ).also {
            sharedBuffer = it
            logger.info(
              "Created shared buffer (batchSize=$bufferBatchSize, " +
                "flushInterval=${bufferFlushIntervalSeconds}s)"
            )
          }
        }
      }
    }

    private fun createStorageClient(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): GcsStorageClient {
      val gcsConfig = dataAvailabilitySyncConfig.dataAvailabilityStorage.gcs
      return GcsStorageClient(
        StorageOptions.newBuilder()
          .also {
            if (gcsConfig.projectId.isNotEmpty()) {
              it.setProjectId(gcsConfig.projectId)
            }
          }
          .build()
          .service,
        gcsConfig.bucketName,
      )
    }
  }
}
