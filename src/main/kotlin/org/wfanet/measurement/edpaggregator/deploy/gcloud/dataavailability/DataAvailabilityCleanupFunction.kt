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
import com.google.protobuf.util.JsonFormat
import io.grpc.ClientInterceptors
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.BufferedReader
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest

/**
 * Cloud Function that handles cleanup of ImpressionMetadata records when GCS objects are deleted.
 *
 * This function is invoked by DataWatcher when an OBJECT_DELETE event is detected in GCS. It reads
 * the ImpressionMetadata resource name from the request header and calls DeleteImpressionMetadata
 * to soft-delete the corresponding Spanner record.
 *
 * ## Headers
 * - `X-DataWatcher-Path`: Required. The GCS object path that was deleted (used as blob URI).
 * - `X-Impression-Metadata-Resource-Id`: Optional. The ImpressionMetadata resource name to delete.
 *   If not provided, the function will look up the record by blob URI.
 *
 * ## Environment Variables
 * - `IMPRESSION_METADATA_TARGET`: Required. Target endpoint for the Impression Metadata service.
 * - `IMPRESSION_METADATA_CERT_HOST`: Optional. Overrides TLS authority for testing.
 *
 * ## Configuration
 * - A [DataAvailabilitySyncConfig] is provided in the request body by the DataWatcher Cloud
 *   Function. This is used to configure the gRPC channel.
 */
class DataAvailabilityCleanupFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting DataAvailabilityCleanupFunction")

      val requestBody: BufferedReader = request.reader
      val dataAvailabilitySyncConfig =
        DataAvailabilitySyncConfig.newBuilder()
          .apply { JsonFormat.parser().merge(requestBody, this) }
          .build()

      // Read the path as request header
      val deletedBlobPath =
        request.getFirstHeader(DATA_WATCHER_PATH_HEADER).orElseThrow {
          IllegalArgumentException("Missing required header: $DATA_WATCHER_PATH_HEADER")
        }

      // Read the ImpressionMetadata resource ID from header (optional)
      val impressionMetadataResourceId =
        request.getFirstHeader(IMPRESSION_METADATA_RESOURCE_ID_HEADER).orElse(null)

      val grpcChannels =
        DataAvailabilitySyncFunction.getOrCreateSharedChannels(dataAvailabilitySyncConfig)
      val impressionMetadataChannel = grpcChannels.impressionMetadataChannel

      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val instrumentedChannel =
        ClientInterceptors.intercept(impressionMetadataChannel, grpcTelemetry.newClientInterceptor())

      val impressionMetadataServiceStub = ImpressionMetadataServiceCoroutineStub(instrumentedChannel)

      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) {
          val resourceIdToDelete: String? =
            if (!impressionMetadataResourceId.isNullOrEmpty()) {
              logger.info(
                "Using resource ID from header for deleted blob: $deletedBlobPath, " +
                  "resourceId: $impressionMetadataResourceId"
              )
              impressionMetadataResourceId
            } else {
              // Look up the ImpressionMetadata by blob URI
              logger.info(
                "No resource ID header found. Looking up ImpressionMetadata by blob URI: $deletedBlobPath"
              )
              val listResponse =
                impressionMetadataServiceStub.listImpressionMetadata(
                  listImpressionMetadataRequest {
                    parent = dataAvailabilitySyncConfig.dataProvider
                    filter = ListImpressionMetadataRequestKt.filter { blobUri = deletedBlobPath }
                  }
                )
              if (listResponse.impressionMetadataList.isEmpty()) {
                logger.warning(
                  "No ImpressionMetadata found for blob URI: $deletedBlobPath. Skipping cleanup."
                )
                null
              } else {
                listResponse.impressionMetadataList.first().name
              }
            }

          if (resourceIdToDelete != null) {
            try {
              impressionMetadataServiceStub.deleteImpressionMetadata(
                deleteImpressionMetadataRequest { name = resourceIdToDelete }
              )
              logger.info("Successfully soft-deleted ImpressionMetadata: $resourceIdToDelete")
            } catch (e: StatusException) {
              if (e.status.code == Status.Code.NOT_FOUND) {
                // Idempotent - the record may have already been deleted
                logger.info(
                  "ImpressionMetadata not found (already deleted): $resourceIdToDelete"
                )
              } else {
                throw e
              }
            }
          }
        }
      }

      response.setStatusCode(200)
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in DataAvailabilityCleanupFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      // Critical for Cloud Functions: flush metrics before function freezes
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val IMPRESSION_METADATA_RESOURCE_ID_HEADER: String =
      "X-Impression-Metadata-Resource-Id"
  }
}

