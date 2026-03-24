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
import io.grpc.ClientInterceptors
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsRequest

/**
 * Cloud Function that checks whether impression data is lagging for each configured EDP.
 *
 * Invoked by Cloud Scheduler on a periodic schedule. For each EDP listed in
 * [DataAvailabilitySyncConfigs], this function calls `ComputeModelLineBounds` on the
 * ImpressionMetadataService and checks whether the latest data interval end time for every model
 * line falls within the allowed lag window. If any model line's latest data end time is older than
 * [lagThresholdDays] days a `SEVERE` log entry is emitted, enabling Cloud Logging-based alerts to
 * fire.
 *
 * ## Environment Variables
 * - `CONFIG_BLOB_KEY`: Required. GCS key for the [DataAvailabilitySyncConfigs] blob.
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. GCS bucket containing the config blob.
 * - `IMPRESSION_METADATA_TARGET`: Required. Target endpoint for the ImpressionMetadata service.
 * - `IMPRESSION_METADATA_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `LAG_THRESHOLD_DAYS`: Optional. Days before data is considered lagging (default: 3).
 */
class ImpressionDataLagCheckFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting ImpressionDataLagCheckFunction")
      val thresholdInstant = Instant.now().minus(lagThresholdDays, ChronoUnit.DAYS)
      var lagDetected = false

      for (config in runtimeConfigs.configsList) {
        val grpcChannels = DataAvailabilitySyncFunction.getOrCreateSharedChannels(config)
        val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
        val instrumentedChannel =
          ClientInterceptors.intercept(
            grpcChannels.impressionMetadataChannel,
            grpcTelemetry.newClientInterceptor(),
          )
        val impressionMetadataStub = ImpressionMetadataServiceCoroutineStub(instrumentedChannel)

        val boundsResponse = runBlocking {
          impressionMetadataStub.computeModelLineBounds(
            computeModelLineBoundsRequest { parent = config.dataProvider }
          )
        }

        if (boundsResponse.modelLineBoundsList.isEmpty()) {
          logger.warning(
            "No model line bounds found for dataProvider=${config.dataProvider}; " +
              "skipping lag check for this EDP."
          )
          continue
        }

        for (entry in boundsResponse.modelLineBoundsList) {
          val endInstant =
            Instant.ofEpochSecond(entry.value.endTime.seconds, entry.value.endTime.nanos.toLong())
          if (endInstant.isBefore(thresholdInstant)) {
            logger.log(
              Level.SEVERE,
              "DATA LAG DETECTED: dataProvider=${config.dataProvider}, " +
                "modelLine=${entry.key}, latestDataEnd=$endInstant, " +
                "lagThresholdDays=$lagThresholdDays",
            )
            lagDetected = true
          }
        }
      }

      response.setStatusCode(200)
      response.writer.write(
        if (lagDetected) "Data lag detected for one or more EDPs. Check logs for details."
        else "All configured EDPs have data within the lag threshold."
      )
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in ImpressionDataLagCheckFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger =
      Logger.getLogger(ImpressionDataLagCheckFunction::class.java.name)

    private const val DEFAULT_LAG_THRESHOLD_DAYS = 3L

    private val lagThresholdDays: Long =
      System.getenv("LAG_THRESHOLD_DAYS")?.toLongOrNull()?.takeIf { it > 0 }
        ?: DEFAULT_LAG_THRESHOLD_DAYS

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
  }
}
