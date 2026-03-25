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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import java.io.File
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfigs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * Cloud Function that monitors data availability for staleness.
 *
 * Designed to be invoked on a schedule (e.g., daily via Cloud Scheduler). Checks each configured
 * EDP impression path and its active model lines for:
 * - **Staleness**: No upload for more than `max_stale_days` days.
 *
 * Issues are logged at SEVERE level for integration with Cloud Monitoring alerting policies.
 *
 * ## Environment Variables
 * - `CONFIG_BLOB_KEY`: Required. GCS path to the [DataAvailabilityMonitorConfigs] proto.
 * - `DATA_AVAILABILITY_FILE_SYSTEM_PATH`: Optional. If set, uses local filesystem instead of GCS.
 */
class DataAvailabilityMonitorFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.info("Starting DataAvailabilityMonitorFunction")

      var hasAnyIssues = false

      for (config in monitorConfigs.configsList) {
        val storageClient = createStorageClient(config)
        val activeModelLines = config.activeModelLinesList.map {
          requireNotNull(ModelLineKey.fromName(it)) {
            "Invalid Model Line resource name: $it"
          }
        }.toSet()

        if (activeModelLines.isEmpty()) {
          logger.warning(
            "No active model lines configured for path: ${config.edpImpressionPath}. Skipping."
          )
          continue
        }

        val maxStaleDays =
          if (config.maxStaleDays > 0) config.maxStaleDays
          else DataAvailabilityMonitor.DEFAULT_MAX_STALE_DAYS

        val monitor =
          DataAvailabilityMonitor(
            storageClient = storageClient,
            edpImpressionPath = config.edpImpressionPath,
            activeModelLines = activeModelLines,
          )

        val result = runBlocking {
          monitor.checkFullStatus(
            maxStaleDays = maxStaleDays,
            clock = { LocalDate.now(ZoneOffset.UTC) },
          )
        }

        if (result.statuses.any { it.isStale == true || !it.gapDates.isNullOrEmpty() || !it.zeroImpressionDates.isNullOrEmpty() || !it.datesWithoutDoneBlob.isNullOrEmpty() }) {
          hasAnyIssues = true
          for (status in result.statuses) {
            if (status.isStale == true) {
              logger.log(
                Level.SEVERE,
                "ALERT: Model line ${status.modelLineKey.toName()} in ${config.edpImpressionPath} " +
                  "is stale. Latest upload: ${status.latestDate} " +
                  "(${status.staleDays} days ago, threshold: $maxStaleDays)",
              )
            }
            if (!status.gapDates.isNullOrEmpty()) {
              logger.log(
                Level.SEVERE,
                "ALERT: Model line ${status.modelLineKey.toName()} in ${config.edpImpressionPath} " +
                  "has gap dates: ${status.gapDates}",
              )
            }
            if (!status.zeroImpressionDates.isNullOrEmpty()) {
              logger.log(
                Level.SEVERE,
                "ALERT: Model line ${status.modelLineKey.toName()} in ${config.edpImpressionPath} " +
                  "has zero impression dates (done blob but no data): ${status.zeroImpressionDates}",
              )
            }
            if (!status.datesWithoutDoneBlob.isNullOrEmpty()) {
              logger.log(
                Level.SEVERE,
                "ALERT: Model line ${status.modelLineKey.toName()} in ${config.edpImpressionPath} " +
                  "has dates without done blob: ${status.datesWithoutDoneBlob}",
              )
            }
          }
        } else {
          logger.info("All model lines healthy for path: ${config.edpImpressionPath}")
        }
      }

      if (hasAnyIssues) {
        response.setStatusCode(500)
        response.writer.write("Data availability issues detected. See logs for details.")
      } else {
        response.setStatusCode(200)
        response.writer.write("All model lines healthy.")
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in DataAvailabilityMonitorFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  private fun createStorageClient(config: DataAvailabilityMonitorConfig): StorageClient {
    return if (!fileSystemPath.isNullOrEmpty()) {
      FileSystemStorageClient(File(fileSystemPath))
    } else {
      val gcsConfig = config.storage.gcs
      GcsStorageClient(
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val fileSystemPath: String? = System.getenv("DATA_AVAILABILITY_FILE_SYSTEM_PATH")

    private val configBlobKey: String =
      requireNotNull(System.getenv("CONFIG_BLOB_KEY")) {
        "CONFIG_BLOB_KEY environment variable must be set"
      }

    private val monitorConfigs: DataAvailabilityMonitorConfigs = runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        DataAvailabilityMonitorConfigs.getDefaultInstance(),
      )
    }
  }
}
