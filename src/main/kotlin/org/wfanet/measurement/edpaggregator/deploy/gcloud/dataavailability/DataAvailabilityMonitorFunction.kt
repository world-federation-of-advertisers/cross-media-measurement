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
import io.grpc.ManagedChannel
import java.io.File
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfigs
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityMonitor
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * Cloud Function that monitors data availability for staleness, gaps, missing days, and
 * late-arriving files.
 *
 * Designed to be invoked on a schedule (e.g., daily via Cloud Scheduler). Checks each configured
 * EDP impression path and its active model lines for:
 * - **Staleness**: No upload for more than `max_stale_days` days.
 * - **Gaps**: Missing dates between the earliest and latest uploaded dates.
 * - **Missing days**: Date folders with a "done" blob but no data files, or date folders without a
 *   "done" blob.
 * - **Late-arriving files**: Data files updated after the "done" blob was written.
 *
 * Issues are logged at SEVERE level for integration with Cloud Monitoring alerting policies.
 *
 * ## Environment Variables
 * - `CONFIG_BLOB_KEY`: Required. GCS path to the [DataAvailabilityMonitorConfigs] proto.
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. GCS bucket containing the config blob.
 * - `DATA_AVAILABILITY_FILE_SYSTEM_PATH`: Optional. If set, uses local filesystem instead of GCS.
 * - OpenTelemetry variables (e.g. `OTEL_EXPORTER_OTLP_ENDPOINT`): Optional. Configures telemetry
 *   export for metrics recorded by [DataAvailabilityMonitor].
 *
 * Configuration is loaded eagerly at class initialization via [runBlocking]. If the config blob is
 * unavailable at startup, the Cloud Function class will fail to load (fail-fast).
 */
class DataAvailabilityMonitorFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.info("Starting DataAvailabilityMonitorFunction")

      // HttpFunction.service is synchronous; runBlocking bridges to suspend functions.
      val hasAnyIssues = runBlocking {
        monitorConfigs.configsList.map { config -> checkAndLogConfigIssues(config) }.any { it }
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

  /**
   * Checks a single config's model lines for data availability issues and logs any findings.
   *
   * @return `true` if any issues were detected for this config.
   */
  private suspend fun checkAndLogConfigIssues(config: DataAvailabilityMonitorConfig): Boolean {
    val storageClient = createStorageClient(config)
    val activeModelLines =
      config.modelLineConfigsList
        .map { modelLineConfig ->
          requireNotNull(ModelLineKey.fromName(modelLineConfig.modelLine)) {
            "Invalid Model Line resource name: ${modelLineConfig.modelLine}"
          }
        }
        .toSet()

    require(activeModelLines.isNotEmpty()) {
      "No active model lines configured for path: ${config.edpImpressionPath}"
    }

    require(config.timeZone.isNotEmpty()) {
      "time_zone must be set in DataAvailabilityMonitorConfig for path: ${config.edpImpressionPath}"
    }
    val timeZone = ZoneId.of(config.timeZone)

    val maxStaleDays =
      if (config.maxStaleDays > 0) config.maxStaleDays
      else DataAvailabilityMonitor.DEFAULT_MAX_STALE_DAYS

    val spuriousDeletionConfig = config.spuriousDeletionCheck
    val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub? =
      if (spuriousDeletionConfig.enabled) {
        require(config.hasImpressionMetadataConnection()) {
          "impression_metadata_connection must be set when spurious_deletion_check is enabled"
        }
        require(config.dataProviderName.isNotEmpty()) {
          "data_provider_name must be set when spurious_deletion_check is enabled"
        }
        val channel = getOrCreateImpressionMetadataChannel(config.impressionMetadataConnection)
        ImpressionMetadataServiceCoroutineStub(channel)
      } else {
        null
      }

    val monitor =
      DataAvailabilityMonitor(
        storageClient = storageClient,
        edpImpressionPath = config.edpImpressionPath,
        activeModelLines = activeModelLines,
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = config.dataProviderName.ifEmpty { null },
      )

    val spuriousLookbackDays =
      if (spuriousDeletionConfig.enabled) spuriousDeletionConfig.lookbackDays else null

    val result =
      monitor.checkFullStatus(
        maxStaleDays = maxStaleDays,
        clock = { LocalDate.now(timeZone) },
        spuriousDeletionLookbackDays = spuriousLookbackDays,
      )

    if (result.statuses.none { hasIssues(it) }) {
      logger.info("All model lines healthy for path: ${config.edpImpressionPath}")
      return false
    }

    for (status in result.statuses) {
      logStatusIssues(status, config.edpImpressionPath, maxStaleDays)
    }
    return true
  }

  private fun logStatusIssues(
    status: DataAvailabilityMonitor.ModelLineStatus,
    edpImpressionPath: String,
    maxStaleDays: Int,
  ) {
    val modelLineName = status.modelLineKey.toName()
    if (status.isStale == true) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "is stale. Latest upload: ${status.latestDate} " +
          "(${status.staleDays} days ago, threshold: $maxStaleDays)",
      )
    }
    if (!status.gapDates.isNullOrEmpty()) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "has gap dates: ${status.gapDates}",
      )
    }
    if (!status.zeroImpressionDates.isNullOrEmpty()) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "has zero impression dates (done blob but no data): ${status.zeroImpressionDates}",
      )
    }
    if (!status.datesWithoutDoneBlob.isNullOrEmpty()) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "has dates without done blob: ${status.datesWithoutDoneBlob}",
      )
    }
    if (!status.lateArrivingDates.isNullOrEmpty()) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "has late-arriving data after done blob: ${status.lateArrivingDates}",
      )
    }
    if ((status.spuriousDeletionCount ?: 0) > 0) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName in $edpImpressionPath " +
          "has ${status.spuriousDeletionCount} spuriously deleted entries " +
          "(deleted in metadata store but blob still exists on bucket)",
      )
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

    private val impressionMetadataTarget: String by lazy {
      EnvVars.checkNotNullOrEmpty("IMPRESSION_METADATA_TARGET")
    }

    private val impressionMetadataCertHost: String? = System.getenv("IMPRESSION_METADATA_CERT_HOST")

    private val channelShutdownDurationSeconds: Long =
      System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLongOrNull() ?: 3L

    private data class ChannelKey(
      val tls: TransportLayerSecurityParams,
      val target: String,
      val hostName: String?,
    )

    private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

    private fun getOrCreateImpressionMetadataChannel(
      tlsParams: TransportLayerSecurityParams
    ): ManagedChannel {
      val key = ChannelKey(tlsParams, impressionMetadataTarget, impressionMetadataCertHost)
      return channelCache.computeIfAbsent(key) {
        logger.info("Creating new ImpressionMetadata channel for TLS params: $key")
        val signingCerts =
          SigningCerts.fromPemFiles(
            certificateFile = File(tlsParams.certFilePath),
            privateKeyFile = File(tlsParams.privateKeyFilePath),
            trustedCertCollectionFile = File(tlsParams.certCollectionFilePath),
          )
        buildMutualTlsChannel(impressionMetadataTarget, signingCerts, impressionMetadataCertHost)
          .withShutdownTimeout(Duration.ofSeconds(channelShutdownDurationSeconds))
      }
    }

    private val configBlobKey: String =
      requireNotNull(System.getenv("CONFIG_BLOB_KEY")) {
        "CONFIG_BLOB_KEY environment variable must be set"
      }

    /**
     * Eagerly loaded at class init via [runBlocking]. Cloud Functions initialize the class on the
     * framework's classloading thread before serving the first request, making this effectively the
     * application startup phase. If the config blob is unavailable, the class will fail to load
     * (fail-fast), causing the Cloud Function to reject all requests with a load error.
     */
    private val monitorConfigs: DataAvailabilityMonitorConfigs = runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        DataAvailabilityMonitorConfigs.getDefaultInstance(),
      )
    }

    private fun hasIssues(status: DataAvailabilityMonitor.ModelLineStatus): Boolean =
      status.isStale == true ||
        !status.gapDates.isNullOrEmpty() ||
        !status.zeroImpressionDates.isNullOrEmpty() ||
        !status.datesWithoutDoneBlob.isNullOrEmpty() ||
        !status.lateArrivingDates.isNullOrEmpty() ||
        (status.spuriousDeletionCount ?: 0) > 0
  }
}
