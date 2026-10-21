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

package org.wfanet.measurement.edpaggregator.tools

import com.google.cloud.NoCredentials
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.ZoneId
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams.StorageCase
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.dataavailability.CmmsEventGroupActivityPublisher
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityBackfill
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityPublisherMetrics
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/** Backfills EventGroup activity for completion blobs selected by URI or storage prefix. */
@Command(
  name = "BackfillEventGroupActivities",
  description = ["Backfills EventGroup activity from active finalized impression metadata"],
  mixinStandardHelpOptions = true,
)
class BackfillEventGroupActivities : Runnable {
  private class BackfillSelection {
    @Option(
      names = ["--blob-prefix"],
      description = ["GCS URI or bucket-relative prefix to search recursively for done blobs"],
      required = true,
    )
    lateinit var blobPrefix: String

    @Option(
      names = ["--done-blob-uri"],
      description = ["Exact completion blob URI to backfill; may be repeated"],
      required = true,
    )
    lateinit var doneBlobUris: List<String>

    fun hasBlobPrefix(): Boolean = this::blobPrefix.isInitialized
  }

  @ArgGroup(exclusive = true, multiplicity = "1", heading = "Backfill selection:%n")
  private lateinit var backfillSelection: BackfillSelection

  @Option(
    names = ["--config-file"],
    description = ["Path to a DataAvailabilitySyncConfig textproto"],
    required = true,
  )
  private lateinit var configFile: File

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["Kingdom public API target"],
    required = true,
  )
  private lateinit var kingdomPublicApiTarget: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description = ["Expected hostname in the Kingdom public API TLS certificate"],
  )
  private var kingdomPublicApiCertHost: String? = null

  @Option(
    names = ["--impression-metadata-api-target"],
    description = ["ImpressionMetadata public API target"],
    required = true,
  )
  private lateinit var impressionMetadataApiTarget: String

  @Option(
    names = ["--impression-metadata-api-cert-host"],
    description = ["Expected hostname in the ImpressionMetadata API TLS certificate"],
  )
  private var impressionMetadataApiCertHost: String? = null

  @Option(
    names = ["--storage-api-endpoint"],
    description = ["Google Cloud Storage API endpoint override"],
    hidden = true,
  )
  private var storageApiEndpoint: String? = null

  @Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum interval between ImpressionMetadata API calls"],
    defaultValue = "1s",
  )
  private lateinit var throttlerMinimumInterval: Duration

  @set:Option(
    names = ["--impression-metadata-page-size"],
    description = ["Maximum ImpressionMetadata resources per list request"],
    defaultValue = "1000",
  )
  private var impressionMetadataPageSize: Int by Delegates.notNull()

  override fun run() {
    val config = parseTextProto(configFile, DataAvailabilitySyncConfig.getDefaultInstance())
    require(config.dataAvailabilityStorage.storageCase == StorageCase.GCS) {
      "data_availability_storage must use GCS"
    }
    require(config.edpImpressionPath.isNotEmpty()) { "edp_impression_path must be set" }
    require(config.dataProvider.isNotEmpty()) { "data_provider must be set" }
    require(config.hasEventGroupActivityPublishing()) {
      "event_group_activity_publishing must be set"
    }

    val storageConfig = config.dataAvailabilityStorage.gcs
    require(storageConfig.bucketName.isNotEmpty()) { "GCS bucket_name must be set" }
    val selection = buildBackfillSelection(config.edpImpressionPath, storageConfig.bucketName)
    val storageApiEndpoint = storageApiEndpoint
    val storageClient =
      GcsStorageClient(
        StorageOptions.newBuilder()
          .apply {
            if (storageConfig.projectId.isNotEmpty()) {
              setProjectId(storageConfig.projectId)
            }
            if (storageApiEndpoint != null) {
              setHost(storageApiEndpoint)
              setCredentials(NoCredentials.getInstance())
            }
          }
          .build()
          .service,
        storageConfig.bucketName,
      )
    val kingdomChannel =
      buildChannel(config.cmmsConnection, kingdomPublicApiTarget, kingdomPublicApiCertHost)
    val impressionMetadataChannel =
      buildChannel(
        config.impressionMetadataStorageConnection,
        impressionMetadataApiTarget,
        impressionMetadataApiCertHost,
      )
    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
    val instrumentedKingdomChannel =
      ClientInterceptors.intercept(kingdomChannel, grpcTelemetry.newClientInterceptor())
    val impressionMetadataStub =
      ImpressionMetadataServiceCoroutineStub(
        ClientInterceptors.intercept(
          impressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )
      )

    val publishingConfig = config.eventGroupActivityPublishing
    val marketTimeZoneId = publishingConfig.marketTimeZone.id
    require(marketTimeZoneId.isNotEmpty()) {
      "event_group_activity_publishing.market_time_zone.id must not be empty"
    }
    require(publishingConfig.eventGroupMapBlobUri.isNotEmpty()) {
      "event_group_activity_publishing.event_group_map_blob_uri must not be empty"
    }
    require(publishingConfig.eventGroupMapStorage.storageCase == StorageCase.GCS) {
      "event_group_activity_publishing.event_group_map_storage must use GCS"
    }
    val eventGroupMapBlobUri =
      SelectedStorageClient.parseBlobUri(publishingConfig.eventGroupMapBlobUri)
    val eventGroupMapStorageClient =
      MesosRecordIoStorageClient(
        SelectedStorageClient(
          blobUri = eventGroupMapBlobUri,
          rootDirectory = null,
          projectId = publishingConfig.eventGroupMapStorage.gcs.projectId.ifEmpty { null },
        )
      )
    val eventGroupActivityPublisher =
      CmmsEventGroupActivityPublisher(
        EventGroupActivitiesCoroutineStub(instrumentedKingdomChannel),
        eventGroupMapStorageClient,
        eventGroupMapBlobUri.key,
        config.dataProvider,
        ZoneId.of(marketTimeZoneId),
        EventGroupActivityPublisherMetrics(Instrumentation.meter),
      )
    val backfill =
      EventGroupActivityBackfill(
        storageClient = storageClient,
        storageRootUri = BlobUri(scheme = "gs", bucket = storageConfig.bucketName, key = ""),
        selection = selection,
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = config.dataProvider,
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval),
        impressionMetadataPageSize = impressionMetadataPageSize,
        publishEventGroupActivities = eventGroupActivityPublisher::publish,
      )

    val result =
      try {
        runBlocking { backfill.backfill() }
      } finally {
        shutdownChannel(kingdomChannel)
        shutdownChannel(impressionMetadataChannel)
      }

    logger.info(
      "Backfill result: " +
        "doneBlobsSelected=${result.doneBlobsSelected}, " +
        "activityPairsPublished=${result.activityPairsPublished}, " +
        "noWork=${result.noWork}, " +
        "errors=${result.errors.size}"
    )
    for (error in result.errors) {
      logger.warning("Backfill error [${error.doneBlobUri}]: ${error.message}")
    }
    if (result.errors.isNotEmpty()) {
      exitProcess(1)
    }
  }

  private fun buildBackfillSelection(
    edpImpressionPath: String,
    bucketName: String,
  ): EventGroupActivityBackfill.Selection {
    if (backfillSelection.hasBlobPrefix()) {
      val suppliedBlobPrefix = backfillSelection.blobPrefix.trimEnd('/')
      val normalizedBlobPrefix =
        if (suppliedBlobPrefix.startsWith("gs://")) {
          val parsedUri = SelectedStorageClient.parseBlobUri(suppliedBlobPrefix)
          require(parsedUri.bucket == bucketName) {
            "blob-prefix must use the configured GCS bucket: $suppliedBlobPrefix"
          }
          parsedUri.key.trimEnd('/')
        } else {
          suppliedBlobPrefix
        }
      require(
        normalizedBlobPrefix == edpImpressionPath ||
          normalizedBlobPrefix.startsWith("$edpImpressionPath/")
      ) {
        "blob-prefix must be the configured edp_impression_path or one of its descendants"
      }
      return EventGroupActivityBackfill.Selection.BlobPrefix(normalizedBlobPrefix)
    }

    val doneBlobUris = backfillSelection.doneBlobUris.toSet()
    for (doneBlobUri in doneBlobUris) {
      val parsedUri = SelectedStorageClient.parseBlobUri(doneBlobUri)
      require(parsedUri.scheme == "gs") { "done-blob-uri must use the gs scheme: $doneBlobUri" }
      require(parsedUri.bucket == bucketName) {
        "done-blob-uri must use the configured GCS bucket: $doneBlobUri"
      }
      require(parsedUri.key.substringAfterLast('/') == DONE_BLOB_FILE_NAME) {
        "done-blob-uri must identify a done blob: $doneBlobUri"
      }
      require(parsedUri.key.startsWith("$edpImpressionPath/")) {
        "done-blob-uri must be below the configured edp_impression_path: $doneBlobUri"
      }
    }
    return EventGroupActivityBackfill.Selection.DoneBlobUris(doneBlobUris)
  }

  private fun buildChannel(
    tlsParams: TransportLayerSecurityParams,
    target: String,
    certHost: String?,
  ): ManagedChannel {
    val certs =
      SigningCerts.fromPemFiles(
        certificateFile = File(tlsParams.certFilePath),
        privateKeyFile = File(tlsParams.privateKeyFilePath),
        trustedCertCollectionFile = File(tlsParams.certCollectionFilePath),
      )
    return buildMutualTlsChannel(target, certs, certHost)
  }

  private fun shutdownChannel(channel: ManagedChannel) {
    channel.shutdown()
    channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DONE_BLOB_FILE_NAME = "done"
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

fun main(args: Array<String>) = commandLineMain(BackfillEventGroupActivities(), args)
