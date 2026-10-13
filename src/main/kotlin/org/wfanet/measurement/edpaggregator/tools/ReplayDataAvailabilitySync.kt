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
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
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
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncReplay
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityPublisherMetrics
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/** Replays `DataAvailabilitySync` for completion blobs selected by URI or storage prefix. */
@Command(
  name = "ReplayDataAvailabilitySync",
  description = ["Replays DataAvailabilitySync for existing completion blobs"],
  mixinStandardHelpOptions = true,
)
class ReplayDataAvailabilitySync : Runnable {
  private class ReplaySelection {
    @Option(
      names = ["--blob-prefix"],
      description = ["GCS URI or bucket-relative prefix to search recursively for done blobs"],
      required = true,
    )
    lateinit var blobPrefix: String

    @Option(
      names = ["--done-blob-uri"],
      description = ["Exact completion blob URI to replay; may be repeated"],
      required = true,
    )
    lateinit var doneBlobUris: List<String>

    fun hasBlobPrefix(): Boolean = this::blobPrefix.isInitialized
  }

  @ArgGroup(exclusive = true, multiplicity = "1", heading = "Replay selection:%n")
  private lateinit var replaySelection: ReplaySelection

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
    description = ["Minimum interval between API calls"],
    defaultValue = "1s",
  )
  private lateinit var throttlerMinimumInterval: Duration

  @set:Option(
    names = ["--impression-metadata-batch-size"],
    description = ["Maximum ImpressionMetadata resources per list or write request"],
    defaultValue = "1000",
  )
  private var impressionMetadataBatchSize: Int by Delegates.notNull()

  override fun run() {
    val config = parseTextProto(configFile, DataAvailabilitySyncConfig.getDefaultInstance())
    require(config.dataAvailabilityStorage.storageCase == StorageCase.GCS) {
      "data_availability_storage must use GCS"
    }
    require(config.edpImpressionPath.isNotEmpty()) { "edp_impression_path must be set" }
    require(config.dataProvider.isNotEmpty()) { "data_provider must be set" }

    val storageConfig = config.dataAvailabilityStorage.gcs
    require(storageConfig.bucketName.isNotEmpty()) { "GCS bucket_name must be set" }
    val selection = buildReplaySelection(config.edpImpressionPath, storageConfig.bucketName)
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
    val dataProvidersStub = DataProvidersCoroutineStub(instrumentedKingdomChannel)
    val impressionMetadataStub =
      ImpressionMetadataServiceCoroutineStub(
        ClientInterceptors.intercept(
          impressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )
      )
    val eventGroupActivityPublisher =
      config.eventGroupActivityPublishing
        .takeIf { config.hasEventGroupActivityPublishing() }
        ?.let { publishingConfig ->
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
          CmmsEventGroupActivityPublisher(
            EventGroupActivitiesCoroutineStub(instrumentedKingdomChannel),
            eventGroupMapStorageClient,
            eventGroupMapBlobUri.key,
            config.dataProvider,
            ZoneId.of(marketTimeZoneId),
            EventGroupActivityPublisherMetrics(Instrumentation.meter),
          )
        }
    val dataAvailabilitySync =
      DataAvailabilitySync(
        edpImpressionPath = config.edpImpressionPath,
        storageClient = storageClient,
        dataProvidersStub = dataProvidersStub,
        impressionMetadataServiceStub = impressionMetadataStub,
        dataProviderName = config.dataProvider,
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval),
        impressionMetadataBatchSize = impressionMetadataBatchSize,
        modelLineMap = config.modelLineMapMap.mapValues { it.value.modelLinesList },
        errorIfGapsExist = config.errorIfGapsExist,
        publishEventGroupActivities = eventGroupActivityPublisher?.let { it::publish },
      )
    val replay =
      DataAvailabilitySyncReplay(
        storageClient = storageClient,
        storageRootUri = BlobUri(scheme = "gs", bucket = storageConfig.bucketName, key = ""),
        selection = selection,
        sync = dataAvailabilitySync::sync,
      )

    val result =
      try {
        runBlocking { replay.replay() }
      } finally {
        shutdownChannel(kingdomChannel)
        shutdownChannel(impressionMetadataChannel)
      }

    logger.info(
      "Replay result: " +
        "doneBlobsSelected=${result.doneBlobsSelected}, " +
        "published=${result.published}, " +
        "noWork=${result.noWork}, " +
        "blockedByGaps=${result.blockedByGaps}, " +
        "errors=${result.errors.size}"
    )
    for (error in result.errors) {
      logger.warning("Replay error [${error.doneBlobUri}]: ${error.message}")
    }
    if (result.blockedByGaps > 0 || result.errors.isNotEmpty()) {
      exitProcess(1)
    }
  }

  private fun buildReplaySelection(
    edpImpressionPath: String,
    bucketName: String,
  ): DataAvailabilitySyncReplay.Selection {
    if (replaySelection.hasBlobPrefix()) {
      val suppliedBlobPrefix = replaySelection.blobPrefix.trimEnd('/')
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
      return DataAvailabilitySyncReplay.Selection.BlobPrefix(normalizedBlobPrefix)
    }

    val doneBlobUris = replaySelection.doneBlobUris.toSet()
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
    return DataAvailabilitySyncReplay.Selection.DoneBlobUris(doneBlobUris)
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

fun main(args: Array<String>) = commandLineMain(ReplayDataAvailabilitySync(), args)
