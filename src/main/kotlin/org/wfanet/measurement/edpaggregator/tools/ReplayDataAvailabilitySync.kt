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
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.ZoneId
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams.StorageCase
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.ModelLineCutoverValidator
import org.wfanet.measurement.edpaggregator.dataavailability.CmmsEventGroupActivityPublisher
import org.wfanet.measurement.edpaggregator.dataavailability.CompletionBlobNames
import org.wfanet.measurement.edpaggregator.dataavailability.CompletionBlobType
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncReplay
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityBackfill
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityPublisherMetrics
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.GcsGenerationMatchedBlobMetadataUpdater
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private data class ReplayStorageSource(
  val storage: Storage,
  val storageClient: GcsStorageClient,
  val bucketName: String,
  val edpImpressionPath: String,
  val replayPrefix: String,
  val completionBlobType: CompletionBlobType,
)

private data class ReplaySourceSelection(
  val source: ReplayStorageSource,
  val selection: DataAvailabilitySyncReplay.Selection,
)

/** Replays data availability or only EventGroup activity for selected completion blobs. */
@Command(
  name = "ReplayDataAvailabilitySync",
  description = ["Replays finalized impression metadata in the selected mode"],
  mixinStandardHelpOptions = true,
)
class ReplayDataAvailabilitySync : Runnable {
  enum class Mode {
    DATA_AVAILABILITY_ONLY,
    EVENT_GROUP_ACTIVITY_ONLY,
    FULL_SYNC,
  }

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
    names = ["--mode"],
    description = ["Replay mode: DATA_AVAILABILITY_ONLY, EVENT_GROUP_ACTIVITY_ONLY, or FULL_SYNC"],
    required = true,
  )
  private lateinit var mode: Mode

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
    defaultValue = "100",
  )
  private var impressionMetadataBatchSize: Int by Delegates.notNull()

  override fun run() {
    val config = parseTextProto(configFile, DataAvailabilitySyncConfig.getDefaultInstance())
    require(config.dataAvailabilityStorage.storageCase == StorageCase.GCS) {
      "data_availability_storage must use GCS"
    }
    require(config.edpImpressionPath.isNotEmpty()) { "edp_impression_path must be set" }
    require(config.dataProvider.isNotEmpty()) { "data_provider must be set" }
    val externalPath = config.externalImpressionPath.ifEmpty { config.edpImpressionPath }
    val internalPath = config.vidLabeledImpressionPath
    require(
      internalPath.isEmpty() ||
        (externalPath != internalPath &&
          !externalPath.startsWith("$internalPath/") &&
          !internalPath.startsWith("$externalPath/"))
    ) {
      "Configured impression paths must not overlap"
    }
    val configuredPaths = listOf(externalPath, internalPath).filter(String::isNotEmpty)
    if (mode == Mode.EVENT_GROUP_ACTIVITY_ONLY) {
      require(config.hasEventGroupActivityPublishing()) {
        "event_group_activity_publishing must be set in EVENT_GROUP_ACTIVITY_ONLY mode"
      }
    }

    val storageConfig = config.dataAvailabilityStorage.gcs
    require(storageConfig.bucketName.isNotEmpty()) { "GCS bucket_name must be set" }
    val storageApiEndpoint = storageApiEndpoint
    val storage =
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
        .service
    val storageClient = GcsStorageClient(storage, storageConfig.bucketName)
    val modelLineCutovers = config.modelLineCutoversList.map(ModelLineCutover::from)
    ModelLineCutoverValidator.validate(
      modelLineCutovers,
      config.modelLineMapMap.values.flatMap { it.modelLinesList }.toSet(),
    )
    val contributors =
      modelLineCutovers.flatMap { listOf(it.historicalModelLine, it.replacementModelLine) }
    require(config.modelLineDataSourcesMap.keys.containsAll(contributors)) {
      "model_line_data_sources must contain every cutover contributor"
    }
    val replacementModelLines = modelLineCutovers.mapTo(mutableSetOf()) { it.replacementModelLine }
    val modelLineStorageSources =
      config.modelLineDataSourcesMap.mapValues { (modelLine, source) ->
        val modelLineKey =
          requireNotNull(ModelLineKey.fromName(modelLine)) {
            "model_line_data_sources key must be a ModelLine resource name: $modelLine"
          }
        require(
          !source.edpImpressionPath.startsWith("/") && !source.edpImpressionPath.endsWith("/")
        ) {
          "model_line_data_sources[$modelLine].edp_impression_path must not start or end with a slash"
        }
        require(source.storage.storageCase == StorageCase.GCS) {
          "model_line_data_sources[$modelLine].storage must use GCS"
        }
        val sourceStorageConfig = source.storage.gcs
        require(sourceStorageConfig.bucketName.isNotEmpty()) {
          "model_line_data_sources[$modelLine].storage.gcs.bucket_name must be set"
        }
        val sourceStorage =
          StorageOptions.newBuilder()
            .also {
              if (sourceStorageConfig.projectId.isNotEmpty()) {
                it.setProjectId(sourceStorageConfig.projectId)
              }
              if (storageApiEndpoint != null) {
                it.setHost(storageApiEndpoint)
                it.setCredentials(NoCredentials.getInstance())
              }
            }
            .build()
            .service
        ReplayStorageSource(
          storage = sourceStorage,
          storageClient = GcsStorageClient(sourceStorage, sourceStorageConfig.bucketName),
          bucketName = sourceStorageConfig.bucketName,
          edpImpressionPath = source.edpImpressionPath,
          replayPrefix =
            if (source.edpImpressionPath.isEmpty()) {
              "model-line/${modelLineKey.modelLineId}"
            } else {
              "${source.edpImpressionPath}/model-line/${modelLineKey.modelLineId}"
            },
          // Replacement contributors are produced by the internal VID Labeler.
          completionBlobType =
            if (modelLine in replacementModelLines) {
              CompletionBlobType.VID_LABELER_OR_LEGACY
            } else {
              completionType(config, sourceStorageConfig.bucketName, source.edpImpressionPath)
            },
        )
      }
    require(
      modelLineStorageSources.values.map { it.bucketName to it.replayPrefix }.distinct().size ==
        modelLineStorageSources.size
    ) {
      "model_line_data_sources must use distinct model-line replay paths"
    }
    val modelLineDataSources =
      modelLineStorageSources.mapValues { (_, source) ->
        DataAvailabilitySync.ModelLineDataSource(
          storageClient = source.storageClient,
          edpImpressionPath = source.edpImpressionPath,
        )
      }
    val storageSources =
      (configuredPaths.map { impressionPath ->
          ReplayStorageSource(
            storage = storage,
            storageClient = storageClient,
            bucketName = storageConfig.bucketName,
            edpImpressionPath = impressionPath,
            replayPrefix = impressionPath,
            completionBlobType = completionType(config, storageConfig.bucketName, impressionPath),
          )
        } + modelLineStorageSources.values)
        .groupBy { it.bucketName to it.replayPrefix }
        .map { (_, sources) ->
          require(
            sources.map { it.edpImpressionPath to it.completionBlobType }.distinct().size == 1
          ) {
            "Configured replay sources have conflicting semantics at the same path"
          }
          sources.first()
        }
    val sourceSelections = runBlocking {
      buildReplaySelections(storageSources, storageConfig.bucketName)
    }
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
    val instrumentedMetadataChannel =
      ClientInterceptors.intercept(impressionMetadataChannel, grpcTelemetry.newClientInterceptor())
    val impressionMetadataStub = ImpressionMetadataServiceCoroutineStub(instrumentedMetadataChannel)
    val leaseRunner =
      DataAvailabilitySyncLeaseRunner(
        GrpcDataAvailabilitySyncLeaseClient(
          DataAvailabilitySyncLeaseServiceCoroutineStub(instrumentedMetadataChannel)
        ),
        renewalDelay = { delay(it) },
      )
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)
    val eventGroupActivityPublisher =
      when (mode) {
        Mode.DATA_AVAILABILITY_ONLY -> null
        Mode.EVENT_GROUP_ACTIVITY_ONLY,
        Mode.FULL_SYNC ->
          buildEventGroupActivityPublisher(config, instrumentedKingdomChannel, throttler)
      }
    val succeeded =
      try {
        when (mode) {
          Mode.EVENT_GROUP_ACTIVITY_ONLY -> {
            val publisher = checkNotNull(eventGroupActivityPublisher)
            val results = runBlocking {
              sourceSelections.map { sourceSelection ->
                EventGroupActivityBackfill(
                    storageClient = sourceSelection.source.storageClient,
                    storageRootUri =
                      BlobUri(scheme = "gs", bucket = sourceSelection.source.bucketName, key = ""),
                    selection = sourceSelection.selection.toEventGroupActivitySelection(),
                    impressionMetadataStub = impressionMetadataStub,
                    dataProviderName = config.dataProvider,
                    throttler = throttler,
                    impressionMetadataPageSize = impressionMetadataBatchSize,
                    publishEventGroupActivities = publisher::publish,
                  )
                  .backfill()
              }
            }
            results.forEach(::logActivityOnlyResult)
            results.all { it.errors.isEmpty() }
          }
          Mode.DATA_AVAILABILITY_ONLY,
          Mode.FULL_SYNC -> {
            val results = runBlocking {
              sourceSelections.map { sourceSelection ->
                DataAvailabilitySyncReplay(
                    storageClient = sourceSelection.source.storageClient,
                    storageRootUri =
                      BlobUri(scheme = "gs", bucket = sourceSelection.source.bucketName, key = ""),
                    selection = sourceSelection.selection,
                    sync = { doneBlobUri ->
                      val triggerSource = resolveReplayStorageSource(storageSources, doneBlobUri)
                      val availabilityPaths =
                        storageSources
                          .filter { it.bucketName == triggerSource.bucketName }
                          .map { it.edpImpressionPath }
                      leaseRunner.run(config.dataProvider) { lease ->
                        val doneKey = SelectedStorageClient.parseBlobUri(doneBlobUri).key
                        val doneBlob =
                          requireNotNull(triggerSource.storageClient.getBlob(doneKey)) {
                            "Completion blob does not exist: $doneBlobUri"
                          }
                        val doneGeneration =
                          requireNotNull(
                            (doneBlob as? ConditionalOperationStorageClient.Blob)
                              ?.freshnessToken
                              ?.toLongOrNull()
                          ) {
                            "Completion blob has no numeric generation: $doneBlobUri"
                          }
                        DataAvailabilitySync(
                            edpImpressionPath = triggerSource.edpImpressionPath,
                            storageClient = triggerSource.storageClient,
                            dataProvidersStub =
                              DataProvidersCoroutineStub(instrumentedKingdomChannel),
                            impressionMetadataServiceStub = impressionMetadataStub,
                            dataProviderName = config.dataProvider,
                            throttler = throttler,
                            impressionMetadataBatchSize = impressionMetadataBatchSize,
                            modelLineMap =
                              config.modelLineMapMap.mapValues { it.value.modelLinesList },
                            errorIfGapsExist = config.errorIfGapsExist,
                            availabilityImpressionPaths = availabilityPaths,
                            modelLineCutovers = modelLineCutovers,
                            modelLineDataSources = modelLineDataSources,
                            publishEventGroupActivities =
                              eventGroupActivityPublisher?.let { it::publish },
                            generationMatchedMetadataUpdater =
                              GcsGenerationMatchedBlobMetadataUpdater(
                                triggerSource.storage,
                                triggerSource.bucketName,
                              ),
                          )
                          .sync(
                            doneBlobPath = doneBlobUri,
                            dataAvailabilitySyncLease = lease.name,
                            doneBlobGeneration = doneGeneration,
                            externalDoneBlobCreateTime =
                              doneBlob.createTime.takeIf {
                                triggerSource.completionBlobType == CompletionBlobType.EXTERNAL
                              },
                            discoveryMode = DataAvailabilitySync.DiscoveryMode.FolderScan,
                            ensureLeaseActive = lease::invoke,
                          )
                      }
                    },
                  )
                  .replay()
              }
            }
            results.forEach(::logDataAvailabilityReplayResult)
            results.all { it.blockedByGaps == 0 && it.errors.isEmpty() }
          }
        }
      } finally {
        shutdownChannel(kingdomChannel)
        shutdownChannel(impressionMetadataChannel)
      }
    if (!succeeded) {
      exitProcess(1)
    }
  }

  private fun buildEventGroupActivityPublisher(
    config: DataAvailabilitySyncConfig,
    kingdomChannel: Channel,
    throttler: Throttler,
  ): CmmsEventGroupActivityPublisher? {
    if (!config.hasEventGroupActivityPublishing()) {
      return null
    }

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
    return CmmsEventGroupActivityPublisher(
      EventGroupActivitiesCoroutineStub(kingdomChannel),
      eventGroupMapStorageClient,
      eventGroupMapBlobUri.key,
      config.dataProvider,
      ZoneId.of(marketTimeZoneId),
      EventGroupActivityPublisherMetrics(Instrumentation.meter),
      throttler = throttler,
    )
  }

  private fun completionType(
    config: DataAvailabilitySyncConfig,
    bucketName: String,
    impressionPath: String,
  ): CompletionBlobType {
    val internalPath = config.vidLabeledImpressionPath
    return if (
      bucketName == config.dataAvailabilityStorage.gcs.bucketName &&
        internalPath.isNotEmpty() &&
        (impressionPath == internalPath || impressionPath.startsWith("$internalPath/"))
    ) {
      CompletionBlobType.VID_LABELER_OR_LEGACY
    } else {
      CompletionBlobType.EXTERNAL
    }
  }

  private suspend fun buildReplaySelections(
    sources: List<ReplayStorageSource>,
    defaultBucketName: String,
  ): List<ReplaySourceSelection> {
    if (replaySelection.hasBlobPrefix()) {
      val suppliedBlobPrefix = replaySelection.blobPrefix.trimEnd('/')
      val parsedPrefix =
        if (suppliedBlobPrefix.startsWith("gs://")) {
          val parsedUri = SelectedStorageClient.parseBlobUri(suppliedBlobPrefix)
          parsedUri.bucket to parsedUri.key.trimEnd('/')
        } else {
          defaultBucketName to suppliedBlobPrefix
        }
      require(sources.any { it.bucketName == parsedPrefix.first }) {
        "blob-prefix must use the configured GCS bucket: $suppliedBlobPrefix"
      }
      val scanSource =
        requireNotNull(
          sources
            .filter {
              it.bucketName == parsedPrefix.first &&
                isWithinPrefix(parsedPrefix.second, it.edpImpressionPath)
            }
            .maxByOrNull { it.edpImpressionPath.length }
        ) {
          "blob-prefix must be a configured impression path or one of its descendants"
        }
      val urisBySource = linkedMapOf<ReplayStorageSource, MutableSet<String>>()
      val prefix = if (parsedPrefix.second.isEmpty()) "" else "${parsedPrefix.second}/"
      scanSource.storageClient.listBlobs(prefix).collect { blob ->
        val source =
          findReplayStorageSource(sources, parsedPrefix.first, blob.blobKey) ?: return@collect
        if (
          CompletionBlobNames.matches(
            source.completionBlobType,
            blob.blobKey.substringAfterLast('/'),
          )
        ) {
          val uri =
            BlobUris.buildUri(
              BlobUri(scheme = "gs", bucket = source.bucketName, key = ""),
              blob.blobKey,
            )
          urisBySource.getOrPut(source) { linkedSetOf() }.add(uri)
        }
      }
      if (urisBySource.isEmpty()) {
        logger.info("Replay selection: doneBlobsSelected=0 for $suppliedBlobPrefix")
      }
      return urisBySource.map { (source, uris) ->
        ReplaySourceSelection(
          source,
          DataAvailabilitySyncReplay.Selection.DoneBlobUris(uris.toSet()),
        )
      }
    }

    val doneBlobUris = replaySelection.doneBlobUris.toSet()
    val urisBySource =
      doneBlobUris.groupBy { doneBlobUri ->
        val parsedUri = SelectedStorageClient.parseBlobUri(doneBlobUri)
        require(parsedUri.scheme == "gs") { "done-blob-uri must use the gs scheme: $doneBlobUri" }
        require(sources.any { it.bucketName == parsedUri.bucket }) {
          "done-blob-uri must use the configured GCS bucket: $doneBlobUri"
        }
        val source =
          resolveReplayStorageSource(
            sources,
            parsedUri.bucket,
            parsedUri.key,
            doneBlobUri,
            "done-blob-uri must be below a configured impression path: $doneBlobUri",
          )
        require(
          CompletionBlobNames.matches(
            source.completionBlobType,
            parsedUri.key.substringAfterLast('/'),
          )
        ) {
          "done-blob-uri has the wrong completion filename for its configured source: $doneBlobUri"
        }
        source
      }
    return urisBySource.map { (source, uris) ->
      ReplaySourceSelection(source, DataAvailabilitySyncReplay.Selection.DoneBlobUris(uris.toSet()))
    }
  }

  private fun resolveReplayStorageSource(
    sources: List<ReplayStorageSource>,
    doneBlobUri: String,
  ): ReplayStorageSource {
    val parsedUri = SelectedStorageClient.parseBlobUri(doneBlobUri)
    require(parsedUri.scheme == "gs") { "done blob must use the gs scheme: $doneBlobUri" }
    return resolveReplayStorageSource(sources, parsedUri.bucket, parsedUri.key, doneBlobUri)
  }

  private fun resolveReplayStorageSource(
    sources: List<ReplayStorageSource>,
    bucketName: String,
    blobKey: String,
    suppliedValue: String,
    errorMessage: String = "Object must be below a configured impression source: $suppliedValue",
  ): ReplayStorageSource {
    return requireNotNull(findReplayStorageSource(sources, bucketName, blobKey)) { errorMessage }
  }

  private fun findReplayStorageSource(
    sources: List<ReplayStorageSource>,
    bucketName: String,
    blobKey: String,
  ): ReplayStorageSource? =
    sources
      .filter { source ->
        source.bucketName == bucketName && isWithinPrefix(blobKey, source.replayPrefix)
      }
      .maxByOrNull { it.replayPrefix.length }

  private fun isWithinPrefix(blobKey: String, prefix: String): Boolean =
    prefix.isEmpty() || blobKey == prefix || blobKey.startsWith("$prefix/")

  private fun DataAvailabilitySyncReplay.Selection.toEventGroupActivitySelection():
    EventGroupActivityBackfill.Selection {
    return when (this) {
      is DataAvailabilitySyncReplay.Selection.BlobPrefix ->
        EventGroupActivityBackfill.Selection.BlobPrefix(value, completionBlobType)
      is DataAvailabilitySyncReplay.Selection.DoneBlobUris ->
        EventGroupActivityBackfill.Selection.DoneBlobUris(values)
    }
  }

  private fun logActivityOnlyResult(result: EventGroupActivityBackfill.Result) {
    logger.info(
      "Activity-only replay result: " +
        "doneBlobsSelected=${result.doneBlobsSelected}, " +
        "activityPairsPublished=${result.activityPairsPublished}, " +
        "noWork=${result.noWork}, " +
        "errors=${result.errors.size}"
    )
    for (error in result.errors) {
      logger.log(
        Level.WARNING,
        "Activity-only replay error [${error.doneBlobUri}]: ${error.message}",
        error.exception,
      )
    }
  }

  private fun logDataAvailabilityReplayResult(result: DataAvailabilitySyncReplay.Result) {
    logger.info(
      "Data-availability replay result: " +
        "doneBlobsSelected=${result.doneBlobsSelected}, " +
        "published=${result.published}, " +
        "noWork=${result.noWork}, " +
        "blockedByGaps=${result.blockedByGaps}, " +
        "errors=${result.errors.size}"
    )
    for (error in result.errors) {
      logger.log(
        Level.WARNING,
        "Data-availability replay error [${error.doneBlobUri}]: ${error.message}",
        error.exception,
      )
    }
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
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

fun main(args: Array<String>) = commandLineMain(ReplayDataAvailabilitySync(), args)
