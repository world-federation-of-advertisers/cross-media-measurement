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
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
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
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.dataavailability.CmmsEventGroupActivityPublisher
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.DataDateSelection
import org.wfanet.measurement.edpaggregator.dataavailability.EventGroupActivityPublisherMetrics
import org.wfanet.measurement.edpaggregator.dataavailability.GenerationMatchedBlobMetadataUpdater
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcManagedDoneObjectResolver
import org.wfanet.measurement.edpaggregator.dataavailability.MissingImpressionMetadataRecovery
import org.wfanet.measurement.edpaggregator.dataavailability.MissingImpressionMetadataRecoveryMetrics
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability.GcsGenerationMatchedBlobMetadataUpdater
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private data class RecoveryStorageSource(
  val storage: Storage,
  val storageClient: GcsStorageClient,
  val bucketName: String,
  val edpImpressionPath: String,
)

private class FilteringBlobMetadataStorageClient(
  private val delegate: BlobMetadataStorageClient,
  private val includedBlobKeys: Set<String>,
  private val metadataFolderPrefix: String,
) : BlobMetadataStorageClient by delegate {
  private val mutableProcessedMetadataBlobKeys = mutableSetOf<String>()

  val processedMetadataBlobKeys: Set<String>
    get() = mutableProcessedMetadataBlobKeys

  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    // Keep model-line scans complete for provider-wide gap checks.
    val blobs = delegate.listBlobs(prefix)
    return if (prefix?.trimEnd('/') == metadataFolderPrefix.trimEnd('/')) {
      blobs.filter { it.blobKey in includedBlobKeys }
    } else {
      blobs
    }
  }

  override suspend fun updateBlobMetadata(
    blobKey: String,
    customCreateTime: Instant?,
    metadata: Map<String, String>,
  ) {
    delegate.updateBlobMetadata(blobKey, customCreateTime, metadata)
    if (
      blobKey in includedBlobKeys &&
        metadata[DataAvailabilityBlobs.SYNCED_BY_KEY] == DataAvailabilityBlobs.SYNCED_BY_VALUE
    ) {
      mutableProcessedMetadataBlobKeys += blobKey
    }
  }
}

/** Recovers missing and incorrectly deleted `ImpressionMetadata` resources. */
@Command(
  name = "RecoverMissingImpressionMetadata",
  description = ["Recovers missing or incorrectly deleted ImpressionMetadata resources"],
  mixinStandardHelpOptions = true,
)
class RecoverMissingImpressionMetadata : Runnable {
  private class DateRange {
    @set:Option(
      names = ["--lookback-days"],
      description = ["Lookback horizon in days, including today"],
      defaultValue = "90",
    )
    var lookbackDays: Int by Delegates.notNull()

    @set:Option(
      names = ["--end-days-ago"],
      description = ["Days before today for the newest date folder to reconcile"],
      required = true,
    )
    var endDaysAgo: Int by Delegates.notNull()
  }

  private class DateSelection {
    @Option(
      names = ["--data-date"],
      description = ["Exact UTC data date to reconcile (YYYY-MM-DD); may be repeated"],
      required = true,
    )
    var dataDates: List<LocalDate> = emptyList()

    @ArgGroup(exclusive = false, multiplicity = "1") var dateRange: DateRange? = null
  }

  @ArgGroup(exclusive = true, multiplicity = "1", heading = "Date selection:%n")
  private lateinit var dateSelection: DateSelection

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

  @set:Option(
    names = ["--publish-event-group-activities"],
    description = ["Publish EventGroup activity for metadata resynchronized by this recovery run"],
    defaultValue = "false",
  )
  private var publishEventGroupActivities: Boolean by Delegates.notNull()

  override fun run() {
    require(impressionMetadataBatchSize in 1..MAX_IMPRESSION_METADATA_BATCH_SIZE) {
      "--impression-metadata-batch-size must be between 1 and " + MAX_IMPRESSION_METADATA_BATCH_SIZE
    }
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
    require(!publishEventGroupActivities || config.hasEventGroupActivityPublishing()) {
      "event_group_activity_publishing must be set when publishing EventGroup activity"
    }

    val storageConfig = config.dataAvailabilityStorage.gcs
    require(storageConfig.bucketName.isNotEmpty()) { "GCS bucket_name must be set" }
    val today = LocalDate.now(ZoneOffset.UTC)
    val selectedDataDates = dateSelection.dataDates.toSet()
    val recoveryDateSelection =
      if (selectedDataDates.isNotEmpty()) {
        require(selectedDataDates.none { it.isAfter(today) }) {
          "data-date must not be after the current UTC date"
        }
        DataDateSelection.SelectedDates(selectedDataDates)
      } else {
        val dateRange = checkNotNull(dateSelection.dateRange)
        require(dateRange.lookbackDays > 0) { "lookback-days must be greater than zero" }
        require(dateRange.endDaysAgo >= 0) { "end-days-ago must not be negative" }
        require(dateRange.endDaysAgo < dateRange.lookbackDays) {
          "end-days-ago must be less than lookback-days"
        }
        DataDateSelection.Range(
          earliestDate = today.minusDays((dateRange.lookbackDays - 1).toLong()),
          latestDate = today.minusDays(dateRange.endDaysAgo.toLong()),
        )
      }
    val storageApiEndpoint = storageApiEndpoint
    val storage =
      StorageOptions.newBuilder()
        .also {
          if (storageConfig.projectId.isNotEmpty()) {
            it.setProjectId(storageConfig.projectId)
          }
          if (storageApiEndpoint != null) {
            it.setHost(storageApiEndpoint)
            it.setCredentials(NoCredentials.getInstance())
          }
        }
        .build()
        .service
    val storageClient = GcsStorageClient(storage, storageConfig.bucketName)
    val modelLineStorageSources =
      config.modelLineDataSourcesMap.mapValues { (modelLine, source) ->
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
        RecoveryStorageSource(
          storage = sourceStorage,
          storageClient = GcsStorageClient(sourceStorage, sourceStorageConfig.bucketName),
          bucketName = sourceStorageConfig.bucketName,
          edpImpressionPath = source.edpImpressionPath,
        )
      }
    val modelLineDataSources =
      modelLineStorageSources.mapValues { (_, source) ->
        DataAvailabilitySync.ModelLineDataSource(
          storageClient = source.storageClient,
          edpImpressionPath = source.edpImpressionPath,
        )
      }
    val recoverySources =
      (configuredPaths.map { impressionPath ->
          RecoveryStorageSource(
            storage = storage,
            storageClient = storageClient,
            bucketName = storageConfig.bucketName,
            edpImpressionPath = impressionPath,
          )
        } + modelLineStorageSources.values)
        .distinctBy { it.bucketName to it.edpImpressionPath }
    val kingdomChannel =
      buildChannel(config.cmmsConnection, kingdomPublicApiTarget, kingdomPublicApiCertHost)
    val impressionMetadataChannel =
      buildChannel(
        config.impressionMetadataStorageConnection,
        impressionMetadataApiTarget,
        impressionMetadataApiCertHost,
      )
    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
    val dataProvidersStub =
      DataProvidersCoroutineStub(
        ClientInterceptors.intercept(kingdomChannel, grpcTelemetry.newClientInterceptor())
      )
    val instrumentedMetadataChannel =
      ClientInterceptors.intercept(impressionMetadataChannel, grpcTelemetry.newClientInterceptor())
    val impressionMetadataStub = ImpressionMetadataServiceCoroutineStub(instrumentedMetadataChannel)
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)
    val eventGroupActivityPublisher =
      if (publishEventGroupActivities) {
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
        CmmsEventGroupActivityPublisher(
          EventGroupActivitiesCoroutineStub(
            ClientInterceptors.intercept(kingdomChannel, grpcTelemetry.newClientInterceptor())
          ),
          eventGroupMapStorageClient,
          eventGroupMapBlobUri.key,
          config.dataProvider,
          ZoneId.of(marketTimeZoneId),
          EventGroupActivityPublisherMetrics(Instrumentation.meter),
          throttler = throttler,
        )
      } else {
        null
      }
    val dataAvailabilitySyncLeaseRunner =
      DataAvailabilitySyncLeaseRunner(
        GrpcDataAvailabilitySyncLeaseClient(
          DataAvailabilitySyncLeaseServiceCoroutineStub(
            ClientInterceptors.intercept(
              impressionMetadataChannel,
              grpcTelemetry.newClientInterceptor(),
            )
          )
        ),
        renewalDelay = { delay(it) },
      )
    val results =
      try {
        runBlocking {
          dataAvailabilitySyncLeaseRunner.run(config.dataProvider) { lease ->
            recoverySources.map { source ->
              val availabilityPaths =
                recoverySources
                  .filter { it.bucketName == source.bucketName }
                  .map { it.edpImpressionPath }
              MissingImpressionMetadataRecovery(
                  storageClient = source.storageClient,
                  storageRootUri = BlobUri(scheme = "gs", bucket = source.bucketName, key = ""),
                  edpImpressionPath = source.edpImpressionPath,
                  impressionMetadataStub = impressionMetadataStub,
                  dataProviderName = config.dataProvider,
                  throttler = throttler,
                  impressionMetadataBatchSize = impressionMetadataBatchSize,
                  dateSelection = recoveryDateSelection,
                  dataAvailabilitySyncLease = lease.name,
                  ensureLeaseActive = lease::invoke,
                  sync = { doneBlobUri, metadataBlobKeys ->
                    val filteringStorageClient =
                      FilteringBlobMetadataStorageClient(
                        source.storageClient,
                        metadataBlobKeys,
                        SelectedStorageClient.parseBlobUri(doneBlobUri).key.substringBeforeLast("/"),
                      )
                    val doneKey = SelectedStorageClient.parseBlobUri(doneBlobUri).key
                    val replacementOutput =
                      config.modelLineCutoversList.any { cutover ->
                        val contributor =
                          config.modelLineDataSourcesMap[cutover.replacementModelLine]
                        if (
                          contributor == null ||
                            contributor.storage.gcs.bucketName != source.bucketName
                        ) {
                          false
                        } else {
                          val modelLineId = cutover.replacementModelLine.substringAfterLast('/')
                          val prefix =
                            if (contributor.edpImpressionPath.isEmpty()) {
                              "model-line/$modelLineId/"
                            } else {
                              "${contributor.edpImpressionPath}/model-line/$modelLineId/"
                            }
                          doneKey.startsWith(prefix)
                        }
                      }
                    val external =
                      !replacementOutput &&
                        source.bucketName == storageConfig.bucketName &&
                        doneKey.startsWith("$externalPath/")
                    val doneBlob =
                      checkNotNull(source.storageClient.getBlob(doneKey)) {
                        "Completion blob not found: $doneBlobUri"
                      }
                    val doneGeneration =
                      requireNotNull(
                        (doneBlob as? ConditionalOperationStorageClient.Blob)
                          ?.freshnessToken
                          ?.toLongOrNull()
                      ) {
                        "Completion blob has no numeric generation"
                      }
                    val processedMetadataBlobKeys = mutableSetOf<String>()
                    val metadataUpdater =
                      GcsGenerationMatchedBlobMetadataUpdater(source.storage, source.bucketName)
                    DataAvailabilitySync(
                        edpImpressionPath = source.edpImpressionPath,
                        storageClient =
                          if (external) source.storageClient else filteringStorageClient,
                        dataProvidersStub = dataProvidersStub,
                        impressionMetadataServiceStub = impressionMetadataStub,
                        dataProviderName = config.dataProvider,
                        throttler = throttler,
                        impressionMetadataBatchSize = impressionMetadataBatchSize,
                        modelLineMap = config.modelLineMapMap.mapValues { it.value.modelLinesList },
                        errorIfGapsExist = config.errorIfGapsExist,
                        managedDoneObjectResolver = {
                          GrpcManagedDoneObjectResolver(
                            config.dataProvider,
                            RawImpressionUploadServiceCoroutineStub(instrumentedMetadataChannel),
                            RawImpressionUploadModelLineServiceCoroutineStub(
                              instrumentedMetadataChannel
                            ),
                            RawImpressionUploadFileServiceCoroutineStub(
                              instrumentedMetadataChannel
                            ),
                            VidLabelingJobServiceCoroutineStub(instrumentedMetadataChannel),
                            throttler,
                          )
                        },
                        availabilityImpressionPaths = availabilityPaths,
                        modelLineCutovers =
                          config.modelLineCutoversList.map(ModelLineCutover::from),
                        modelLineDataSources = modelLineDataSources,
                        publishEventGroupActivities =
                          eventGroupActivityPublisher?.let { it::publish },
                        generationMatchedMetadataUpdater =
                          GenerationMatchedBlobMetadataUpdater {
                            key,
                            generation,
                            createTime,
                            metadata ->
                            metadataUpdater.update(key, generation, createTime, metadata)
                            if (
                              key in metadataBlobKeys &&
                                metadata[DataAvailabilityBlobs.SYNCED_BY_KEY] ==
                                  DataAvailabilityBlobs.SYNCED_BY_VALUE
                            ) {
                              processedMetadataBlobKeys += key
                            }
                          },
                      )
                      .sync(
                        doneBlobUri,
                        lease.name,
                        doneGeneration,
                        externalDoneBlobCreateTime = doneBlob.createTime.takeIf { external },
                        discoveryMode =
                          if (external)
                            DataAvailabilitySync.DiscoveryMode.SelectedMetadataBlobs(
                              metadataBlobKeys
                            )
                          else DataAvailabilitySync.DiscoveryMode.FolderScan,
                        ensureLeaseActive = lease::invoke,
                      )
                    if (external) processedMetadataBlobKeys
                    else filteringStorageClient.processedMetadataBlobKeys
                  },
                  metrics = MissingImpressionMetadataRecoveryMetrics(Instrumentation.meter),
                )
                .recover()
            }
          }
        }
      } finally {
        shutdownChannel(kingdomChannel)
        shutdownChannel(impressionMetadataChannel)
      }

    logger.info(
      "Recovery result: " +
        "finalizedMetadataBlobs=${results.sumOf { it.finalizedMetadataBlobs }}, " +
        "missingBlobs=${results.sumOf { it.missingBlobs }}, " +
        "deletedRecordsWithBlobs=${results.sumOf { it.deletedRecordsWithBlobs }}, " +
        "undeletedRecords=${results.sumOf { it.undeletedRecords }}, " +
        "failedUndeletes=${results.sumOf { it.failedUndeletes }}, " +
        "recoveredBlobs=${results.sumOf { it.recoveredBlobs }}, " +
        "failedBlobs=${results.sumOf { it.failedBlobs }}, " +
        "dateFoldersResynced=${results.sumOf { it.dateFoldersResynced }}, " +
        "incompleteFullSyncFolders=${results.sumOf { it.incompleteFullSyncFolders }}"
    )
    for (error in results.flatMap { it.errors }) {
      logger.warning("Recovery error [${error.target}]: ${error.message}")
    }

    if (results.any { it.errors.isNotEmpty() }) {
      exitProcess(1)
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
    private const val MAX_IMPRESSION_METADATA_BATCH_SIZE = 100
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

fun main(args: Array<String>) = commandLineMain(RecoverMissingImpressionMetadata(), args)
