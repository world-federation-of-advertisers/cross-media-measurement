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
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.StorageParams.StorageCase
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.MissingImpressionMetadataRecovery
import org.wfanet.measurement.edpaggregator.dataavailability.MissingImpressionMetadataRecoveryMetrics
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private class FilteringBlobMetadataStorageClient(
  private val delegate: BlobMetadataStorageClient,
  private val includedBlobKeys: Set<String>,
) : BlobMetadataStorageClient by delegate {
  override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
    return delegate.listBlobs(prefix).filter { it.blobKey in includedBlobKeys }
  }
}

/** Recovers missing and incorrectly deleted `ImpressionMetadata` resources. */
@Command(
  name = "RecoverMissingImpressionMetadata",
  description = ["Recovers missing or incorrectly deleted ImpressionMetadata resources"],
  mixinStandardHelpOptions = true,
)
class RecoverMissingImpressionMetadata : Runnable {
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

  @set:Option(
    names = ["--lookback-days"],
    description = ["Lookback horizon in days, including today"],
    defaultValue = "90",
  )
  private var lookbackDays: Int by Delegates.notNull()

  @set:Option(
    names = ["--end-days-ago"],
    description = ["Days before today for the newest date folder to reconcile"],
    required = true,
  )
  private var endDaysAgo: Int by Delegates.notNull()

  override fun run() {
    val config = parseTextProto(configFile, DataAvailabilitySyncConfig.getDefaultInstance())
    require(config.dataAvailabilityStorage.storageCase == StorageCase.GCS) {
      "data_availability_storage must use GCS"
    }
    require(config.edpImpressionPath.isNotEmpty()) { "edp_impression_path must be set" }
    require(config.dataProvider.isNotEmpty()) { "data_provider must be set" }

    val storageConfig = config.dataAvailabilityStorage.gcs
    require(storageConfig.bucketName.isNotEmpty()) { "GCS bucket_name must be set" }
    require(lookbackDays > 0) { "lookback-days must be greater than zero" }
    require(endDaysAgo >= 0) { "end-days-ago must not be negative" }
    require(endDaysAgo < lookbackDays) { "end-days-ago must be less than lookback-days" }
    val storageApiEndpoint = storageApiEndpoint
    val storageClient =
      GcsStorageClient(
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
    val dataProvidersStub =
      DataProvidersCoroutineStub(
        ClientInterceptors.intercept(kingdomChannel, grpcTelemetry.newClientInterceptor())
      )
    val impressionMetadataStub =
      ImpressionMetadataServiceCoroutineStub(
        ClientInterceptors.intercept(
          impressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )
      )
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)
    val today = LocalDate.now(ZoneOffset.UTC)
    val latestDataDate = today.minusDays(endDaysAgo.toLong())
    val recovery =
      MissingImpressionMetadataRecovery(
        storageClient = storageClient,
        storageRootUri = BlobUri(scheme = "gs", bucket = storageConfig.bucketName, key = ""),
        edpImpressionPath = config.edpImpressionPath,
        impressionMetadataStub = impressionMetadataStub,
        dataProviderName = config.dataProvider,
        throttler = throttler,
        impressionMetadataBatchSize = impressionMetadataBatchSize,
        earliestDataDate = today.minusDays((lookbackDays - 1).toLong()),
        latestDataDate = latestDataDate,
        sync = { doneBlobUri, metadataBlobKeys ->
          DataAvailabilitySync(
              edpImpressionPath = config.edpImpressionPath,
              storageClient = FilteringBlobMetadataStorageClient(storageClient, metadataBlobKeys),
              dataProvidersStub = dataProvidersStub,
              impressionMetadataServiceStub = impressionMetadataStub,
              dataProviderName = config.dataProvider,
              throttler = throttler,
              impressionMetadataBatchSize = impressionMetadataBatchSize,
              modelLineMap = config.modelLineMapMap.mapValues { it.value.modelLinesList },
              errorIfGapsExist = config.errorIfGapsExist,
            )
            .sync(doneBlobUri)
        },
        metrics = MissingImpressionMetadataRecoveryMetrics(Instrumentation.meter),
      )

    val result =
      try {
        runBlocking { recovery.recover() }
      } finally {
        shutdownChannel(kingdomChannel)
        shutdownChannel(impressionMetadataChannel)
      }

    logger.info(
      "Recovery result: " +
        "finalizedMetadataBlobs=${result.finalizedMetadataBlobs}, " +
        "missingBlobs=${result.missingBlobs}, " +
        "deletedRecordsWithBlobs=${result.deletedRecordsWithBlobs}, " +
        "undeletedRecords=${result.undeletedRecords}, " +
        "failedUndeletes=${result.failedUndeletes}, " +
        "recoveredBlobs=${result.recoveredBlobs}, " +
        "failedBlobs=${result.failedBlobs}, " +
        "dateFoldersResynced=${result.dateFoldersResynced}, " +
        "incompleteFullSyncFolders=${result.incompleteFullSyncFolders}"
    )
    for (error in result.errors) {
      logger.warning("Recovery error [${error.target}]: ${error.message}")
    }

    if (result.errors.isNotEmpty()) {
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
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

fun main(args: Array<String>) = commandLineMain(RecoverMissingImpressionMetadata(), args)
