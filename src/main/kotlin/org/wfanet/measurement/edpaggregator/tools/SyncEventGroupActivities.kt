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

import com.google.protobuf.TextFormat
import io.grpc.ManagedChannel
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.EventGroupActivitySyncConfig
import org.wfanet.measurement.edpaggregator.eventgroupactivities.EventGroupActivitySync
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SpotDataParser
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SpotRecord
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SyncResult
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

/** Source of the input data: exactly one of a GCS blob URI or a local file. */
private class InputSource {
  @Option(
    names = ["--blob-uri"],
    description = ["GCS gs:// URI of the JSON input"],
    required = true,
  )
  var blobUri: String? = null
    private set

  @Option(names = ["--file"], description = ["Local path to the JSON input"], required = true)
  var file: File? = null
    private set
}

/**
 * Command that synchronizes `EventGroupActivity` resources for a DataProvider against the CMMS
 * Public API from a JSON spot-data file.
 *
 * Per-environment values (DataProvider, spot-data blob URI, Kingdom target) can be supplied via a
 * textproto config file (`--config-file`, the production path used by the CronJob) or via
 * individual flags (the local-operator path). When both are present, the config file takes
 * precedence field-by-field — set a field in the textproto to override the flag, or leave it
 * unset to fall back.
 */
@Command(
  name = "SyncEventGroupActivities",
  description = ["Syncs EventGroupActivities for a DataProvider from a JSON file"],
  mixinStandardHelpOptions = true,
)
class SyncEventGroupActivities : Runnable {
  @ArgGroup(exclusive = true, multiplicity = "1") private lateinit var inputSource: InputSource

  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--config-file"],
    description =
      [
        "Path to an EventGroupActivitySyncConfig textproto. When set, its non-empty fields " +
          "override the matching --data-provider / --kingdom-public-api-target / " +
          "--kingdom-public-api-cert-host / --gcs-project-id flags."
      ],
    required = false,
  )
  private var configFile: File? = null

  @Option(
    names = ["--data-provider"],
    description = ["DataProvider resource name, e.g. dataProviders/CqJcvwaa5tI"],
    required = false,
  )
  private var dataProviderFlag: String? = null

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (host:port) of the Kingdom public API"],
    required = false,
  )
  private var kingdomPublicApiTargetFlag: String? = null

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description = ["Expected hostname in the Kingdom public API's TLS certificate"],
    required = false,
  )
  private var kingdomPublicApiCertHostFlag: String? = null

  @Option(
    names = ["--throttler-minimum-interval"],
    description =
      [
        "Minimum interval between gRPC calls. The sync makes one list call per EventGroup plus " +
          "one batch call per 1000 created/deleted activities, so call volume is roughly " +
          "proportional to the number of EventGroups."
      ],
    defaultValue = "100ms",
  )
  private lateinit var throttlerMinimumInterval: Duration

  @Option(
    names = ["--list-page-size"],
    description = ["Page size used when listing existing EventGroupActivities"],
    defaultValue = "50",
  )
  private var listPageSize: Int = 50

  @Option(
    names = ["--max-delete-fraction"],
    description =
      [
        "Abort deletions for an EventGroup if the fraction of existing activities to delete " +
          "exceeds this (safety guard for truncated input). 1.0 disables."
      ],
    defaultValue = "1.0",
  )
  private var maxDeleteFraction: Double = 1.0

  @Option(
    names = ["--dry-run"],
    description = ["List and diff only; make no BatchUpdate/BatchDelete calls"],
    defaultValue = "false",
  )
  private var dryRun: Boolean = false

  @Option(
    names = ["--gcs-project-id"],
    description = ["GCP project ID for reading the --blob-uri"],
    required = false,
  )
  private var gcsProjectIdFlag: String? = null

  override fun run() {
    val config: EventGroupActivitySyncConfig = loadConfigOrEmpty()
    val dataProvider: String =
      config.dataProvider.ifEmpty { dataProviderFlag }
        ?: error("--data-provider must be set (via flag or config_file.data_provider)")
    val kingdomPublicApiTarget: String =
      config.kingdomPublicApiTarget.ifEmpty { kingdomPublicApiTargetFlag }
        ?: error(
          "--kingdom-public-api-target must be set (via flag or config_file.kingdom_public_api_target)"
        )
    val kingdomPublicApiCertHost: String? =
      config.kingdomPublicApiCertHost.ifEmpty { kingdomPublicApiCertHostFlag }
    val gcsProjectId: String? = config.gcsProject.ifEmpty { gcsProjectIdFlag }
    val effectiveBlobUri: String? = config.spotDataBlobUri.ifEmpty { inputSource.blobUri }

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    val channel: ManagedChannel =
      buildMutualTlsChannel(kingdomPublicApiTarget, clientCerts, kingdomPublicApiCertHost)

    val activitiesClient = EventGroupActivitiesCoroutineStub(channel)
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)
    val sync =
      EventGroupActivitySync(
        eventGroupActivitiesClient = activitiesClient,
        throttler = throttler,
        dataProviderName = dataProvider,
        listPageSize = listPageSize,
        maxDeleteFraction = maxDeleteFraction,
      )

    // Always shut down the channel; only exit non-zero AFTER the finally runs, since exitProcess
    // would otherwise skip it. A tripped max-delete-fraction guard is intentional partial work, not
    // a failure, so only runtime (RPC) failures drive the non-zero exit.
    val hasFailures: Boolean =
      try {
        runBlocking {
          val records: Flow<SpotRecord> =
            SpotDataParser.parseJson(readInput(effectiveBlobUri, gcsProjectId))
          val result: SyncResult = sync.sync(records, dryRun = dryRun)

          println("Sync result:")
          println("  totalInputRecords: ${result.totalInputRecords}")
          println("  eventGroupsSucceeded: ${result.eventGroupsSucceeded}")
          println("  eventGroupsGuardSkipped: ${result.eventGroupsGuardSkipped}")
          println("  eventGroupsFailed: ${result.eventGroupsFailed}")
          println("  activitiesCreated: ${result.activitiesCreated}")
          println("  activitiesDeleted: ${result.activitiesDeleted}")
          println("  activitiesUnchanged: ${result.activitiesUnchanged}")
          for (skip in result.guardSkipped) {
            logger.warning("Guard-skipped deletes [${skip.eventGroup}]: ${skip.message}")
          }
          for (error in result.errors) {
            logger.warning("Sync error [${error.eventGroup}]: ${error.message}")
          }

          result.eventGroupsFailed > 0
        }
      } finally {
        channel.shutdown()
        channel.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
      }

    if (hasFailures) {
      exitProcess(1)
    }
  }

  private fun loadConfigOrEmpty(): EventGroupActivitySyncConfig {
    val file = configFile ?: return EventGroupActivitySyncConfig.getDefaultInstance()
    val builder = EventGroupActivitySyncConfig.newBuilder()
    InputStreamReader(FileInputStream(file), Charsets.UTF_8).use { reader ->
      TextFormat.merge(reader, builder)
    }
    return builder.build()
  }

  /** Opens the input as a stream, from either the local file or the GCS blob. */
  private suspend fun readInput(blobUriOverride: String?, gcsProjectId: String?): InputStream {
    val localFile = inputSource.file
    if (localFile != null) {
      return FileInputStream(localFile)
    }
    val blobUri =
      blobUriOverride
        ?: checkNotNull(inputSource.blobUri) {
          "--blob-uri must be set (or set spot_data_blob_uri in --config-file)"
        }
    val parsedBlobUri = SelectedStorageClient.parseBlobUri(blobUri)
    val storageClient = SelectedStorageClient(blobUri = parsedBlobUri, projectId = gcsProjectId)
    val blob = storageClient.getBlob(parsedBlobUri.key) ?: error("Blob not found for URI: $blobUri")
    return blob.read().flatten().newInput()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Maximum time to wait for the gRPC channel to terminate during shutdown. */
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

fun main(args: Array<String>) = commandLineMain(SyncEventGroupActivities(), args)
