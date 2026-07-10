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

import io.grpc.ManagedChannel
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
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
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.EventGroupActivitySyncConfig
import org.wfanet.measurement.config.edpaggregator.EventGroupActivitySyncConfig.InputCase
import org.wfanet.measurement.edpaggregator.eventgroupactivities.EventGroupActivitySync
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SpotDataParser
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SpotRecord
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SyncMode
import org.wfanet.measurement.edpaggregator.eventgroupactivities.SyncResult
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.Command
import picocli.CommandLine.ITypeConverter
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

/**
 * Command that synchronizes `EventGroupActivity` resources for a DataProvider against the CMMS
 * Public API from a JSON spot-data file.
 *
 * Per-environment values (DataProvider, spot-data input, Kingdom target) are supplied via an
 * [EventGroupActivitySyncConfig] textproto passed with `--config-file`.
 */
@Command(
  name = "SyncEventGroupActivities",
  description = ["Syncs EventGroupActivities for a DataProvider from a JSON file"],
  mixinStandardHelpOptions = true,
)
class SyncEventGroupActivities : Runnable {
  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--config-file"],
    description = ["Path to an EventGroupActivitySyncConfig textproto"],
    required = true,
  )
  private lateinit var configFile: File

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
    names = ["--mode"],
    description =
      [
        "How the sync writes to the Kingdom: " +
          "sync (default: write creates and deletes to match input exactly), " +
          "append (write creates only; leave Kingdom records absent from input untouched), " +
          "or preview (make no mutating calls; log the planned diff)."
      ],
    converter = [SyncModeConverter::class],
    defaultValue = "sync",
  )
  private var mode: SyncMode = SyncMode.SYNC

  override fun run() {
    val config: EventGroupActivitySyncConfig =
      parseTextProto(configFile, EventGroupActivitySyncConfig.getDefaultInstance())
    require(config.inputCase != InputCase.INPUT_NOT_SET) {
      "config must set one of spot_data_blob_uri or local_file_path"
    }
    val dataProvider: String = config.dataProvider
    val kingdomPublicApiTarget: String = config.kingdomPublicApiTarget
    val kingdomPublicApiCertHost: String? = config.kingdomPublicApiCertHost.ifEmpty { null }

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
        mode = mode,
      )

    // Always shut down the channel; only exit non-zero AFTER the finally runs, since exitProcess
    // would otherwise skip it. Skipped deletes (non-FULL delete modes) are intentional, so only
    // runtime (RPC) failures drive the non-zero exit.
    val hasFailures: Boolean =
      try {
        runBlocking {
          val records: Flow<SpotRecord> = SpotDataParser.parseJson(readInput(config))
          val result: SyncResult = sync.sync(records)

          println("Sync result:")
          println("  totalInputRecords: ${result.totalInputRecords}")
          println("  eventGroupsSucceeded: ${result.eventGroupsSucceeded}")
          println("  eventGroupsFailed: ${result.eventGroupsFailed}")
          println("  activitiesCreated: ${result.activitiesCreated}")
          println("  activitiesDeleted: ${result.activitiesDeleted}")
          println("  activitiesUnchanged: ${result.activitiesUnchanged}")
          println("  activitiesWouldDelete: ${result.activitiesWouldDelete}")
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

  /** Opens the input as a stream, from either the local file or the GCS blob per the config. */
  private suspend fun readInput(config: EventGroupActivitySyncConfig): InputStream {
    return when (config.inputCase) {
      InputCase.LOCAL_FILE_PATH -> FileInputStream(File(config.localFilePath))
      InputCase.SPOT_DATA_BLOB_URI -> {
        val blobUri: String = config.spotDataBlobUri
        val parsedBlobUri = SelectedStorageClient.parseBlobUri(blobUri)
        val storageClient =
          SelectedStorageClient(
            blobUri = parsedBlobUri,
            projectId = config.gcsProject.ifEmpty { null },
          )
        val blob =
          storageClient.getBlob(parsedBlobUri.key) ?: error("Blob not found for URI: $blobUri")
        blob.read().flatten().newInput()
      }
      InputCase.INPUT_NOT_SET ->
        error("config must set one of spot_data_blob_uri or local_file_path")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Maximum time to wait for the gRPC channel to terminate during shutdown. */
    private const val SHUTDOWN_TIMEOUT_SECONDS = 30L
  }
}

private class SyncModeConverter : ITypeConverter<SyncMode> {
  override fun convert(value: String): SyncMode = SyncMode.valueOf(value.uppercase())
}

fun main(args: Array<String>) = commandLineMain(SyncEventGroupActivities(), args)
