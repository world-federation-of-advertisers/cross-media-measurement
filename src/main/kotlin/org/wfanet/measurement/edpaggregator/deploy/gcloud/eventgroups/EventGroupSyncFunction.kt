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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.protobuf.util.JsonFormat
import java.io.BufferedReader
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/*
 * Cloud Run Function that receives a [HTTPRequest] with EventGroupSyncConfig. It updates/registers
 * EventGroups with the kingdom and writes a map of the registered resource names to storage.
 */
class EventGroupSyncFunction() : HttpFunction {

  override fun service(request: HttpRequest, response: HttpResponse) {
    logger.fine("Starting EventGroupSyncFunction")
    val requestBody: BufferedReader = request.getReader()
    val eventGroupSyncConfig =
      EventGroupSyncConfig.newBuilder()
        .apply { JsonFormat.parser().merge(requestBody, this) }
        .build()

    val signingCerts =
      SigningCerts.fromPemFiles(
        certificateFile = checkNotNull(File(eventGroupSyncConfig.cmmsConnection.certFilePath)),
        privateKeyFile = checkNotNull(File(eventGroupSyncConfig.cmmsConnection.privateKeyFilePath)),
        trustedCertCollectionFile =
          checkNotNull(File(eventGroupSyncConfig.cmmsConnection.certCollectionFilePath)),
      )
    val publicChannel =
      buildMutualTlsChannel(kingdomTarget, signingCerts, kingdomCertHost)
        .withShutdownTimeout(channelShutdownDuration)

    val eventGroupsClient = EventGroupsCoroutineStub(publicChannel)
    val eventGroups = runBlocking {
      val eventGroupsBlobUri =
        SelectedStorageClient.parseBlobUri(eventGroupSyncConfig.eventGroupsBlobUri)
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = eventGroupsBlobUri,
            rootDirectory =
              if (eventGroupSyncConfig.eventGroupStorage.hasFileSystem())
                File(checkNotNull(fileSystemStorageRoot))
              else null,
            projectId = eventGroupSyncConfig.eventGroupStorage.gcs.projectId,
          )
        )
        .getBlob(eventGroupsBlobUri.key)!!
        .read()
        .map { EventGroup.parseFrom(it) }
    }
    val eventGroupSync =
      EventGroupSync(
        edpName = eventGroupSyncConfig.dataProvider,
        eventGroupsStub = eventGroupsClient,
        eventGroups = eventGroups,
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerDuration),
      )
    val mappedData = runBlocking { eventGroupSync.sync() }
    runBlocking {
      val mappedDataBlobUri =
        SelectedStorageClient.parseBlobUri(eventGroupSyncConfig.eventGroupMapBlobUri)
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = mappedDataBlobUri,
            rootDirectory =
              if (eventGroupSyncConfig.eventGroupMapStorage.hasFileSystem())
                File(checkNotNull(fileSystemStorageRoot))
              else null,
            projectId = eventGroupSyncConfig.eventGroupMapStorage.gcs.projectId,
          )
        )
        .writeBlob(mappedDataBlobUri.key, mappedData.map { it.toByteString() })
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val KINGDOM_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private const val THROTTLER_DURATION_MILLIS = 1000L

    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
    private val throttlerDuration =
      Duration.ofMillis(System.getenv("THROTTLER_MILLIS")?.toLong() ?: THROTTLER_DURATION_MILLIS)
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("KINGDOM_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: KINGDOM_SHUTDOWN_DURATION_SECONDS
      )
    private val fileSystemStorageRoot = System.getenv("FILE_STORAGE_ROOT")
  }
}
