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
import java.io.File
import java.net.URI
import java.time.Duration
import java.util.logging.Logger
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.Campaigns
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroupMap
import org.wfanet.measurement.storage.SelectedStorageClient
import java.io.BufferedReader

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
    val blobUri = SelectedStorageClient.parseBlobUri(eventGroupSyncConfig.campaignsBlobUri)
    val inputStorageClient =
      SelectedStorageClient(
        blobUri = blobUri,
        rootDirectory =
          if (System.getenv("FILE_STORAGE_ROOT") != null) File(System.getenv("FILE_STORAGE_ROOT"))
          else null,
        projectId = System.getenv("GCS_PROJECT_ID"),
      )
    val campaigns = runBlocking {
      Campaigns.parseFrom(
        inputStorageClient.getBlob(blobUri.key)!!.read().flatten()
      )
    }
    val publicChannel =
      buildMutualTlsChannel(
          System.getenv("KINGDOM_TARGET"),
          getClientCerts(),
          System.getenv("KINGDOM_CERT_HOST"),
        )
        .withShutdownTimeout(
          Duration.ofSeconds(System.getenv("KINGDOM_SHUTDOWN_DURATION_SECONDS")?.toLong() ?: KINGDOM_SHUTDOWN_DURATION_SECONDS)
        )

    val eventGroupsClient = EventGroupsCoroutineStub(publicChannel)
    val eventGroupSync =
      EventGroupSync(
        edpName = campaigns.dataProvider,
        eventGroupsStub = eventGroupsClient,
        eventGroups = campaigns.eventGroupsList,
      )
    val mappedData = runBlocking { eventGroupSync.sync() }
    runBlocking {
      SelectedStorageClient(
          url = eventGroupSyncConfig.eventGroupMapUri,
          rootDirectory =
            if (System.getenv("FILE_STORAGE_ROOT") != null) File(System.getenv("FILE_STORAGE_ROOT"))
            else null,
          projectId = System.getenv("GCS_PROJECT_ID"),
        )
        .writeBlob(
          blobUri.key,
          eventGroupMap { eventGroupMap.putAll(mappedData) }.toByteString(),
        )
    }
  }

  private fun getClientCerts(): SigningCerts {
    return SigningCerts.fromPemFiles(
      certificateFile = Path(System.getenv("CERT_FILE_PATH")).toFile(),
      privateKeyFile = Path(System.getenv("PRIVATE_KEY_FILE_PATH")).toFile(),
      trustedCertCollectionFile = Path(System.getenv("CERT_COLLECTION_FILE_PATH")).toFile(),
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val KINGDOM_SHUTDOWN_DURATION_SECONDS: Long = 3L
  }
}
