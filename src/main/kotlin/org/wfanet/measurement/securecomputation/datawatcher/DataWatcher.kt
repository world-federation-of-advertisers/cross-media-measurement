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

package org.wfanet.measurement.securecomputation.datawatcher

import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import com.google.events.cloud.storage.v1.StorageObjectData
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import com.google.cloud.functions.CloudEventsFunction
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import java.nio.charset.StandardCharsets
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.common.pack
import com.google.protobuf.Any
import io.cloudevents.CloudEvent
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import kotlin.text.matches
import kotlin.io.path.Path
import org.wfanet.measurement.common.crypto.SigningCerts

class DataWatcher(
  private val workItemsService: GooglePubSubWorkItemsService,
  private val dataWatcherConfigs: List<DataWatcherConfig>
) : CloudEventsFunction {

  private fun getClientCerts(): SigningCerts {
    return SigningCerts.fromPemFiles(
      certificateFile = Path(System.getenv("CERT_FILE_PATH")).toFile(),
      privateKeyFile = Path(System.getenv("PRIVATE_KEY_FILE_PATH")).toFile(),
      trustedCertCollectionFile = Path(System.getenv("CERT_COLLECTION_FILE_PATH")).toFile(),
    )
  }

override fun service(request: HttpRequest, response: HttpResponse) {
  response.getWriter().write("OK")
  if (true) {
    val publicChannel =
      buildMutualTlsChannel(System.getenv("TARGET"), getClientCerts(), System.getenv("CERT_HOST"))

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    val requisitionsStorageClient =
      GcsStorageClient(
        StorageOptions.newBuilder()
          .setProjectId(System.getenv("REQUISITIONS_GCS_PROJECT_ID"))
          .build()
          .service,
        System.getenv("REQUISITIONS_GCS_BUCKET"),
      )
    val requisitionFetcher =
      RequisitionFetcher(
        requisitionsStub,
        requisitionsStorageClient,
        System.getenv("DATAPROVIDER_NAME"),
        System.getenv("PAGE_SIZE").toInt(),
        System.getenv("STORAGE_PATH_PREFIX"),
      )
    runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }

  override fun accept(event: CloudEvent) {
    val cloudEventData = String(event.getData().toBytes(), StandardCharsets.UTF_8)
    val builder = StorageObjectData.newBuilder()
    JsonFormat.parser().merge(cloudEventData, builder)
    val data = builder.build()
    val bucket = data.getBucket()
    val blobKey = data.getName()
    dataWatcherConfigs.forEach { config ->
      val regex = config.sourcePathRegex.toRegex()
      if (regex.containsMatchIn(blobKey)) {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (config.sinkConfigCase) {
          DataWatcherConfig.SinkConfigCase.CONTROL_PLANE_CONFIG -> {
            val queueConfig = config.controlPlaneConfig
            val workItemId = UUID.randomUUID().toString()
            val workItemParams =
              triggeredApp {
                  this.config = queueConfig.appConfig
                  this.path = path
                }
                .pack()
            val request = createWorkItemRequest {
              this.workItemId = workItemId
              this.workItem = workItem {
                queue = queueConfig.queueName
                this.workItemParams = workItemParams
              }
            }
            runBlocking { workItemsService.createWorkItem(request) }
          }
          DataWatcherConfig.SinkConfigCase.CLOUD_FUNCTION_CONFIG ->
            TODO("Cloud Function Sink not currently supported")
          DataWatcherConfig.SinkConfigCase.SINKCONFIG_NOT_SET ->
            error("Invalid sink config: ${config.sinkConfigCase}")
        }
      }
    }
  }
}
