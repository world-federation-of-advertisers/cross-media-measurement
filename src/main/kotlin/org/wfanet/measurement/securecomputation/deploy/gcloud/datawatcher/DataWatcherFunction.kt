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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigs
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

/*
 * The DataWatcherFunction receives a CloudEvent and calls the DataWatcher with the path and config.
 */
open class DataWatcherFunction(
  private val workItemsService: Lazy<WorkItemsCoroutineStub> = lazy {
    val projectId = getPropertyValue("CONTROL_PLANE_PROJECT_ID")
    val googlePubSubClient = DefaultGooglePubSubClient()
    GooglePubSubWorkItemsService(projectId, googlePubSubClient)
  }
) : CloudEventsFunction {

  private val schema = "gs://"
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  override fun accept(event: CloudEvent) {
    logger.fine("Starting DataWatcherFunction")
    val configData: String = getPropertyValue("DATA_WATCHER_CONFIGS")
    val dataWatcherConfigs =
      DataWatcherConfigs.newBuilder()
        .apply { TextFormat.Parser.newBuilder().build().merge(configData, this) }
        .build()
    val dataWatcher =
      DataWatcher(
        workItemsService = workItemsService.value,
        dataWatcherConfigs = dataWatcherConfigs.configsList,
      )
    val cloudEventData =
      requireNotNull(event.getData()) { "event must have data" }.toBytes().decodeToString()
    val data =
      StorageObjectData.newBuilder()
        .apply { JsonFormat.parser().merge(cloudEventData, this) }
        .build()
    val blobKey: String = data.getName()
    val bucket: String = data.getBucket()
    val path = "$schema$bucket/$blobKey"
    logger.info("Receiving path $path")
    runBlocking { dataWatcher.receivePath(path) }
  }
}

fun getPropertyValue(propertyName: String): String {
  return System.getProperty(propertyName) ?: System.getenv(propertyName)
}
