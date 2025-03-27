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

package org.wfanet.measurement.securecomputation.deploy.gcloud.eventgroups

import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync

/*
 * The EventGroupSyncFunction receives a CloudEvent, syncs the Kingdom's event groups and writes out a map of eventGroupReferenceId to Event Group resource name that an EDP can later consume.
 */
open class EventGroupSyncFunction() : CloudEventsFunction {

  private val schema = "gs://"
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  override fun accept(event: CloudEvent) {
    logger.fine("Starting EventGroupSyncFunction")
    val configData: String = getPropertyValue("EVENT_GROUP_SYNC_CONFIG")
    val eventGroupSync =
      EventGroupSync(
        eventGroupsStub = eventGroupsClient,
        campaigns = campaigns,
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
