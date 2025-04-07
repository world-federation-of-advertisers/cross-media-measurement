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
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getJarResourceFile
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.DataWatcherConfigs
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

/*
 * Cloud Function receives a CloudEvent. If the cloud event path matches config, it calls the
 * DataWatcher with the path and config.
 */
class DataWatcherFunction : CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    logger.fine("Starting DataWatcherFunction")
    val publicChannel =
      buildMutualTlsChannel(
          System.getenv("CONTROL_PLANE_TARGET"),
          getClientCerts(),
          System.getenv("CONTROL_PLANE_CERT_HOST"),
        )
        .withShutdownTimeout(
          Duration.ofSeconds(
            System.getenv("CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
              ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
          )
        )

    val workItemsStub = WorkItemsCoroutineStub(publicChannel)
    val config =
      checkNotNull(
        CLASS_LOADER.getJarResourceFile(System.getenv("DATA_WATCHER_CONFIG_RESOURCE_PATH"))
      )
    val dataWatcherConfigs = parseTextProto(config, DataWatcherConfigs.getDefaultInstance())
    val dataWatcher =
      DataWatcher(
        workItemsStub = workItemsStub,
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
    val path = "$scheme://$bucket/$blobKey"
    logger.info("Receiving path $path")
    runBlocking { dataWatcher.receivePath(path) }
  }

  private fun getClientCerts(): SigningCerts {
    return SigningCerts.fromPemFiles(
      certificateFile =
        checkNotNull(CLASS_LOADER.getJarResourceFile(System.getenv("CERT_FILE_PATH"))),
      privateKeyFile =
        checkNotNull(CLASS_LOADER.getJarResourceFile(System.getenv("PRIVATE_KEY_FILE_PATH"))),
      trustedCertCollectionFile =
        checkNotNull(CLASS_LOADER.getJarResourceFile(System.getenv("CERT_COLLECTION_FILE_PATH"))),
    )
  }

  companion object {
    private const val scheme = "gs"
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private val CLASS_LOADER: ClassLoader = Thread.currentThread().contextClassLoader
  }
}
