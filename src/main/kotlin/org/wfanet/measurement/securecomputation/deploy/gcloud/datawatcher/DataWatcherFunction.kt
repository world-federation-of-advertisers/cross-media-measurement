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
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getJarResourceFile
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

/*
 * Cloud Function receives a CloudEvent. If the cloud event path matches config, it calls the
 * DataWatcher with the path and config.
 */
class DataWatcherFunction : CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    logger.fine("Starting DataWatcherFunction")
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

  companion object {
    private const val scheme = "gs"
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private val CLASS_LOADER: ClassLoader = Thread.currentThread().contextClassLoader
    private val certFilePath = checkIsPath("CERT_FILE_PATH")
    private val privateKeyFilePath = checkIsPath("PRIVATE_KEY_FILE_PATH")
    private val certCollectionFilePath = checkIsPath("CERT_COLLECTION_FILE_PATH")
    private const val dataWatcherConfigResourcePath =
      "securecomputation/datawatcher/data_watcher_config.textproto"
    private val controlPlaneTarget = checkNotEmpty("CONTROL_PLANE_TARGET")
    private val controlPlaneCertHost = checkNotEmpty("CONTROL_PLANE_CERT_HOST")
    private val channelShutdownTimeout =
      Duration.ofSeconds(
        System.getenv("CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

    private fun checkNotEmpty(envVar: String): String {
      val value = System.getenv(envVar)
      checkNotNull(value) { "Missing env var: $envVar" }
      check(value.isNotBlank())
      return value
    }

    private fun checkIsPath(envVar: String): String {
      val value = System.getenv(envVar) ?: throw IllegalStateException("Missing env var: $envVar")
      Paths.get(value)
      return value
    }

    private fun getClientCerts(): SigningCerts {
      fun logFileStatus(label: String, path: String) {
        val file = File(path)
        logger.info("$label - Path: $path")
        if (file.exists()) {
          logger.info("$label exists. Size: ${file.length()} bytes")
        } else {
          logger.severe("$label NOT FOUND at path: $path")
        }
      }

      logFileStatus("CERT_FILE", certFilePath)
      logFileStatus("PRIVATE_KEY_FILE", privateKeyFilePath)
      logFileStatus("CERT_COLLECTION_FILE", certCollectionFilePath)

      return SigningCerts.fromPemFiles(
        certificateFile =
          File(certFilePath).also { require(it.exists()) { "Cert not found: $certFilePath" } },
        privateKeyFile =
          File(privateKeyFilePath).also {
            require(it.exists()) { "Key not found: $privateKeyFilePath" }
          },
        trustedCertCollectionFile =
          File(certCollectionFilePath).also {
            require(it.exists()) { "CA not found: $certCollectionFilePath" }
          },
      )
    }

    private val publicChannel by lazy {
      buildMutualTlsChannel(controlPlaneTarget, getClientCerts(), controlPlaneCertHost)
        .withShutdownTimeout(channelShutdownTimeout)
    }

    private val workItemsStub by lazy { WorkItemsCoroutineStub(publicChannel) }
    private val config by lazy {
      checkNotNull(CLASS_LOADER.getJarResourceFile(dataWatcherConfigResourcePath))
    }
    private val dataWatcherConfig by lazy {
      runBlocking { parseTextProto(config, DataWatcherConfig.getDefaultInstance()) }
    }
    private val dataWatcher by lazy {
      DataWatcher(
        workItemsStub = workItemsStub,
        dataWatcherConfigs = dataWatcherConfig.watchedPathsList,
      )
    }
  }
}
