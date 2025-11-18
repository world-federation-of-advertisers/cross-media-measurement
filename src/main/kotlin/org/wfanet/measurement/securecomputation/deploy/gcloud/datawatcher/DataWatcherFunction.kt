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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import io.grpc.ClientInterceptors
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getConfigAsProtoMessage
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

/*
 * Cloud Function receives a CloudEvent. If the cloud event path matches config, it calls the
 * DataWatcher with the path and config.
 *
 * @param pathReceiver Function to handle received paths. Defaults to the production DataWatcher,
 *   but can be overridden in tests to inject custom behavior without relying on static test hooks.
 */
class DataWatcherFunction(
  private val pathReceiver: suspend (String) -> Unit = { path ->
    defaultDataWatcher.receivePath(path)
  }
) : CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    try {
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
      val size = data.size
      if (size == 0L) {
        // TODO(world-federation-of-advertisers/cross-media-measurement#2653): Update logic once
        // metadata storage are in place
        // Temporary accept empty blob if path ends with "done"
        if (!path.lowercase().endsWith(VALID_EMPTY_BLOB_NAME)) {
          logger.info("Skipping processing: file '$path' is empty and not a done marker")
          return
        }
      }
      Tracing.withW3CTraceContext(event) {
        Tracing.trace(
          spanName = SPAN_DATA_WATCHER_HANDLE_EVENT,
          attributes =
            Attributes.of(
              ATTR_BUCKET_NAME,
              bucket,
              ATTR_BLOB_NAME,
              blobKey,
              ATTR_DATA_PATH,
              path,
              ATTR_BLOB_SIZE_BYTES,
              size,
            ),
        ) {
          val currentContext = Context.current()
          runBlocking(currentContext.asContextElement()) { pathReceiver(path) }
        }
      }
    } finally {
      // Critical for Cloud Functions: flush metrics before function freezes
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private const val VALID_EMPTY_BLOB_NAME = "done"
    private const val scheme = "gs"
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    init {
      EdpaTelemetry.ensureInitialized()
    }

    /**
     * OpenTelemetry gRPC instrumentation using the global instance initialized by [EdpaTelemetry].
     */
    private val grpcTelemetry by lazy { GrpcTelemetry.create(Instrumentation.openTelemetry) }

    private val ATTR_BUCKET_NAME = AttributeKey.stringKey("bucket")
    private val ATTR_BLOB_NAME = AttributeKey.stringKey("blob_name")
    private val ATTR_DATA_PATH = AttributeKey.stringKey("data_path")
    private val ATTR_BLOB_SIZE_BYTES = AttributeKey.longKey("blob_size_bytes")
    private const val SPAN_DATA_WATCHER_HANDLE_EVENT = "data_watcher.handle_event"
    private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private val certFilePath: String by lazy { checkIsPath("CERT_FILE_PATH") }
    private val privateKeyFilePath: String by lazy { checkIsPath("PRIVATE_KEY_FILE_PATH") }
    private val certCollectionFilePath: String by lazy { checkIsPath("CERT_COLLECTION_FILE_PATH") }
    private val controlPlaneTarget: String by lazy { checkNotEmpty("CONTROL_PLANE_TARGET") }
    private val controlPlaneCertHost: String by lazy { checkNotEmpty("CONTROL_PLANE_CERT_HOST") }
    private val channelShutdownTimeout: Duration by lazy {
      Duration.ofSeconds(
        System.getenv("CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
      )
    }

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
      val channel =
        buildMutualTlsChannel(controlPlaneTarget, getClientCerts(), controlPlaneCertHost)
          .withShutdownTimeout(channelShutdownTimeout)
      ClientInterceptors.intercept(channel, grpcTelemetry.newClientInterceptor())
    }

    private val workItemsStub by lazy { WorkItemsCoroutineStub(publicChannel) }

    private const val CONFIG_BLOB_KEY = "data-watcher-config.textproto"
    private val dataWatcherConfig by lazy {
      runBlocking {
        getConfigAsProtoMessage(
          CONFIG_BLOB_KEY,
          DataWatcherConfig.getDefaultInstance(),
          TypeRegistry.newBuilder().add(ResultsFulfillerParams.getDescriptor()).build(),
        )
      }
    }
    private val defaultDataWatcher by lazy {
      DataWatcher(
        workItemsStub = workItemsStub,
        dataWatcherConfigs = dataWatcherConfig.watchedPathsList,
      )
    }
  }
}
