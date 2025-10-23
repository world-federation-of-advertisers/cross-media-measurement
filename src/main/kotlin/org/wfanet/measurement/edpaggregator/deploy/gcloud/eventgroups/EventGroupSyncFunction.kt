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
import io.grpc.ClientInterceptors
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.BufferedReader
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.EventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroups
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/*
 * Cloud Run Function that receives a [HTTPRequest] with EventGroupSyncConfig. It updates/registers
 * EventGroups with the kingdom and writes a map of the registered resource names to storage.
 */
class EventGroupSyncFunction() : HttpFunction {

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting EventGroupSyncFunction")
      val requestBody: BufferedReader = request.getReader()
      val eventGroupSyncConfig =
        EventGroupSyncConfig.newBuilder()
          .apply { JsonFormat.parser().merge(requestBody, this) }
          .build()

      Tracing.withW3CTraceContext(request.headers) {
        Tracing.trace(spanName = SPAN_EVENT_GROUP_SYNC) {
          runBlocking(Context.current().asContextElement()) {
            val eventGroupsClient =
              createEventGroupsStub(
                target = kingdomTarget,
                eventGroupSyncConfig = eventGroupSyncConfig,
                certHost = kingdomCertHost,
                shutdownTimeout = channelShutdownDuration,
              )
            val eventGroups: Flow<EventGroup> = loadEventGroups(eventGroupSyncConfig)

            writeEventGroupMapToStorage(
              mappedData =
                EventGroupSync(
                    edpName = eventGroupSyncConfig.dataProvider,
                    eventGroupsStub = eventGroupsClient,
                    eventGroups = eventGroups,
                    throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerDuration),
                  )
                  .sync(),
              eventGroupSyncConfig = eventGroupSyncConfig,
            )
          }
        }
      }
    } finally {
      // Critical: flush metrics and traces before function terminates
      // Without this, all telemetry recorded during execution will be lost
      // because Cloud Functions freeze background threads after return
      EdpaTelemetry.flush()
    }
  }

  /**
   * Writes the event group mapping data to blob storage.
   *
   * @param mappedData Flow of MappedEventGroup containing event group reference IDs and resource
   *   names
   * @param eventGroupSyncConfig Configuration specifying storage location and credentials
   */
  private suspend fun writeEventGroupMapToStorage(
    mappedData: Flow<org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup>,
    eventGroupSyncConfig: EventGroupSyncConfig,
  ) {
    withWriteEventGroupMapSpan(
        blobUri = eventGroupSyncConfig.eventGroupMapBlobUri,
        dataProvider = eventGroupSyncConfig.dataProvider,
      ) {
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

  private suspend fun loadEventGroups(
    eventGroupSyncConfig: EventGroupSyncConfig
  ): Flow<EventGroup> {
    val eventGroupsBlobUriSpec = eventGroupSyncConfig.eventGroupsBlobUri
    return withLoadEventGroupsSpan(
      blobUri = eventGroupsBlobUriSpec,
      dataProvider = eventGroupSyncConfig.dataProvider,
    ) {
      val eventGroupsBlobUri = SelectedStorageClient.parseBlobUri(eventGroupsBlobUriSpec)

      when {
        eventGroupsBlobUri.key.endsWith(PROTO_FILE_SUFFIX) -> {
          getEventGroupsFromProtoFormat(eventGroupsBlobUri, eventGroupSyncConfig)
        }
        eventGroupsBlobUri.key.endsWith(JSON_FILE_SUFFIX) -> {
          getEventGroupsFromJsonFormat(eventGroupsBlobUri, eventGroupSyncConfig)
        }
        else -> error("Unsupported EventGroup file format: ${eventGroupsBlobUri.key}")
      }
    }
  }

  private suspend fun <T> withLoadEventGroupsSpan(
    blobUri: String,
    dataProvider: String,
    block: suspend () -> T,
  ): T =
    Tracing.traceSuspending(
      spanName = SPAN_LOAD_EVENT_GROUPS,
      attributes = Attributes.of(ATTR_BLOB_URI, blobUri, ATTR_DATA_PROVIDER_NAME, dataProvider),
      block = block,
    )

  private suspend fun <T> withWriteEventGroupMapSpan(
    blobUri: String,
    dataProvider: String,
    block: suspend () -> T,
  ): T =
    Tracing.traceSuspending(
      spanName = SPAN_WRITE_EVENT_GROUP_MAP,
      attributes = Attributes.of(ATTR_BLOB_URI, blobUri, ATTR_DATA_PROVIDER_NAME, dataProvider),
      block = block,
    )

  private suspend fun getEventGroupsFromProtoFormat(
    eventGroupsBlobUri: BlobUri,
    eventGroupSyncConfig: EventGroupSyncConfig,
  ): Flow<EventGroup> {
    val storageClient =
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

    val blob =
      storageClient.getBlob(eventGroupsBlobUri.key)
        ?: throw IllegalStateException("Blob not found for key: ${eventGroupsBlobUri.key}")
    return blob.read().map { bytes -> EventGroup.parseFrom(bytes) }
  }

  private suspend fun getEventGroupsFromJsonFormat(
    eventGroupsBlobUri: BlobUri,
    eventGroupSyncConfig: EventGroupSyncConfig,
  ): Flow<EventGroup> = flow {
    val storageClient =
      SelectedStorageClient(
        blobUri = eventGroupsBlobUri,
        rootDirectory =
          if (eventGroupSyncConfig.eventGroupStorage.hasFileSystem())
            File(checkNotNull(fileSystemStorageRoot))
          else null,
        projectId = eventGroupSyncConfig.eventGroupStorage.gcs.projectId,
      )

    val blob =
      storageClient.getBlob(eventGroupsBlobUri.key)
        ?: throw IllegalStateException("Blob not found for key: ${eventGroupsBlobUri.key}")
    val parser = JsonFormat.parser()
    val eventGroups: EventGroups =
      EventGroups.newBuilder()
        .apply {
          val bytes = blob.read().flatten()
          parser.merge(bytes.toStringUtf8(), this)
        }
        .build()
    val eventGroupsList = eventGroups.eventGroupsList
    require(eventGroupsList.size > 0) {
      "Uploads of zero event groups are not supported. Or unable to parse."
    }
    emitAll(eventGroupsList.asFlow())
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val ATTR_BLOB_URI = AttributeKey.stringKey("blob_uri")
    private val ATTR_DATA_PROVIDER_NAME = AttributeKey.stringKey("data_provider_name")

    private const val SPAN_LOAD_EVENT_GROUPS = "load_event_groups"
    private const val SPAN_WRITE_EVENT_GROUP_MAP = "write_event_group_map"
    private const val SPAN_EVENT_GROUP_SYNC = "event_group_sync"
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
    private const val PROTO_FILE_SUFFIX = ".binpb"
    private const val JSON_FILE_SUFFIX = ".json"

    init {
      EdpaTelemetry.ensureInitialized()
    }

    /**
     * Creates an instrumented EventGroupsCoroutineStub for the Kingdom Public API.
     *
     * The stub is configured with:
     * - Mutual TLS authentication (certificates loaded from eventGroupSyncConfig)
     * - Graceful shutdown timeout
     * - OpenTelemetry gRPC instrumentation for automatic span and metric creation
     *
     * @param target The Kingdom API target (e.g., "kingdom.example.com:443")
     * @param eventGroupSyncConfig Configuration containing certificate file paths for mutual TLS
     * @param certHost The expected certificate hostname (optional, for cert verification)
     * @param shutdownTimeout Duration to wait for graceful channel shutdown
     * @return Instrumented EventGroupsCoroutineStub ready for use
     */
    private fun createEventGroupsStub(
      target: String,
      eventGroupSyncConfig: EventGroupSyncConfig,
      certHost: String?,
      shutdownTimeout: Duration,
    ): EventGroupsCoroutineStub {
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile = checkNotNull(File(eventGroupSyncConfig.cmmsConnection.certFilePath)),
          privateKeyFile =
            checkNotNull(File(eventGroupSyncConfig.cmmsConnection.privateKeyFilePath)),
          trustedCertCollectionFile =
            checkNotNull(File(eventGroupSyncConfig.cmmsConnection.certCollectionFilePath)),
        )

      val publicChannel =
        buildMutualTlsChannel(target, signingCerts, certHost).withShutdownTimeout(shutdownTimeout)

      // Use official OpenTelemetry gRPC instrumentation for automatic span and metric creation
      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val instrumentedChannel =
        ClientInterceptors.intercept(publicChannel, grpcTelemetry.newClientInterceptor())

      return EventGroupsCoroutineStub(instrumentedChannel)
    }
  }
}
