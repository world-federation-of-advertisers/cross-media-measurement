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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as cmmsCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as cmmsAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.eventGroupSyncConfig
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class EventGroupSyncFunctionTest() {
  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { deleteEventGroup(any<DeleteEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<DeleteEventGroupRequest>(0) }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation ->
        val eventGroup = invocation.getArgument<CreateEventGroupRequest>(0).eventGroup
        eventGroup.copy { name = "resource-name-for-${eventGroup.eventGroupReferenceId}" }
      }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups +=
            listOf(
              cmmsEventGroup {
                name = "dataProviders/data-provider-1/eventGroups/reference-id-1"
                measurementConsumer = "measurementConsumers/measurement-consumer-1"
                eventGroupReferenceId = "reference-id-1"
                mediaTypes += listOf(CmmsMediaType.VIDEO, CmmsMediaType.DISPLAY)
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-1"
                      campaignName = "campaign-1"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-2/eventGroups/reference-id-2"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-2"
                mediaTypes += listOf(CmmsMediaType.OTHER)
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-2"
                      campaignName = "campaign-2"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-3/eventGroups/reference-id-3"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-3"
                mediaTypes += listOf(CmmsMediaType.OTHER)
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "new-brand-name"
                      campaignName = "campaign-3"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
            )
        }
      }
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @get:Rule val tempFolder = TemporaryFolder()

  @Before
  fun startInfra() {
    /** Start gRPC server with mock EventGroups service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "EventGroupsServer",
          services = listOf(eventGroupsServiceMock.bindService()),
        )
        .start()
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FUNCTION_BINARY_PATH,
        classTarget = GCG_TARGET,
      )
    logger.info("Started gRPC server on port ${grpcServer.port}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  private suspend fun startFunction(envOverrides: Map<String, String> = emptyMap()): Int {
    val defaultEnv =
      mapOf(
        "FILE_STORAGE_ROOT" to tempFolder.root.toString(),
        "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
        "KINGDOM_CERT_HOST" to "localhost",
        "KINGDOM_SHUTDOWN_DURATION_SECONDS" to "3",
        "OTEL_METRICS_EXPORTER" to "logging",
        "OTEL_TRACES_EXPORTER" to "logging",
        "OTEL_LOGS_EXPORTER" to "logging",
      )
    return functionProcess.start(defaultEnv + envOverrides)
  }

  @Test
  fun `sync registersUnregisteredEventGroups`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.OTHER)
    }
    val testCampaigns = CAMPAIGNS + newCampaign
    val config = eventGroupSyncConfig {
      dataProvider = "some-data-provider"
      eventGroupsBlobUri = "file:///some/path/campaigns-blob-uri.binpb"
      eventGroupMapBlobUri = "file:///some/other/path/event-groups-map-uri"
      this.cmmsConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      eventGroupStorage = storageParams { fileSystem = fileSystemStorage {} }
      eventGroupMapStorage = storageParams { fileSystem = fileSystemStorage {} }
    }
    File("${tempFolder.root}/some/path").mkdirs()
    File("${tempFolder.root}/some/other/path").mkdirs()
    val port = runBlocking { startFunction() }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    runBlocking {
      MesosRecordIoStorageClient(storageClient)
        .writeBlob(
          "some/path/campaigns-blob-uri.binpb",
          testCampaigns.map { it.toByteString() }.asFlow(),
        )
    }

    // In practice, the DataWatcher makes this HTTP call
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    assertThat(getResponse.statusCode()).isEqualTo(200)

    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
    val mappedData = runBlocking {
      MesosRecordIoStorageClient(storageClient)
        .getBlob("some/other/path/event-groups-map-uri")!!
        .read()
        .map { MappedEventGroup.parseFrom(it) }
        .toList()
        .map { it.eventGroupReferenceId to it.eventGroupResource }
    }
    assertThat(mappedData)
      .isEqualTo(
        listOf(
          "reference-id-1" to "dataProviders/data-provider-1/eventGroups/reference-id-1",
          "reference-id-2" to "dataProviders/data-provider-2/eventGroups/reference-id-2",
          "reference-id-3" to "dataProviders/data-provider-3/eventGroups/reference-id-3",
          "reference-id-4" to "resource-name-for-reference-id-4",
        )
      )
  }

  @Test
  fun `sync registersUnregisteredEventGroups using JSON format`() {
    val newCampaign =
      """
        {
          "eventGroups": [
            {
              "eventGroupReferenceId": "reference-id-4",
              "eventGroupMetadata": {
                "adMetadata": {
                  "campaignMetadata": {
                    "brand": "brand-2",
                    "campaign": "campaign-2"
                  }
                }
              },
              "dataAvailabilityInterval": {
                "startTime": "1970-01-01T00:03:20Z",
                "endTime": "1970-01-01T00:05:00Z"
              },
              "measurementConsumer": "measurement-consumer-2",
              "mediaTypes": ["OTHER"]
            },
            {
              "eventGroupReferenceId": "reference-id-5",
              "eventGroupMetadata": {
                "adMetadata": {
                  "campaignMetadata": {
                    "brand": "brand-2",
                    "campaign": "campaign-3"
                  }
                }
              },
              "dataAvailabilityInterval": {
                "startTime": "1970-01-01T00:03:20Z",
                "endTime": "1970-01-01T00:05:00Z"
              },
              "measurementConsumer": "measurement-consumer-2",
              "mediaTypes": ["OTHER"]
            }
           ]
          }
    """
        .trimIndent()

    val config = eventGroupSyncConfig {
      dataProvider = "some-data-provider"
      eventGroupsBlobUri = "file:///some/path/campaigns-blob-uri.json"
      eventGroupMapBlobUri = "file:///some/other/path/event-groups-map-uri"
      this.cmmsConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      eventGroupStorage = storageParams { fileSystem = fileSystemStorage {} }
      eventGroupMapStorage = storageParams { fileSystem = fileSystemStorage {} }
    }
    File("${tempFolder.root}/some/path").mkdirs()
    File("${tempFolder.root}/some/other/path").mkdirs()
    val port = runBlocking { startFunction() }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    runBlocking {
      storageClient.writeBlob(
        "some/path/campaigns-blob-uri.json",
        flowOf(ByteString.copyFromUtf8(newCampaign)),
      )
    }

    // In practice, the DataWatcher makes this HTTP call
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    assertThat(getResponse.statusCode()).isEqualTo(200)
    verifyBlocking(eventGroupsServiceMock, times(2)) { createEventGroup(any()) }
    val mappedData = runBlocking {
      MesosRecordIoStorageClient(storageClient)
        .getBlob("some/other/path/event-groups-map-uri")!!
        .read()
        .map { MappedEventGroup.parseFrom(it) }
        .toList()
        .map { it.eventGroupReferenceId to it.eventGroupResource }
    }
    assertThat(mappedData)
      .isEqualTo(
        listOf(
          "reference-id-4" to "resource-name-for-reference-id-4",
          "reference-id-5" to "resource-name-for-reference-id-5",
        )
      )
  }

  @Test
  fun `sync registersUnregisteredEventGroups using JSON format throws for invalid json`() {
    val newCampaign =
      """
        {
          "events": [
            {
              "eventGroupReferenceId": "reference-id-4",
              "eventGroupMetadata": {
                "adMetadata": {
                  "campaignMetadata": {
                    "brand": "brand-2",
                    "campaign": "campaign-2"
                  }
                }
              },
              "dataAvailabilityInterval": {
                "startTime": "1970-01-01T00:03:20Z",
                "endTime": "1970-01-01T00:05:00Z"
              },
              "measurementConsumer": "measurement-consumer-2",
              "mediaTypes": ["OTHER"]
            },
            {
              "eventGroupReferenceId": "reference-id-5",
              "eventGroupMetadata": {
                "adMetadata": {
                  "campaignMetadata": {
                    "brand": "brand-2",
                    "campaign": "campaign-3"
                  }
                }
              },
              "dataAvailabilityInterval": {
                "startTime": "1970-01-01T00:03:20Z",
                "endTime": "1970-01-01T00:05:00Z"
              },
              "measurementConsumer": "measurement-consumer-2",
              "mediaTypes": ["OTHER"]
            }
           ]
          }
    """
        .trimIndent()

    val config = eventGroupSyncConfig {
      dataProvider = "some-data-provider"
      eventGroupsBlobUri = "file:///some/path/campaigns-blob-uri.json"
      eventGroupMapBlobUri = "file:///some/other/path/event-groups-map-uri"
      this.cmmsConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      eventGroupStorage = storageParams { fileSystem = fileSystemStorage {} }
      eventGroupMapStorage = storageParams { fileSystem = fileSystemStorage {} }
    }
    File("${tempFolder.root}/some/path").mkdirs()
    File("${tempFolder.root}/some/other/path").mkdirs()
    val port = runBlocking { startFunction() }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    runBlocking {
      storageClient.writeBlob(
        "some/path/campaigns-blob-uri.json",
        flowOf(ByteString.copyFromUtf8(newCampaign)),
      )
    }

    // In practice, the DataWatcher makes this HTTP call
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("X-DataWatcher-Path", "")
        .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    assertThat(getResponse.statusCode()).isEqualTo(500)
    verifyBlocking(eventGroupsServiceMock, times(0)) { createEventGroup(any()) }
  }

  @Test
  fun `sync registersUnregisteredEventGroups with path from data watcher`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.OTHER)
    }
    val testCampaigns = CAMPAIGNS + newCampaign
    val config = eventGroupSyncConfig {
      dataProvider = "some-data-provider"
      eventGroupsBlobUri = "file:///some/path/that/does/not/exist"
      eventGroupMapBlobUri = "file:///some/other/path/event-groups-map-uri"
      this.cmmsConnection = transportLayerSecurityParams {
        certFilePath = SECRETS_DIR.resolve("edp7_tls.pem").toString()
        privateKeyFilePath = SECRETS_DIR.resolve("edp7_tls.key").toString()
        certCollectionFilePath = SECRETS_DIR.resolve("kingdom_root.pem").toString()
      }
      eventGroupStorage = storageParams { fileSystem = fileSystemStorage {} }
      eventGroupMapStorage = storageParams { fileSystem = fileSystemStorage {} }
    }
    File("${tempFolder.root}/some/path").mkdirs()
    File("${tempFolder.root}/some/other/path").mkdirs()
    val port = runBlocking { startFunction() }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    runBlocking {
      MesosRecordIoStorageClient(storageClient)
        .writeBlob(
          "some/path/campaigns-blob-uri.binpb",
          testCampaigns.map { it.toByteString() }.asFlow(),
        )
    }

    // In practice, the DataWatcher makes this HTTP call
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("X-DataWatcher-Path", "file:///some/path/campaigns-blob-uri.binpb")
        .POST(HttpRequest.BodyPublishers.ofString(config.toJson()))
        .build()
    val getResponse = client.send(getRequest, HttpResponse.BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")

    assertThat(getResponse.statusCode()).isEqualTo(200)

    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
    val mappedData = runBlocking {
      MesosRecordIoStorageClient(storageClient)
        .getBlob("some/other/path/event-groups-map-uri")!!
        .read()
        .map { MappedEventGroup.parseFrom(it) }
        .toList()
        .map { it.eventGroupReferenceId to it.eventGroupResource }
    }
    assertThat(mappedData)
      .isEqualTo(
        listOf(
          "reference-id-1" to "dataProviders/data-provider-1/eventGroups/reference-id-1",
          "reference-id-2" to "dataProviders/data-provider-2/eventGroups/reference-id-2",
          "reference-id-3" to "dataProviders/data-provider-3/eventGroups/reference-id-3",
          "reference-id-4" to "resource-name-for-reference-id-4",
        )
      )
  }

  companion object {
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val FUNCTION_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "deploy",
        "gcloud",
        "eventgroups",
        "testing",
        "InvokeEventGroupSyncFunction",
      )
    private const val GCG_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups.EventGroupSyncFunction"

    private val CAMPAIGNS =
      listOf(
        eventGroup {
          eventGroupReferenceId = "reference-id-1"
          measurementConsumer = "measurementConsumers/measurement-consumer-1"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-1"
                campaign = "campaign-1"
              }
            }
          }
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.VIDEO, MediaType.DISPLAY)
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-2"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-2"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.OTHER)
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-3"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-3"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.OTHER)
        },
      )
  }
}
