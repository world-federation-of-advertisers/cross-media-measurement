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
import kotlin.io.path.Path
import kotlinx.coroutines.flow.flowOf
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
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as eventGroupCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.edpaggregator.eventgroups.eventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.CampaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupMap
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.campaigns as protoCampaigns
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroupMap
import org.wfanet.measurement.gcloud.testing.CloudFunctionProcess
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class EventGroupSyncFunctionTest() {
  private lateinit var grpcServer: CommonServer
  private lateinit var functionProcess: CloudFunctionProcess
  private val functionBinaryPath =
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
  private val gcfTarget =
    "org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups.EventGroupSyncFunction"

  private val campaigns =
    listOf(
      campaignMetadata {
        eventGroupReferenceId = "reference-id-1"
        campaignName = "campaign-1"
        measurementConsumerName = "measurement-consumer-1"
        brandName = "brand-1"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("VIDEO", "DISPLAY")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-2"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("OTHER")
      },
      campaignMetadata {
        eventGroupReferenceId = "reference-id-3"
        campaignName = "campaign-2"
        measurementConsumerName = "measurement-consumer-2"
        brandName = "brand-2"
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
        mediaTypes += listOf("OTHER")
      },
    )

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation ->
        val eventGroup = invocation.getArgument<CreateEventGroupRequest>(0).eventGroup
        eventGroup.copy { name = "resource-name-for-${eventGroup.eventGroupReferenceId}" }
      }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups += campaigns[0].toEventGroup()
          eventGroups += campaigns[1].toEventGroup()
          eventGroups +=
            campaigns[2].toEventGroup().copy {
              this.eventGroupMetadata = eventGroupMetadata {
                this.adMetadata = adMetadata {
                  this.campaignMetadata = eventGroupCampaignMetadata {
                    brandName = "new-brand-name"
                  }
                }
              }
            }
        }
      }
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @Rule @JvmField val tempFolder = TemporaryFolder()

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
      CloudFunctionProcess(javaBinaryPath = functionBinaryPath, classTarget = gcfTarget)
    logger.info("Started gRPC server on port ${grpcServer.port}")
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  @Test
  fun `sync registersUnregisteredEventGroups`() {
    val newCampaign = campaignMetadata {
      eventGroupReferenceId = "reference-id-4"
      campaignName = "campaign-2"
      measurementConsumerName = "measurement-consumer-2"
      brandName = "brand-2"
      startTime = timestamp { seconds = 200 }
      endTime = timestamp { seconds = 300 }
      mediaTypes += listOf("OTHER")
    }
    val testCampaigns = campaigns + newCampaign
    val config = eventGroupSyncConfig {
      campaignsBlobUri = "file:///some/path/campaigns-blob-uri"
      eventGroupMapUri = "file:///some/other/path/event-groups-map-uri"
    }
    File("${tempFolder.root}/some/path").mkdirs()
    File("${tempFolder.root}/some/other/path").mkdirs()
    val port = runBlocking {
      functionProcess.start(
        mapOf(
          "FILE_STORAGE_ROOT" to tempFolder.root.toString(),
          "GCS_PROJECT_ID" to "some-project-id",
          "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "KINGDOM_SHUTDOWN_DURATION_SECONDS" to "3",
          "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
          "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
          "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
        )
      )
    }

    val url = "http://localhost:$port"
    logger.info("Testing Cloud Function at: $url")

    val storageClient = FileSystemStorageClient(File(tempFolder.root.toString()))

    runBlocking {
      storageClient.writeBlob(
        "some/path/campaigns-blob-uri",
        flowOf(protoCampaigns { campaigns += testCampaigns }.toByteString()),
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

    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
    val mappedData = runBlocking {
      EventGroupMap.parseFrom(
        storageClient.getBlob("some/other/path/event-groups-map-uri")!!.read().flatten()
      )
    }
    assertThat(mappedData)
      .isEqualTo(
        eventGroupMap {
          this.eventGroupMap.putAll(
            mapOf(
              "reference-id-1" to "resource-name-for-reference-id-1",
              "reference-id-2" to "resource-name-for-reference-id-2",
              "reference-id-3" to "resource-name-for-reference-id-3",
              "reference-id-4" to "resource-name-for-reference-id-4",
            )
          )
        }
      )
  }

  companion object {
    private const val BUCKET = "test-bucket"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp1_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun CampaignMetadata.toEventGroup(): EventGroup {
  val campaign = this
  return eventGroup {
    name = "resource-name-for-${campaign.eventGroupReferenceId}"
    measurementConsumer = campaign.measurementConsumerName
    eventGroupReferenceId = campaign.eventGroupReferenceId
    this.eventGroupMetadata = eventGroupMetadata {
      this.adMetadata = adMetadata {
        this.campaignMetadata = eventGroupCampaignMetadata {
          brandName = campaign.brandName
          campaignName = campaign.campaignName
        }
      }
    }
    mediaTypes += campaign.mediaTypesList.map { MediaType.valueOf(it) }
    dataAvailabilityInterval = interval {
      startTime = campaign.startTime
      endTime = campaign.endTime
    }
  }
}
