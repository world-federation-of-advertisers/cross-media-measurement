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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.kotlin.unpack
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient
import com.google.cloud.functions.CloudEventsFunction
import com.google.events.cloud.storage.v1.StorageObjectData
import com.google.protobuf.TextFormat
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import io.cloudevents.CloudEvent
import java.net.URI
import java.util.logging.Logger
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSyncConfig
import org.wfanet.measurement.edpaggregator.eventgroups.toEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.Campaigns
import org.wfanet.measurement.storage.SelectedStorageClient

@RunWith(JUnit4::class)
class EventGroupSyncFunctionTest() {
  private lateinit var storageClient: GcsStorageClient
  private lateinit var grpcServer: CommonServer

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<CreateEventGroupRequest>(0).eventGroup }
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

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Before
  fun startInfra() {
    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
        verboseGrpcLogging = true,
        certs = serverCerts,
        clientAuth = ClientAuth.REQUIRE,
        nameForLogging = "WorkItemsServer",
        services = listOf(eventGroupsServiceMock.bindService()),
      )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")
    System.setProperty("KINGDOM_TARGET", "localhost:${grpcServer.port}")
    System.setProperty("KINGDOM_CERT_HOST", "localhost")
    System.setProperty("CERT_FILE_PATH", SECRETS_DIR.resolve("edp1_tls.pem").toString())
    System.setProperty("PRIVATE_KEY_FILE_PATH", SECRETS_DIR.resolve("edp1_tls.key").toString())
    System.setProperty(
      "CERT_COLLECTION_FILE_PATH",
      SECRETS_DIR.resolve("kingdom_root.pem").toString(),
    )
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
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, testCampaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
    runBlocking { eventGroupSync.sync() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun sync_returnsMapOfEventGroupReferenceIdsToEventGroups() {
    runBlocking {
      val eventGroupSync = EventGroupSync("edp-name", eventGroupsStub, campaigns)
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result)
        .isEqualTo(
          mapOf(
            "reference-id-1" to "resource-name-for-reference-id-1",
            "reference-id-2" to "resource-name-for-reference-id-2",
            "reference-id-3" to "resource-name-for-reference-id-3",
          )
        )
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
