/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.securecomputation

import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Paths
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig.TriggeredApp
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfigKt.vidLabelingConfig
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.teeAppConfig
import org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.compiledNode
import org.wfanet.virtualpeople.common.copy
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.labelerOutput

@RunWith(JUnit4::class)
class ControlPlaneIntegrationTest() {

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private lateinit var workItemsService: GooglePubSubWorkItemsService
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  private val projectId = "test-project-id"
  private val topicId = "test-topic-id"
  private val subscriptionId = "test-subscription-id"

  @Before
  fun setup() {
    googlePubSubClient =
      GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port,
      )
    workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
    runBlocking {
      googlePubSubClient.createTopic(projectId, topicId)
      googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)
    }
  }

  @After
  fun cleanPubSubResources() {
    runBlocking {
      googlePubSubClient.deleteTopic(projectId, topicId)
      googlePubSubClient.deleteSubscription(projectId, subscriptionId)
    }
  }

  @Test
  fun vidLabelWithTextProtoModel() {
    runBlocking {
      val queueSubscriber = Subscriber(projectId, googlePubSubClient)
      val inMemoryStorageClient = InMemoryStorageClient()
      val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)
      val vidLabelerApp =
        VidLabelerApp(subscribingStorageClient, subscriptionId, queueSubscriber, Any.parser())
      val job = launch { vidLabelerApp.run() }
      val inputEventsPath = "gs://fake-bucket/input-events"
      val outputEventsPath = "gs://fake-bucket/output-events"
      val vidModelPath = "gs://fake-bucket/vid-model"
      val modelFileName = "single_id_model.textproto"
      val vidModelData =
        parseTextProto(
          Paths.get("$TEXTPROTO_PATH/$modelFileName").toFile().bufferedReader(),
          compiledNode {}
        )
      subscribingStorageClient.writeBlob(
        vidModelPath,
        flowOf(vidModelData.toString().toByteStringUtf8())
      )

      val dataWatcherConfig1 = dataWatcherConfig {
        sourcePathRegex = "^gs://fake-bucket/input-events"
        this.controlPlaneConfig = controlPlaneConfig {
          queueName = topicId
          appConfig = Any.pack(
            teeAppConfig {
              this.vidLabelingConfig = vidLabelingConfig {
                this.inputBasePath = inputEventsPath
                this.outputBasePath = outputEventsPath
                this.modelBlobTextProtoPath = vidModelPath
              }
            }
          )
        }
      }

      val dataWatcher = DataWatcher(workItemsService, listOf(dataWatcherConfig1))
      subscribingStorageClient.subscribe(dataWatcher)

      val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)
      val inputEvents =
        (1 until 19).map { index ->
          val indexString = "%02d".format(index)
          val inputFileName = "labeler_input_$indexString.textproto"
          parseTextProto(
            Paths.get("$TEXTPROTO_PATH/$inputFileName").toFile().bufferedReader(),
            labelerInput {}
          )
        }
      mesosRecordIoStorageClient.writeBlob(
        inputEventsPath,
        inputEvents.map { it.toString().toByteStringUtf8() }.asFlow()
      )

      withTimeout(1000) {
        while (true) {
          val blob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
          if (blob != null) {
            val labelerOutputData =
              blob
                .read()
                .map { byteString ->
                  val labelerOutput =
                    LabelerOutput.getDefaultInstance()
                      .newBuilderForType()
                      .apply {
                        TextFormat.Parser.newBuilder()
                          .build()
                          .merge(byteString.toStringUtf8(), this)
                      }
                      .build() as LabelerOutput
                  labelerOutput.copy { clearSerializedDebugTrace() }
                }
                .toList()
            // Parse and validate labeler output
            for (output in labelerOutputData) {
              val outputFileName = "single_id_labeler_output.textproto"
              val expectedOutput =
                parseTextProto(
                  Paths.get("$TEXTPROTO_PATH/$outputFileName").toFile().bufferedReader(),
                  labelerOutput {}
                )
              ProtoTruth.assertThat(output).isEqualTo(expectedOutput)
            }
            break
          }
          delay(100)
        }
      }
      job.cancelAndJoin()
    }
  }

  companion object {
    private val TEXTPROTO_PATH =
      getRuntimePath(Paths.get("virtual-people-core-serving~/src/main/resources/labeler"))
  }
}
