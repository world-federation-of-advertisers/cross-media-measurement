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

import com.google.protobuf.ByteString
import java.io.BufferedInputStream
import java.net.URL
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.LabelerOutput
import com.google.protobuf.TextFormat
import java.io.File
import org.wfanet.measurement.common.parseTextProto

@RunWith(JUnit4::class)
class VidLabelerAppTest() {

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
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }

  @After
  fun clear() {
    runBlocking {
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }

  @Test
  fun vidLabel() {
    runBlocking {
      googlePubSubClient.createTopic(projectId, topicId)
      googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)
      val queueSubscriber = Subscriber(projectId, googlePubSubClient)
      val inMemoryStorageClient = InMemoryStorageClient()
      val vidLabelerApp =
        VidLabelerApp(inMemoryStorageClient, subscriptionId, queueSubscriber, Any.parser())
      val job = launch { vidLabelerApp.run() }
      val inputEventsPath = "input-events"
      val outputEventsPath = "output-events"
      val vidModelPath = "vid-model"

      val modelFileName = "single_id_model.textproto"
      val vidModelData =
        parseTextProto(File("$TEXTPROTO_PATH/$modelFileName").bufferedReader(), compiledNode {})
      inMemoryStorageClient.writeBlob(
        flowOf(vidModelData)
      )

      val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)
      val rawEvents = (1 until 19).map { index ->
        val indexString = "%02d".format(index)
        val inputFileName = "labeler_input_$indexString.textproto"
        parseTextProto(File("$TEXTPROTO_PATH/$inputFileName").bufferedReader(), compiledNode {})
      }
      mesosRecordIoStorageClient.writeBlob(inputEventsPath, rawEvents)

      withTimeout(10000) {
        while (true) {
          val blob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
          if (blob != null) {
            val labelerOutputData = blob.read().map { byteString ->
              LabelerOutput.getDefaultInstance()
                .newBuilderForType()
                .apply {
                  TextFormat.Parser.newBuilder().build().merge(byteString.toStringUtf8(), this)
                }
                .build() as LabelerOutput
            }.toList()
            // Parse and validate labeler output
            for (output in labelerOutputData) {
              val outputFileName = "single_id_labeler_output.textproto"
              val expectedOutput = parseTextProto(File("$TEXTPROTO_PATH/$outputFileName").bufferedReader(), labelerOutput {})
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
    private const val TEXTPROTO_PATH = "src/main/resources/labeler"
  }
}
