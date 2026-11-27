/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.sun.net.httpserver.HttpServer
import io.cloudevents.core.builder.CloudEventBuilder
import java.net.InetSocketAddress
import java.net.URI
import java.security.Permission
import java.security.Policy
import java.security.ProtectionDomain
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs
import org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction

@RunWith(JUnit4::class)
class ReplayStorageCleanupEventTest {
  @get:Rule val temporaryFolder = TemporaryFolder()
  private val requests = ConcurrentLinkedQueue<Pair<String, String>>()
  private val receivedObjects = ConcurrentLinkedQueue<Pair<String, Map<String, String>>>()
  private val receivedHeaders = ConcurrentLinkedQueue<Map<String, String>>()
  private lateinit var server: HttpServer
  private var responseStatus = 200
  private val watcher = DataWatcherFunction { path, metadata ->
    receivedObjects.add(path to metadata)
  }

  @Before
  fun startServer() {
    server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/") { exchange ->
      val body = exchange.requestBody.readAllBytes()
      requests.add(exchange.requestHeaders.getFirst("ce-type") to body.toString(Charsets.UTF_8))
      receivedHeaders.add(
        listOf("Authorization", "ce-id", "ce-source", "ce-time", "ce-specversion")
          .mapNotNull { key -> exchange.requestHeaders.getFirst(key)?.let { key to it } }
          .toMap()
      )
      if (responseStatus == 200) {
        watcher.accept(
          CloudEventBuilder.v1()
            .withId(exchange.requestHeaders.getFirst("ce-id"))
            .withSource(URI(exchange.requestHeaders.getFirst("ce-source")))
            .withType(exchange.requestHeaders.getFirst("ce-type"))
            .withData("application/json", body)
            .build()
        )
      }
      exchange.sendResponseHeaders(responseStatus, -1)
      exchange.close()
    }
    server.start()
  }

  @After
  fun stopServer() {
    server.stop(0)
  }

  @Test
  fun `main replays native delete and archive notifications without a generation`() {
    for ((eventType, cloudEventType) in
      listOf(
        "OBJECT_DELETE" to "google.cloud.storage.object.v1.deleted",
        "OBJECT_ARCHIVE" to "google.cloud.storage.object.v1.archived",
      )) {
      val output = replay(export(eventType = eventType))
      CommandLineTesting.assertThat(output).status().isEqualTo(0)
      assertThat(requests.last()).isEqualTo(cloudEventType to OBJECT_DATA)
      val (path, metadata) = receivedObjects.last()
      assertThat(path).isEqualTo("gs://test-bucket/path/metadata.binpb")
      assertThat(metadata[WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY])
        .isEqualTo(RESOURCE_NAME)
      assertThat(metadata[DataWatcher.EVENT_TYPE_METADATA_KEY]).isEqualTo(cloudEventType)
      assertThat(metadata[DataWatcher.GENERATION_METADATA_KEY]).isEqualTo("0")
      assertThat(receivedHeaders.last()["Authorization"]).isEqualTo("Bearer test-token")
    }
  }

  @Test
  fun `main retries the same saved event after a failed cleanup`() {
    val savedEvent = export(eventType = "OBJECT_DELETE")
    responseStatus = 500
    val failure = replay(savedEvent)
    CommandLineTesting.assertThat(failure).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(failure).err().contains("leave the queue message unacknowledged")
    CommandLineTesting.assertThat(failure).err().doesNotContain("test-token")
    assertThat(receivedObjects).isEmpty()
    responseStatus = 200

    val success = replay(savedEvent)

    CommandLineTesting.assertThat(success).status().isEqualTo(0)
    assertThat(requests.toList())
      .containsExactly(
        "google.cloud.storage.object.v1.deleted" to OBJECT_DATA,
        "google.cloud.storage.object.v1.deleted" to OBJECT_DATA,
      )
    assertThat(receivedObjects).hasSize(1)
  }

  @Test
  fun `main preserves an exported CloudEvent`() {
    val attributes =
      """{"ce-specversion":"1.0","ce-id":"original-event","ce-source":"//storage.googleapis.com/projects/_/buckets/test-bucket","ce-type":"google.cloud.storage.object.v1.archived","ce-time":"2026-01-01T00:00:00Z"}"""
    val output = replay(exportMessage(attributes, OBJECT_DATA))
    CommandLineTesting.assertThat(output).status().isEqualTo(0)
    assertThat(requests.single())
      .isEqualTo("google.cloud.storage.object.v1.archived" to OBJECT_DATA)
    assertThat(receivedHeaders.single()["ce-id"]).isEqualTo("original-event")
    assertThat(receivedHeaders.single()["ce-time"]).isEqualTo("2026-01-01T00:00:00Z")
    assertThat(receivedHeaders.single()["ce-source"])
      .isEqualTo("//storage.googleapis.com/projects/_/buckets/test-bucket")
  }

  @Test
  fun `main preserves generation and handles an event without a metadata resource ID`() {
    val body =
      """{"bucket":"test-bucket","name":"path/metadata.binpb","size":"123","generation":"42"}"""
    val output = replay(export(eventType = "OBJECT_DELETE", body = body))
    CommandLineTesting.assertThat(output).status().isEqualTo(0)
    val (path, metadata) = receivedObjects.single()
    assertThat(path).isEqualTo("gs://test-bucket/path/metadata.binpb")
    assertThat(metadata[DataWatcher.GENERATION_METADATA_KEY]).isEqualTo("42")
    assertThat(metadata).doesNotContainKey(WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY)
  }

  @Test
  fun `main rejects invalid exports before contacting the watcher`() {
    for (invalid in
      listOf(
        "[]",
        "[${export(eventType = "OBJECT_DELETE").removeSurrounding("[", "]")},${export(eventType = "OBJECT_DELETE").removeSurrounding("[", "]")}]",
        export(eventType = "OBJECT_FINALIZE"),
        export(eventType = "OBJECT_DELETE")
          .replace("\"bucketId\":\"test-bucket\"", "\"bucketId\":\"different-bucket\""),
        export(
          eventType = "OBJECT_DELETE",
          body = """{"bucket":"test-bucket","name":"path/metadata.binpb","size":"0"}""",
        ),
        export(
          eventType = "OBJECT_DELETE",
          body = """{"bucket":"test-bucket","name":"path/metadata.binpb"}""",
        ),
        export(eventType = "OBJECT_DELETE").replace("\"data\":\"", "\"data\":\"!"),
      )) {
      CommandLineTesting.assertThat(replay(invalid)).status().isNotEqualTo(0)
    }
    assertThat(requests).isEmpty()
  }

  @Test
  fun `main rejects redirects without forwarding the identity token`() {
    responseStatus = 302
    val output = replay(export(eventType = "OBJECT_DELETE"))
    CommandLineTesting.assertThat(output).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(output).err().contains("HTTP 302")
    assertThat(requests).hasSize(1)
  }

  private fun replay(json: String): CommandLineTesting.CapturedOutput {
    val message = temporaryFolder.newFile().apply { writeText(json) }
    val token = temporaryFolder.newFile().apply { writeText("test-token\n") }
    return CommandLineTesting.capturingOutput(
      arrayOf(
        "--message-file=${message.path}",
        "--identity-token-file=${token.path}",
        "--watcher-url=http://127.0.0.1:${server.address.port}/",
      ),
      ::main,
    )
  }

  private fun export(eventType: String): String = export(eventType, OBJECT_DATA)

  private fun export(eventType: String, body: String): String =
    exportMessage(
      """{"eventType":"$eventType","payloadFormat":"JSON_API_V1","bucketId":"test-bucket","objectId":"path/metadata.binpb","CloudPubSubDeadLetterSourceSubscription":"projects/test/subscriptions/original"}""",
      body,
    )

  private fun exportMessage(eventAttributes: String, body: String): String {
    return """[{"ackId":"saved-ack","message":{"messageId":"message-1","attributes":$eventAttributes,"data":"${Base64.getEncoder().encodeToString(body.toByteArray())}"}}]"""
  }

  companion object {
    init {
      System.setProperty("otel.metrics.exporter", "none")
      System.setProperty("otel.traces.exporter", "none")
      System.setProperty("otel.logs.exporter", "none")
      // HttpClient checks the access control context directly while the exit-intercepting
      // security manager is installed. Allow the test's local HTTP requests.
      Policy.setPolicy(
        object : Policy() {
          override fun implies(domain: ProtectionDomain?, permission: Permission?): Boolean = true
        }
      )
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    private const val RESOURCE_NAME = "dataProviders/test/impressionMetadata/test"
    private val OBJECT_DATA =
      """{"bucket":"test-bucket","name":"path/metadata.binpb","size":"123","metadata":{"${WatchedBlobs.IMPRESSION_METADATA_RESOURCE_ID_KEY}":"$RESOURCE_NAME"}}"""
  }
}
