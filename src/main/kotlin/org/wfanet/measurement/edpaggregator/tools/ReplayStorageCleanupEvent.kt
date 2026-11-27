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

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Base64
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/** Replays one exported Pub/Sub storage notification through the deletion watcher. */
@Command(
  name = "ReplayStorageCleanupEvent",
  description = ["Retries one cleanup event without acknowledging its queue message"],
  mixinStandardHelpOptions = true,
)
class ReplayStorageCleanupEvent : Runnable {
  @Option(
    names = ["--message-file"],
    required = true,
    description = ["JSON output from gcloud pubsub subscriptions pull --limit=1 --format=json"],
  )
  lateinit var messageFile: File

  @Option(
    names = ["--watcher-url"],
    required = true,
    description = ["HTTPS URL of the deletion watcher for this queue"],
  )
  lateinit var watcherUrl: URI

  @Option(
    names = ["--identity-token-file"],
    required = true,
    description = ["File containing an identity token for the deletion watcher"],
  )
  lateinit var identityTokenFile: File

  override fun run() {
    require(
      watcherUrl.scheme == "https" ||
        (watcherUrl.scheme == "http" && watcherUrl.host in setOf("localhost", "127.0.0.1", "[::1]"))
    ) {
      "--watcher-url must use HTTPS (HTTP is allowed only on loopback)"
    }
    require(watcherUrl.host != null && watcherUrl.userInfo == null && watcherUrl.fragment == null) {
      "--watcher-url must be an absolute URL without credentials or a fragment"
    }
    val token = identityTokenFile.readText().trim()
    require(token.isNotEmpty()) { "Identity token file is empty" }
    require(token.none { it.isWhitespace() }) { "Identity token file must contain a single token" }
    val export = JsonParser.parseString(messageFile.readText())
    require(export.isJsonArray && export.asJsonArray.size() == 1) {
      "Message file must contain exactly one message; pull with --limit=1 without --auto-ack"
    }
    val message = export.asJsonArray[0].asJsonObject.getAsJsonObject("message")
    requireNotNull(message) { "Message file has no Pub/Sub message" }
    val attributes = message.getAsJsonObject("attributes") ?: JsonObject()
    val body = Base64.getDecoder().decode(message.requiredString("data"))
    val objectData = JsonParser.parseString(body.toString(Charsets.UTF_8)).asJsonObject
    val bucket = objectData.requiredString("bucket")
    val objectName = objectData.requiredString("name")
    val size = objectData.requiredString("size").toLongOrNull()
    require(size != null && size >= 0) { "Storage notification has no valid object size" }
    require(size != 0L || objectName.endsWith("done", ignoreCase = true)) {
      "The deletion watcher skips empty objects; this event cannot be replayed as cleanup"
    }

    val headers: Map<String, String> =
      if (attributes.has("ce-type")) {
        require(attributes.requiredString("ce-type") in CLEANUP_EVENT_TYPES) {
          "Only storage deletion and archive events can be replayed"
        }
        mapOf(
          "ce-specversion" to attributes.requiredString("ce-specversion"),
          "ce-id" to attributes.requiredString("ce-id"),
          "ce-source" to attributes.requiredString("ce-source"),
          "ce-type" to attributes.requiredString("ce-type"),
        ) +
          attributes
            .entrySet()
            .filter { it.key in OPTIONAL_EVENT_HEADERS }
            .associate { it.key to it.value.asString }
      } else {
        val type =
          when (attributes.requiredString("eventType")) {
            "OBJECT_DELETE" -> "google.cloud.storage.object.v1.deleted"
            "OBJECT_ARCHIVE" -> "google.cloud.storage.object.v1.archived"
            else -> error("Only storage deletion and archive events can be replayed")
          }
        require(attributes.requiredString("payloadFormat") == "JSON_API_V1") {
          "Storage notification must use JSON_API_V1"
        }
        require(
          attributes.requiredString("bucketId") == bucket &&
            attributes.requiredString("objectId") == objectName
        ) {
          "Storage notification attributes do not match its object data"
        }
        mapOf(
          "ce-specversion" to "1.0",
          "ce-id" to message.requiredString("messageId"),
          "ce-source" to "//storage.googleapis.com/projects/_/buckets/$bucket",
          "ce-type" to type,
        ) +
          if (attributes.has("eventTime"))
            mapOf("ce-time" to attributes.requiredString("eventTime"))
          else emptyMap()
      }
    val request =
      HttpRequest.newBuilder(watcherUrl)
        .timeout(Duration.ofMinutes(2))
        .header("Authorization", "Bearer $token")
        .header("Content-Type", "application/json")
        .apply { headers.forEach { (key, value) -> header(key, value) } }
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build()
    val client =
      HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build()
    val response = client.send(request, HttpResponse.BodyHandlers.discarding())
    check(response.statusCode() == 200) {
      "Deletion watcher returned HTTP ${response.statusCode()}; leave the queue message unacknowledged and check the watcher and cleanup logs"
    }
    println("Cleanup event processed. You can now acknowledge the saved queue message.")
  }

  private fun JsonObject.requiredString(field: String): String =
    get(field)?.asString?.takeIf { it.isNotEmpty() }
      ?: error("Storage notification is missing $field")

  companion object {
    private val CLEANUP_EVENT_TYPES =
      setOf("google.cloud.storage.object.v1.deleted", "google.cloud.storage.object.v1.archived")
    private val OPTIONAL_EVENT_HEADERS =
      setOf("ce-time", "ce-subject", "ce-traceparent", "ce-tracestate")
  }
}

fun main(args: Array<String>) = commandLineMain(ReplayStorageCleanupEvent(), args)
