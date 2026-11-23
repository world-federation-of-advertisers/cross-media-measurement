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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.zip.GZIPInputStream
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.dataavailability.ExternalDataAvailabilitySnapshotChangedException

class GcsGenerationMatchedBlobMetadataUpdaterTest {
  @Test
  fun `update preserves metadata across disjoint completion writes`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        val updater = GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)

        updater.update(KEY, GENERATION, CREATE_TIME, mapOf("sync_id" to "sync-1"))
        updater.update(KEY, GENERATION, null, mapOf("synced_by" to "availability-sync"))
        updater.update(KEY, GENERATION, null, mapOf("published_sync_id" to "sync-1"))

        assertThat(endpoint.metadata)
          .containsExactly(
            "owner",
            "external-producer",
            "sync_id",
            "sync-1",
            "synced_by",
            "availability-sync",
            "published_sync_id",
            "sync-1",
          )
        assertThat(endpoint.customTime).isEqualTo(CREATE_TIME)
      }
    }

  @Test
  fun `update changes custom time without removing metadata`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)
          .update(KEY, GENERATION, CREATE_TIME, emptyMap())

        assertThat(endpoint.metadata)
          .containsExactly("owner", "external-producer", "sync_id", "old")
        assertThat(endpoint.customTime).isEqualTo(CREATE_TIME)
      }
    }

  @Test
  fun `update rejects an object overwritten after the exact generation read`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.beforeUpdate = { endpoint.overwrite() }

        assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
          GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)
            .update(KEY, GENERATION, CREATE_TIME, mapOf("synced_by" to "availability-sync"))
        }

        assertThat(endpoint.metadata).containsExactly("owner", "new-producer")
        assertThat(endpoint.customTime).isNull()
      }
    }

  @Test
  fun `update preserves metadata changed after the exact generation read`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.beforeUpdate = { endpoint.changeMetadata() }

        assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
          GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)
            .update(KEY, GENERATION, null, mapOf("synced_by" to "availability-sync"))
        }

        assertThat(endpoint.metadata)
          .containsExactly("owner", "external-producer", "sync_id", "old", "concurrent", "writer")
      }
    }

  @Test
  fun `update rejects a missing finalized object`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.exists = false

        assertFailsWith<ExternalDataAvailabilitySnapshotChangedException> {
          GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)
            .update(KEY, GENERATION, null, mapOf("synced_by" to "availability-sync"))
        }
      }
    }

  @Test
  fun `update preserves other storage errors`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.updateStatus = 403

        val exception =
          assertFailsWith<StorageException> {
            GcsGenerationMatchedBlobMetadataUpdater(endpoint.storage, BUCKET)
              .update(KEY, GENERATION, null, mapOf("synced_by" to "availability-sync"))
          }

        assertThat(exception.code).isEqualTo(403)
        assertThat(endpoint.metadata)
          .containsExactly("owner", "external-producer", "sync_id", "old")
      }
    }

  private class GcsEndpoint : AutoCloseable {
    var metadata = mapOf("owner" to "external-producer", "sync_id" to "old")
      private set

    var customTime: Instant? = null
      private set

    var exists = true
    var beforeUpdate: (() -> Unit)? = null
    var updateStatus = 200
    private var generation = GENERATION
    private var metageneration = 3L
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    val storage: Storage

    init {
      server.createContext("/") { exchange ->
        exchange.use {
          val query =
            exchange.requestURI.rawQuery
              .orEmpty()
              .split('&')
              .filter(String::isNotEmpty)
              .associate { parameter ->
                val parts = parameter.split('=', limit = 2)
                parts[0] to URLDecoder.decode(parts.getOrElse(1) { "" }, StandardCharsets.UTF_8)
              }
          val method =
            exchange.requestHeaders.getFirst("X-HTTP-Method-Override") ?: exchange.requestMethod
          val status =
            when (method) {
              "GET" ->
                if (
                  !exists ||
                    (query.containsKey("generation") &&
                      query["generation"] != generation.toString())
                )
                  404
                else 200
              "PATCH" -> {
                beforeUpdate?.invoke()
                beforeUpdate = null
                when {
                  !exists -> 404
                  query["ifGenerationMatch"] != generation.toString() -> 412
                  query.containsKey("ifMetagenerationMatch") &&
                    query["ifMetagenerationMatch"] != metageneration.toString() -> 412
                  else -> updateStatus
                }.also { code ->
                  if (code == 200) {
                    val stream =
                      if (exchange.requestHeaders.getFirst("Content-Encoding") == "gzip")
                        GZIPInputStream(exchange.requestBody)
                      else exchange.requestBody
                    val patch = JsonParser.parseReader(stream.bufferedReader()).asJsonObject
                    patch.getAsJsonObject("metadata")?.let { value ->
                      metadata = value.entrySet().associate { it.key to it.value.asString }
                    }
                    patch.get("customTime")?.let { value ->
                      customTime = Instant.parse(value.asString)
                    }
                    metageneration++
                  }
                }
              }
              else -> 405
            }
          val body =
            if (status == 200) {
              Gson()
                .toJson(
                  buildMap {
                    put("bucket", BUCKET)
                    put("name", KEY)
                    put("generation", generation.toString())
                    put("metageneration", metageneration.toString())
                    put("metadata", metadata)
                    customTime?.let { put("customTime", it.toString()) }
                  }
                )
            } else {
              """{"error":{"code":$status,"message":"Object update rejected"}}"""
            }
          val bytes = body.toByteArray()
          exchange.responseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(status, bytes.size.toLong())
          exchange.responseBody.write(bytes)
        }
      }
      server.start()
      storage =
        StorageOptions.newBuilder()
          .setHost("http://127.0.0.1:${server.address.port}")
          .setProjectId("metadata-test-project")
          .setCredentials(NoCredentials.getInstance())
          .build()
          .service
    }

    fun overwrite() {
      generation++
      metageneration = 1L
      metadata = mapOf("owner" to "new-producer")
      customTime = null
    }

    fun changeMetadata() {
      metadata = metadata + ("concurrent" to "writer")
      metageneration++
    }

    override fun close() {
      storage.close()
      server.stop(0)
    }
  }

  companion object {
    private const val BUCKET = "metadata-test-bucket"
    private const val KEY = "model-line/example/2026-01-01/done"
    private const val GENERATION = 77L
    private val CREATE_TIME = Instant.parse("2026-01-02T00:00:00Z")
  }
}
