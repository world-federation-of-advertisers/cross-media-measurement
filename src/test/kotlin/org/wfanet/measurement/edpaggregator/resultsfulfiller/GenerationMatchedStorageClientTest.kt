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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.gson.Gson
import com.google.protobuf.kotlin.toByteStringUtf8
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

class GenerationMatchedStorageClientTest {
  @Test
  fun `getBlob reads the finalized generation`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        val client = GenerationMatchedStorageClient(GcsStorageClient(endpoint.storage, BUCKET), 7L)
        assertThat(client.getBlob(KEY)!!.read().flatten()).isEqualTo("finalized".toByteStringUtf8())
      }
    }

  @Test
  fun `getBlob rejects an overwrite before lookup`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.overwrite()
        val error =
          assertFailsWith<ImpressionReadException> {
            GenerationMatchedStorageClient(GcsStorageClient(endpoint.storage, BUCKET), 7L)
              .getBlob(KEY)
          }
        assertThat(error.code).isEqualTo(ImpressionReadException.Code.BLOB_GENERATION_CHANGED)
      }
    }

  @Test
  fun `getBlob retains the admitted generation when overwritten before reading`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        val client = GenerationMatchedStorageClient(GcsStorageClient(endpoint.storage, BUCKET), 7L)
        val captured = client.getBlob(KEY)!!
        endpoint.overwrite()
        assertThat(captured.read().flatten()).isEqualTo("finalized".toByteStringUtf8())
      }
    }

  @Test
  fun `getBlob retains legacy current object reads when generation is unset`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.overwrite()
        val client = GenerationMatchedStorageClient(GcsStorageClient(endpoint.storage, BUCKET), 0L)
        assertThat(client.getBlob(KEY)!!.read().flatten()).isEqualTo("late".toByteStringUtf8())
      }
    }

  @Test
  fun `getBlob returns null for a missing finalized object`() =
    runBlocking<Unit> {
      GcsEndpoint().use { endpoint ->
        endpoint.exists = false
        assertThat(
            GenerationMatchedStorageClient(GcsStorageClient(endpoint.storage, BUCKET), 7L)
              .getBlob(KEY)
          )
          .isNull()
      }
    }

  private class GcsEndpoint : AutoCloseable {
    @Volatile private var generation = 7L
    @Volatile var exists = true
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
          val requested = query["generation"]?.toLong() ?: generation
          val media = query["alt"] == "media"
          val status = if (!exists || requested !in 7L..generation) 404 else 200
          val body =
            if (status != 200) {
              """{"error":{"code":404,"message":"Object not found"}}"""
            } else if (media) {
              if (requested == 7L) "finalized" else "late"
            } else {
              Gson()
                .toJson(
                  mapOf(
                    "bucket" to BUCKET,
                    "name" to KEY,
                    "generation" to requested.toString(),
                    "metageneration" to "1",
                    "size" to if (requested == 7L) "9" else "4",
                    "timeCreated" to "2026-01-01T00:00:00Z",
                    "updated" to "2026-01-01T00:00:00Z",
                  )
                )
            }
          val bytes = body.toByteArray()
          exchange.responseHeaders.add(
            "Content-Type",
            if (media) "application/octet-stream" else "application/json",
          )
          exchange.sendResponseHeaders(status, bytes.size.toLong())
          exchange.responseBody.write(bytes)
        }
      }
      server.start()
      storage =
        StorageOptions.newBuilder()
          .setHost("http://127.0.0.1:${server.address.port}")
          .setProjectId("generation-test-project")
          .setCredentials(NoCredentials.getInstance())
          .build()
          .service
    }

    fun overwrite() {
      generation = 8L
    }

    override fun close() {
      storage.close()
      server.stop(0)
    }
  }

  companion object {
    private const val BUCKET = "generation-test-bucket"
    private const val KEY = "impressions"
  }
}
