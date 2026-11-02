// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.cloud.NoCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageException
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

class FixtureStorageClientTest {
  @Test
  fun `writeBlob creates and replaces a fixture`() = runBlocking {
    val backing = InMemoryStorageClient()
    val client = FixtureStorageClient(backing)
    client.writeBlob("fixture", "first".toByteStringUtf8())
    client.writeBlob("fixture", "replacement".toByteStringUtf8())
    assertThat(backing.getBlob("fixture")!!.read().flatten())
      .isEqualTo("replacement".toByteStringUtf8())
  }

  @Test
  fun `writeBlob preserves a fixture created after its generation was read`() = runBlocking {
    val backing = InMemoryStorageClient()
    val racingClient =
      object : ConditionalOperationStorageClient by backing {
        override suspend fun getFreshnessToken(blobKey: String): String? {
          backing.writeBlob(blobKey, "other writer".toByteStringUtf8())
          return null
        }
      }
    assertFailsWith<BlobChangedException> {
      FixtureStorageClient(racingClient).writeBlob("fixture", "replacement".toByteStringUtf8())
    }
    assertThat(backing.getBlob("fixture")!!.read().flatten())
      .isEqualTo("other writer".toByteStringUtf8())
  }

  @Test
  fun `writeBlob preserves a fixture changed after its generation was read`() = runBlocking {
    val backing = InMemoryStorageClient()
    backing.writeBlob("fixture", "original".toByteStringUtf8())
    val racingClient =
      object : ConditionalOperationStorageClient by backing {
        override suspend fun getFreshnessToken(blobKey: String): String? {
          val token = backing.getFreshnessToken(blobKey)
          backing.writeBlob(blobKey, "other writer".toByteStringUtf8())
          return token
        }
      }
    assertFailsWith<BlobChangedException> {
      FixtureStorageClient(racingClient).writeBlob("fixture", "replacement".toByteStringUtf8())
    }
    assertThat(backing.getBlob("fixture")!!.read().flatten())
      .isEqualTo("other writer".toByteStringUtf8())
  }

  @Test
  fun `writeBlob retries GCS upload initiation without recollecting content`() = runBlocking {
    GcsEndpoint(existingGeneration = null, firstUploadStatus = 503).use { endpoint ->
      val collections = AtomicInteger()
      val content = flow {
        collections.incrementAndGet()
        emit("fixture".toByteStringUtf8())
      }
      FixtureStorageClient(GcsStorageClient(endpoint.storage, BUCKET)).writeBlob(KEY, content)
      assertThat(endpoint.uploadAttempts.get()).isEqualTo(2)
      assertThat(endpoint.preconditions.get())
        .containsExactly("ifGenerationMatch=0", "ifGenerationMatch=0")
      assertThat(collections.get()).isEqualTo(1)
      assertThat(endpoint.uploaded.get()).isEqualTo("fixture".toByteArray())
    }
  }

  @Test
  fun `writeBlob retries GCS replacement against the observed generation`() = runBlocking {
    GcsEndpoint(existingGeneration = 7L, firstUploadStatus = 503).use { endpoint ->
      FixtureStorageClient(GcsStorageClient(endpoint.storage, BUCKET))
        .writeBlob(KEY, "fixture".toByteStringUtf8())
      assertThat(endpoint.uploadAttempts.get()).isEqualTo(2)
      assertThat(endpoint.preconditions.get())
        .containsExactly("ifGenerationMatch=7", "ifGenerationMatch=7")
      assertThat(endpoint.uploaded.get()).isEqualTo("fixture".toByteArray())
    }
  }

  @Test
  fun `writeBlob preserves permanent GCS errors`() = runBlocking {
    GcsEndpoint(existingGeneration = null, firstUploadStatus = 403).use { endpoint ->
      val error =
        assertFailsWith<StorageException> {
          FixtureStorageClient(GcsStorageClient(endpoint.storage, BUCKET))
            .writeBlob(KEY, "fixture".toByteStringUtf8())
        }
      val gcsError =
        generateSequence<Throwable>(error) { it.cause }
          .filterIsInstance<com.google.cloud.storage.StorageException>()
          .first()
      assertThat(gcsError.code).isEqualTo(403)
      assertThat(endpoint.uploadAttempts.get()).isEqualTo(1)
      assertThat(endpoint.uploaded.get()).isNull()
    }
  }

  @Test
  fun `writeBlob preserves GCS generation conflicts`() = runBlocking {
    GcsEndpoint(existingGeneration = 7L, firstUploadStatus = 412).use { endpoint ->
      assertFailsWith<BlobChangedException> {
        FixtureStorageClient(GcsStorageClient(endpoint.storage, BUCKET))
          .writeBlob(KEY, "fixture".toByteStringUtf8())
      }
      assertThat(endpoint.uploadAttempts.get()).isEqualTo(1)
      assertThat(endpoint.uploaded.get()).isNull()
    }
  }

  private class GcsEndpoint(existingGeneration: Long?, firstUploadStatus: Int) : AutoCloseable {
    val uploadAttempts = AtomicInteger()
    val preconditions = AtomicReference<List<String>>(emptyList())
    val uploaded = AtomicReference<ByteArray>()
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    val storage: Storage

    init {
      val endpoint = "http://127.0.0.1:${server.address.port}"
      server.createContext("/") { exchange ->
        exchange.use {
          val method = exchange.requestMethod
          val bytes = exchange.requestBody.readBytes()
          val generation =
            if (method == "PUT" || uploaded.get() != null) (existingGeneration ?: 0L) + 1L
            else existingGeneration
          val metadata =
            """{"bucket":"$BUCKET","name":"$KEY","generation":"$generation","size":"7","timeCreated":"2026-01-01T00:00:00.000Z","updated":"2026-01-01T00:00:00.000Z"}"""
          val status: Int
          val body: String
          when (method) {
            "GET" -> {
              status = if (generation == null) 404 else 200
              body =
                if (generation == null) """{"error":{"code":404,"message":"Not found"}}"""
                else metadata
            }
            "POST" -> {
              val condition =
                exchange.requestURI.rawQuery.split('&').singleOrNull {
                  it.startsWith("ifGenerationMatch=")
                } ?: "missing"
              preconditions.updateAndGet { it + condition }
              status = if (uploadAttempts.incrementAndGet() == 1) firstUploadStatus else 200
              body =
                if (status == 200) "{}"
                else """{"error":{"code":$status,"message":"Upload rejected"}}"""
              if (status == 200)
                exchange.responseHeaders.add("Location", "$endpoint/upload/session")
            }
            "PUT" -> {
              uploaded.set(bytes)
              status = 200
              body = metadata
            }
            else -> {
              status = 405
              body = "{}"
            }
          }
          val response = body.toByteArray()
          exchange.responseHeaders.add("Content-Type", "application/json")
          exchange.sendResponseHeaders(status, response.size.toLong())
          exchange.responseBody.write(response)
        }
      }
      server.start()
      storage =
        StorageOptions.newBuilder()
          .setHost(endpoint)
          .setProjectId("fixture-project")
          .setCredentials(NoCredentials.getInstance())
          .setRetrySettings(
            StorageOptions.getDefaultRetrySettings()
              .toBuilder()
              .setInitialRetryDelayDuration(Duration.ZERO)
              .setMaxRetryDelayDuration(Duration.ZERO)
              .setMaxAttempts(3)
              .build()
          )
          .build()
          .service
    }

    override fun close() {
      storage.close()
      server.stop(0)
    }
  }

  companion object {
    private const val BUCKET = "fixture-bucket"
    private const val KEY = "impressions"
  }
}
