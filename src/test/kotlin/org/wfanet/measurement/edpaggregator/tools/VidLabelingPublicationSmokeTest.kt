// Copyright 2026 The Cross-Media Measurement Authors
// Licensed under the Apache License, Version 2.0.

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.io.IOException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

class VidLabelingPublicationSmokeTest {
  private val prefix = "gs://test/rollout/publication"

  @Test
  fun successfulSmokeVerifiesWinnerAndCleansOnlyItsOwnObjects() =
    runBlocking<Unit> {
      val backend = InMemoryStorageClient()
      backend.writeBlob("unrelated", flowOf(ByteString.copyFromUtf8("keep")))
      val result = VidLabelingPublicationSmokeRunner { backend }.run(prefix)
      assertThat(result.fieldsMap.getValue("status").stringValue).isEqualTo("PASS")
      assertThat(result.fieldsMap.getValue("decrypted_records").numberValue).isEqualTo(1.0)
      assertThat(result.fieldsMap.getValue("cleanup_complete").boolValue).isTrue()
      assertThat(backend.listBlobs().toList().map { it.blobKey }).containsExactly("unrelated")
    }

  @Test
  fun unrelatedSelectedDataCannotPassSmoke() =
    runBlocking<Unit> {
      val backend = InMemoryStorageClient()
      val client =
        object : ConditionalOperationStorageClient by backend {
          override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
            backend.listBlobs(prefix).map { blob ->
              if (!blob.blobKey.endsWith(".metadata.binpb")) blob
              else
                object : StorageClient.Blob by blob {
                  override fun read(): Flow<ByteString> =
                    kotlinx.coroutines.flow.flow {
                      val details = BlobDetails.parseFrom(blob.read().flatten())
                      emit(
                        details.toBuilder().setBlobUri("gs://test/unrelated").build().toByteString()
                      )
                    }
                }
            }
        }
      assertFailsWith<IllegalStateException> {
        VidLabelingPublicationSmokeRunner { client }.run(prefix)
      }
      assertThat(backend.listBlobs().toList()).isEmpty()
    }

  @Test
  fun writeFailureCannotProducePassingSmoke() =
    runBlocking<Unit> {
      val backend = InMemoryStorageClient()
      val client =
        object : ConditionalOperationStorageClient by backend {
          override suspend fun writeBlobIfNotFound(
            blobKey: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob {
            throw IOException("injected data write failure")
          }
        }
      assertFailsWith<IOException> { VidLabelingPublicationSmokeRunner { client }.run(prefix) }
      assertThat(backend.listBlobs().toList()).isEmpty()
    }

  @Test
  fun cleanupFailureCannotReportSuccess() =
    runBlocking<Unit> {
      val backend = InMemoryStorageClient()
      val client =
        object : ConditionalOperationStorageClient by backend {
          override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
            backend.listBlobs(prefix).map { blob ->
              object : StorageClient.Blob by blob {
                override suspend fun delete() {
                  throw IOException("injected cleanup failure")
                }
              }
            }
        }
      assertFailsWith<IOException> { VidLabelingPublicationSmokeRunner { client }.run(prefix) }
    }

  @Test
  fun productionNamespaceIsRejectedBeforeAnyWrite() =
    runBlocking<Unit> {
      val backend = InMemoryStorageClient()
      assertFailsWith<IllegalArgumentException> {
        VidLabelingPublicationSmokeRunner { backend }.run("gs://test/production")
      }
      assertThat(backend.listBlobs().toList()).isEmpty()
    }
}
