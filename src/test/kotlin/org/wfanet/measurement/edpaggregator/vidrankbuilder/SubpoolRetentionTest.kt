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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.type.date
import java.time.LocalDate
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

private const val DP = "dataProviders/dp"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val POOL = 7L
private val TODAY = LocalDate.ofEpochDay(100)

@RunWith(JUnit4::class)
class SubpoolRetentionTest {
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var storageClient: InMemoryStorageClient
  private lateinit var rankStore: RankIndexStore

  @Before
  fun setUp() {
    AeadConfig.register()
    kmsClient =
      FakeKmsClient().apply {
        setAead(
          kekUri,
          KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead::class.java),
        )
      }
    storageClient = InMemoryStorageClient()
    rankStore = RankIndexStore(storageClient, kmsClient)
  }

  private suspend fun writeBlob(key: String): RankIndexBlob {
    val dek = rankStore.generateDek(kekUri)
    val checksum =
      rankStore.writeBlob(
        key,
        dek,
        flowOf(
          rankIndexMap {
            poolOffset = POOL
            rankedSize = 100
          }
        ),
      )
    return rankIndexBlob {
      name = "dataProviders/dp/rawImpressionUploads/up0/rankIndexBlobs/$key"
      blobType = RankIndexBlob.BlobType.DAY_ONLY
      cmmsModelLine = MODEL_LINE
      poolOffset = POOL
      blobUri = key
      blobChecksum = checksum
      encryptedDek = dek
      maxEventDate = date {
        year = 2000
        month = 1
        day = 1
      } // old
    }
  }

  @Test
  fun `deleteAgedBlobs soft-deletes the row and removes the blob bytes`() = runBlocking {
    val candidate = writeBlob("day/old")
    val stub: RankIndexBlobServiceCoroutineStub = mock {
      onBlocking { listRankIndexBlobs(any(), any()) } doReturn
        listRankIndexBlobsResponse { rankIndexBlobs += candidate }
      onBlocking { deleteRankIndexBlob(any(), any()) } doReturn candidate
    }
    val retention =
      SubpoolRetention(stub, rankStore, DP, MODEL_LINE, retentionDays = 30, today = TODAY)

    retention.deleteAgedBlobs(POOL)

    verifyBlocking(stub) { deleteRankIndexBlob(any(), any()) }
    assertThat(rankStore.readBlob("day/old", candidate.encryptedDek).toList()).isEmpty()
  }

  @Test
  fun `deleteAgedBlobs is a no-op when nothing is aged out`() = runBlocking {
    val kept = writeBlob("day/keep")
    val stub: RankIndexBlobServiceCoroutineStub = mock {
      onBlocking { listRankIndexBlobs(any(), any()) } doReturn listRankIndexBlobsResponse {}
    }
    val retention =
      SubpoolRetention(stub, rankStore, DP, MODEL_LINE, retentionDays = 30, today = TODAY)

    retention.deleteAgedBlobs(POOL)

    verifyBlocking(stub, never()) { deleteRankIndexBlob(any(), any()) }
    assertThat(rankStore.readBlob("day/keep", kept.encryptedDek).toList()).isNotEmpty()
  }
}
