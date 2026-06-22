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
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.Date
import com.google.type.date
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

private const val DP = "dataProviders/dp"
private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/up1"
private const val UPLOAD2 = "dataProviders/dp/rawImpressionUploads/up2"
private const val PRIOR_UPLOAD = "dataProviders/dp/rawImpressionUploads/up0"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"
private const val PREFIX = "ranks"
private const val POOL = 7L
private const val RETENTION_DAYS = 30
private val TODAY = LocalDate.ofEpochDay(100)
private const val TODAY_EPOCH_DAY = 100 // == eventDay stamped on touched fingerprints

@RunWith(JUnit4::class)
class SubpoolRankerTest {
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var storageClient: InMemoryStorageClient
  private lateinit var subpoolStore: SubpoolFingerprintsStore
  private lateinit var rankStore: RankIndexStore
  private lateinit var subpoolDek: EncryptedDek
  private lateinit var fake: FakeRankIndexBlobs

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
    subpoolStore = SubpoolFingerprintsStore(storageClient, kmsClient)
    rankStore = RankIndexStore(storageClient, kmsClient)
    subpoolDek = subpoolStore.generateDek(kekUri)
    fake = FakeRankIndexBlobs()
  }

  private fun ranker(
    maxEventDate: Date = epochDayToDate(TODAY_EPOCH_DAY),
    upload: String = UPLOAD,
  ): SubpoolRanker {
    val retention = SubpoolRetention(fake.stub, rankStore, DP, MODEL_LINE, RETENTION_DAYS, TODAY)
    return SubpoolRanker(
      subpoolFingerprintsStore = subpoolStore,
      rankIndexStore = rankStore,
      rankIndexBlobsStub = fake.stub,
      retention = retention,
      dataProvider = DP,
      rawImpressionUpload = upload,
      modelLine = MODEL_LINE,
      vidRankMapBlobPrefix = PREFIX,
      kekUri = kekUri,
      encryptedSubpoolMapsDek = subpoolDek,
      maxEventDate = maxEventDate,
      retentionDays = RETENTION_DAYS,
      today = TODAY,
    )
  }

  /** Writes a Phase-0 merged blob for the subpool at [key] and returns it. */
  private suspend fun writePhase0(
    fps: List<Pair<Long, Int>>,
    key: String = "phase0/merged/$POOL",
  ): String {
    subpoolStore.writeBlob(key, subpoolDek, POOL, flowOf(pack(fps)))
    return key
  }

  /**
   * Seeds a prior SNAPSHOT blob under [uploadName] plus its `RankIndexBlob` row. When
   * [maxEventDate] is non-null it is stamped on the row (the snapshot-selection key that backfill
   * detection reads).
   */
  private suspend fun seedPriorSnapshot(
    entries: List<Triple<Pair<Long, Int>, Int, Int>>,
    uploadName: String = PRIOR_UPLOAD,
    createTimeSeconds: Long = 1L,
    maxEventDate: Date? = null,
    blobKey: String = "ranks/prior/snapshot/$POOL/$createTimeSeconds",
    rowName: String = "$uploadName/rankIndexBlobs/snap-$createTimeSeconds",
  ) {
    val dek = rankStore.generateDek(kekUri)
    val lastSeenBytes = ByteArray(entries.size * LastSeenDayBytes.WIDTH)
    entries.forEachIndexed { i, e ->
      LastSeenDayBytes.write(lastSeenBytes, i * LastSeenDayBytes.WIDTH, e.third)
    }
    val record = rankIndexMap {
      poolOffset = POOL
      rankedSize = 100
      fingerprints = pack(entries.map { it.first })
      entries.forEach { ranks += it.second }
      lastSeenDays = ByteString.copyFrom(lastSeenBytes)
    }
    val checksum = rankStore.writeBlob(blobKey, dek, flowOf(record))
    fake.seed(
      rankIndexBlob {
        name = rowName
        blobType = RankIndexBlob.BlobType.SNAPSHOT
        cmmsModelLine = MODEL_LINE
        poolOffset = POOL
        blobUri = blobKey
        blobChecksum = checksum
        encryptedDek = dek
        createTime = timestamp { seconds = createTimeSeconds }
        if (maxEventDate != null) this.maxEventDate = maxEventDate
      }
    )
  }

  /** Reads back the SNAPSHOT row's blob under [upload] as `(hi, lo) -> (rank, lastSeen)`. */
  private suspend fun readSnapshot(upload: String = UPLOAD): Map<Pair<Long, Int>, Pair<Int, Int>> {
    val row =
      fake.rows.single {
        it.blobType == RankIndexBlob.BlobType.SNAPSHOT && it.name.startsWith("$upload/")
      }
    return decode(rankStore.readBlob(row.blobUri, row.encryptedDek).toList())
  }

  /** Reads back the DAY_ONLY row's blob under [upload] as `(hi, lo) -> (rank, lastSeen)`. */
  private suspend fun readDayOnly(upload: String = UPLOAD): Map<Pair<Long, Int>, Pair<Int, Int>> {
    val row =
      fake.rows.single {
        it.blobType == RankIndexBlob.BlobType.DAY_ONLY && it.name.startsWith("$upload/")
      }
    return decode(rankStore.readBlob(row.blobUri, row.encryptedDek).toList())
  }

  /** Whether a row of [blobType] exists under [upload]. */
  private fun hasUploadRow(blobType: RankIndexBlob.BlobType, upload: String = UPLOAD): Boolean =
    fake.rows.any { it.blobType == blobType && it.name.startsWith("$upload/") }

  @Test
  fun `cold subpool allocates sequential ranks and stamps last_seen`() = runBlocking {
    val key = writePhase0(listOf(1L to 0, 2L to 0, 3L to 0))

    val result = ranker().rank(POOL, key, rankedSize = 100)

    assertThat(result.skipped).isFalse()
    assertThat(result.allocated).isEqualTo(3)
    val snapshot = readSnapshot()
    assertThat(snapshot.mapValues { it.value.first })
      .containsExactly(1L to 0, 0, 2L to 0, 1, 3L to 0, 2)
    assertThat(snapshot.values.map { it.second }.toSet()).containsExactly(TODAY_EPOCH_DAY)
    // A DAY_ONLY row is recorded with the dispatch's max event date.
    assertThat(
        fake.rows.any { it.blobType == RankIndexBlob.BlobType.DAY_ONLY && it.hasMaxEventDate() }
      )
      .isTrue()
  }

  @Test
  fun `idempotency gate skips a subpool already ranked for this upload`() = runBlocking {
    // A SNAPSHOT row already exists under THIS upload (a prior attempt committed).
    fake.seed(
      rankIndexBlob {
        name = "$UPLOAD/rankIndexBlobs/snap-existing"
        blobType = RankIndexBlob.BlobType.SNAPSHOT
        cmmsModelLine = MODEL_LINE
        poolOffset = POOL
      }
    )
    val key = writePhase0(listOf(1L to 0))
    val before = fake.rows.size

    val result = ranker().rank(POOL, key, rankedSize = 100)

    assertThat(result.skipped).isTrue()
    assertThat(fake.rows.size).isEqualTo(before) // no new rows created
  }

  @Test
  fun `warm subpool renews existing fingerprints and allocates new ones`() = runBlocking {
    seedPriorSnapshot(
      listOf(Triple(10L to 0, 0, 90), Triple(20L to 0, 1, 90)) // both recent (>= cutoff 70)
    )
    val key = writePhase0(listOf(20L to 0, 30L to 0)) // 20 renewed, 30 new

    ranker().rank(POOL, key, rankedSize = 100)

    val snapshot = readSnapshot()
    assertThat(snapshot.keys).containsExactly(10L to 0, 20L to 0, 30L to 0)
    assertThat(snapshot[10L to 0]).isEqualTo(0 to 90) // untouched: rank + old last_seen preserved
    assertThat(snapshot[20L to 0]).isEqualTo(1 to TODAY_EPOCH_DAY) // renewed: last_seen refreshed
    assertThat(snapshot[30L to 0]!!.first).isEqualTo(2) // new rank
  }

  @Test
  fun `retention frees an aged rank and a new fingerprint reuses the freed slot`() = runBlocking {
    seedPriorSnapshot(
      listOf(
        Triple(10L to 0, 0, 50), // aged (last_seen 50 < cutoff 70), not seen today -> freed
        Triple(20L to 0, 1, 90), // recent -> kept
      )
    )
    val key = writePhase0(listOf(30L to 0)) // only a new fingerprint

    val result = ranker().rank(POOL, key, rankedSize = 100)

    assertThat(result.freed).isEqualTo(1)
    val snapshot = readSnapshot()
    assertThat(snapshot.keys).containsExactly(20L to 0, 30L to 0)
    assertThat(snapshot[20L to 0]!!.first).isEqualTo(1)
    assertThat(snapshot[30L to 0]!!.first).isEqualTo(0) // reused the freed rank 0
  }

  @Test
  fun `overflow leaves fingerprints unranked once the subpool is full`() = runBlocking {
    val key = writePhase0(listOf(1L to 0, 2L to 0, 3L to 0))

    val result = ranker().rank(POOL, key, rankedSize = 2)

    assertThat(result.allocated).isEqualTo(2)
    assertThat(result.overflow).isEqualTo(1)
    assertThat(readSnapshot()).hasSize(2)
  }

  @Test
  fun `prior snapshot with a mismatched checksum fails the rank`() =
    runBlocking<Unit> {
      seedPriorSnapshot(listOf(Triple(10L to 0, 0, 90)))
      // Corrupt the seeded snapshot's recorded checksum so it no longer matches the bytes; the
      // integrity guard in RankIndexStore.readBlob must fire when the allocator loads it.
      val idx = fake.rows.indexOfFirst { it.blobType == RankIndexBlob.BlobType.SNAPSHOT }
      fake.rows[idx] =
        fake.rows[idx].copy { blobChecksum = ByteString.copyFromUtf8("not-the-real-checksum") }
      val key = writePhase0(listOf(20L to 0))

      assertFailsWith<IllegalStateException> { ranker().rank(POOL, key, rankedSize = 100) }
    }

  @Test
  fun `loads the newest prior snapshot across uploads`() =
    runBlocking<Unit> {
      // Two prior SNAPSHOTs for this subpool under different uploads; the newer create_time wins.
      seedPriorSnapshot(
        listOf(Triple(10L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
      )
      seedPriorSnapshot(
        listOf(Triple(99L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upNew",
        createTimeSeconds = 5L,
      )
      val key = writePhase0(emptyList()) // no new fingerprints; result mirrors the loaded prior

      ranker().rank(POOL, key, rankedSize = 100)

      // The allocator loaded the newer snapshot (99), not the older one (10).
      assertThat(readSnapshot().keys).containsExactly(99L to 0)
    }

  @Test
  fun `empty subpool still writes a snapshot carrying the prior cumulative unchanged`() =
    runBlocking<Unit> {
      seedPriorSnapshot(listOf(Triple(10L to 0, 0, 90)))
      val key = writePhase0(emptyList())

      val result = ranker().rank(POOL, key, rankedSize = 100)

      assertThat(result.allocated).isEqualTo(0)
      assertThat(result.renewed).isEqualTo(0)
      assertThat(result.overflow).isEqualTo(0)
      // A SNAPSHOT row is still inserted under this upload, carrying the prior cumulative
      // unchanged.
      assertThat(readSnapshot()).containsExactly(10L to 0, 0 to 90)
    }

  @Test
  fun `rank fails when maxEventDate is unset`() = runBlocking {
    val key = writePhase0(listOf(1L to 0))

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ranker(maxEventDate = Date.getDefaultInstance()).rank(POOL, key, rankedSize = 100)
      }
    assertThat(exception).hasMessageThat().contains("maxEventDate must be set")
  }

  @Test
  fun `backfill takes a free old rank back into both the day-only and the snapshot`() =
    runBlocking<Unit> {
      // Old snapshot (just before the backfilled day) holds F1 at rank 5.
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 75)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      // Latest snapshot is newer; F1 has aged out so rank 5 is free there.
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0)) // the backfilled upload contains F1

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(1)
      assertThat(result.backfillRankCollisions).isEqualTo(0)
      // F1 gets its old rank 5 back in the day-only blob (stamped with the backfill date).
      assertThat(readDayOnly()).containsExactly(1L to 0, 5 to 80)
      // The snapshot is the latest cumulative plus F1 at its reclaimed rank.
      assertThat(readSnapshot()).containsExactly(2L to 0, 0 to 90, 1L to 0, 5 to 80)
    }

  @Test
  fun `backfill counts a collision but keeps the latest rank in the snapshot`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 75)), // old: F1 at rank 5
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 8, 90)), // latest: F1 re-handed a different rank 8
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfillReusedOldRank).isEqualTo(1)
      assertThat(result.backfillRankCollisions).isEqualTo(1)
      // The old rank wins for the backfilled day (one person, one VID for that day)...
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(1L to 0, 5)
      // ...but the snapshot keeps the latest rank so the forward chain is undisturbed.
      assertThat(readSnapshot().mapValues { it.value.first }).containsExactly(1L to 0, 8)
    }

  @Test
  fun `backfill keeps the latest rank for a fingerprint only in the latest snapshot`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(9L to 0, 3, 75)), // old snapshot exists (a different fp)
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(3L to 0, 2, 90)), // latest: F3 at rank 2
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(3L to 0)) // F3 is only in the latest snapshot

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(0)
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(3L to 0, 2)
      assertThat(readSnapshot().mapValues { it.value.first }).containsExactly(3L to 0, 2)
    }

  @Test
  fun `backfill adds a brand-new fingerprint to both the day-only and the snapshot`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(9L to 0, 3, 75)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(8L to 0, 0, 90)), // latest holds rank 0
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(4L to 0)) // F4 is in neither snapshot

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(0)
      // rank 0 is taken in the loaded latest cumulative, so F4 gets the next free rank 1.
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(4L to 0, 1)
      // The new fingerprint is persisted in the snapshot so a later upload will not re-rank it.
      assertThat(readSnapshot().mapValues { it.value.first })
        .containsExactly(8L to 0, 0, 4L to 0, 1)
    }

  @Test
  fun `a fingerprint new in a backfill keeps its rank on a later forward upload`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(9L to 0, 3, 75)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      // Backfill Day 80 introduces a brand-new fingerprint (in neither snapshot).
      val backfillKey = writePhase0(listOf(7L to 0), key = "phase0/backfill")
      val backfillResult =
        ranker(maxEventDate = epochDayToDate(80), upload = UPLOAD)
          .rank(POOL, backfillKey, rankedSize = 100)
      assertThat(backfillResult.backfill).isTrue()
      val backfillRank = readSnapshot(UPLOAD).getValue(7L to 0).first

      // A later forward upload (Day 101) must renew it at the same rank, not re-rank it.
      val forwardKey = writePhase0(listOf(7L to 0), key = "phase0/forward")
      val forwardResult =
        ranker(maxEventDate = epochDayToDate(101), upload = UPLOAD2)
          .rank(POOL, forwardKey, rankedSize = 100)

      assertThat(forwardResult.backfill).isFalse()
      assertThat(forwardResult.renewed).isEqualTo(1) // renewed, not re-allocated
      assertThat(forwardResult.allocated).isEqualTo(0)
      assertThat(readSnapshot(UPLOAD2).getValue(7L to 0).first).isEqualTo(backfillRank)
    }

  @Test
  fun `an upload older than the retention window is not treated as a backfill`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(7L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(5L to 0))

      // 100 - 60 = 40 > RETENTION_DAYS (30): outside the window, so the normal path runs.
      val result = ranker(maxEventDate = epochDayToDate(60)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isFalse()
      assertThat(hasUploadRow(RankIndexBlob.BlobType.SNAPSHOT)).isTrue()
    }

  @Test
  fun `backfill keeps the shared rank when the old and latest snapshots agree`() =
    runBlocking<Unit> {
      // The common case: a stable fingerprint holds the same rank in both snapshots.
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 75)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(1)
      assertThat(result.backfillRankCollisions).isEqualTo(0) // old == latest, no divergence
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(1L to 0, 5)
      assertThat(readSnapshot().mapValues { it.value.first }).containsExactly(1L to 0, 5)
    }

  @Test
  fun `backfill labels with the old rank but gives a fresh snapshot rank when the old slot is taken`() =
    runBlocking<Unit> {
      // F1 held rank 5 in the old snapshot, then aged out; rank 5 is now Bob's in the latest.
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 75)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(75),
      )
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 5, 90)), // Bob (2L) holds rank 5 now
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0)) // backfill F1

      val result = ranker(maxEventDate = epochDayToDate(80)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(1)
      assertThat(result.backfillRankCollisions).isEqualTo(0) // F1 is not in the latest snapshot
      // The backfilled day is labeled with the original rank 5 (shared with Bob — accepted
      // undercount)...
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(1L to 0, 5)
      // ...but the cumulative gives F1 a fresh rank so it never shares Bob's rank going forward.
      assertThat(readSnapshot().mapValues { it.value.first })
        .containsExactly(2L to 0, 5, 1L to 0, 0)
    }

  @Test
  fun `backfill is active at exactly the retention-window boundary`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 65)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(65),
      )
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      // 100 - 70 == RETENTION_DAYS (30): on the boundary, still a backfill.
      val result = ranker(maxEventDate = epochDayToDate(70)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      assertThat(result.backfillReusedOldRank).isEqualTo(1)
    }

  @Test
  fun `an upload one day past the retention window is not a backfill`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 5, 60)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upOld",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(60),
      )
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      // 100 - 69 == RETENTION_DAYS + 1 (31): just outside the window, so the normal path runs.
      val result = ranker(maxEventDate = epochDayToDate(69)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isFalse()
    }

  @Test
  fun `backfill replays the closest older snapshot when several exist`() =
    runBlocking<Unit> {
      // Two snapshots are older than the backfilled day; the closer (Day 60) must win over Day 40.
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 3, 38)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upFar",
        createTimeSeconds = 1L,
        maxEventDate = epochDayToDate(40),
      )
      seedPriorSnapshot(
        listOf(Triple(1L to 0, 7, 58)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upMid",
        createTimeSeconds = 3L,
        maxEventDate = epochDayToDate(60),
      )
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      val result = ranker(maxEventDate = epochDayToDate(70)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isTrue()
      // F1's rank comes from the Day-60 snapshot (7), not the Day-40 one (3).
      assertThat(readDayOnly().mapValues { it.value.first }).containsExactly(1L to 0, 7)
    }

  @Test
  fun `an upload on the latest event date is not a backfill`() =
    runBlocking<Unit> {
      seedPriorSnapshot(
        listOf(Triple(2L to 0, 0, 90)),
        uploadName = "dataProviders/dp/rawImpressionUploads/upLatest",
        createTimeSeconds = 5L,
        maxEventDate = epochDayToDate(100),
      )
      val key = writePhase0(listOf(1L to 0))

      // Same event date as the latest snapshot: forward append, not a backfill.
      val result = ranker(maxEventDate = epochDayToDate(100)).rank(POOL, key, rankedSize = 100)

      assertThat(result.backfill).isFalse()
    }

  companion object {
    private fun pack(fps: List<Pair<Long, Int>>): ByteString {
      val bytes = ByteArray(fps.size * 12)
      fps.forEachIndexed { i, (hi, lo) ->
        EventIdDigestBytes.writeHi(bytes, i * 12, hi)
        EventIdDigestBytes.writeLo(bytes, i * 12 + 8, lo)
      }
      return ByteString.copyFrom(bytes)
    }

    private fun decode(records: List<RankIndexMap>): Map<Pair<Long, Int>, Pair<Int, Int>> {
      val out = mutableMapOf<Pair<Long, Int>, Pair<Int, Int>>()
      for (record in records) {
        val fps = record.fingerprints
        for (i in 0 until record.ranksCount) {
          val hi = EventIdDigestBytes.readHi(fps, i * 12)
          val lo = EventIdDigestBytes.readLo(fps, i * 12 + 8)
          out[hi to lo] =
            record.getRanks(i) to
              LastSeenDayBytes.read(record.lastSeenDays, i * LastSeenDayBytes.WIDTH)
        }
      }
      return out
    }

    private fun epochDayToDate(epochDay: Int): Date {
      val d = LocalDate.ofEpochDay(epochDay.toLong())
      return date {
        year = d.year
        month = d.monthValue
        day = d.dayOfMonth
      }
    }

    private fun epochDayOf(d: Date): Long = LocalDate.of(d.year, d.month, d.day).toEpochDay()

    private fun matchesParent(name: String, parent: String): Boolean =
      if (parent.endsWith("/-")) true else name.startsWith("$parent/")
  }

  /** In-memory fake of the `RankIndexBlobService` honoring parent wildcard + filters. */
  private class FakeRankIndexBlobs {
    val rows = mutableListOf<RankIndexBlob>()
    private var clock = 10L

    fun seed(row: RankIndexBlob) {
      rows.add(row)
    }

    val stub: RankIndexBlobServiceCoroutineStub = mock {
      onBlocking { listRankIndexBlobs(any(), any()) } doAnswer
        { invocation ->
          val request = invocation.getArgument<ListRankIndexBlobsRequest>(0)
          val filter = request.filter
          val matched =
            rows.filter { row ->
              matchesParent(row.name, request.parent) &&
                row.blobType == filter.blobType &&
                (filter.cmmsModelLine.isEmpty() || row.cmmsModelLine == filter.cmmsModelLine) &&
                (!filter.hasPoolOffset() || row.poolOffset == filter.poolOffset) &&
                (!filter.hasMaxEventDateOnOrBefore() ||
                  epochDayOf(row.maxEventDate) <= epochDayOf(filter.maxEventDateOnOrBefore))
            }
          listRankIndexBlobsResponse { rankIndexBlobs += matched }
        }
      onBlocking { batchCreateRankIndexBlobs(any(), any()) } doAnswer
        { invocation ->
          val request = invocation.getArgument<BatchCreateRankIndexBlobsRequest>(0)
          val created =
            request.requestsList.map { createRequest ->
              createRequest.rankIndexBlob.copy {
                name = "${request.parent}/rankIndexBlobs/${blobType.name}-$poolOffset-${clock++}"
                createTime = timestamp { seconds = clock }
              }
            }
          rows.addAll(created)
          batchCreateRankIndexBlobsResponse { rankIndexBlobs += created }
        }
      onBlocking { deleteRankIndexBlob(any(), any()) } doAnswer
        { invocation ->
          val request = invocation.getArgument<DeleteRankIndexBlobRequest>(0)
          rows.removeAll { it.name == request.name }
          rankIndexBlob {}
        }
    }
  }
}
