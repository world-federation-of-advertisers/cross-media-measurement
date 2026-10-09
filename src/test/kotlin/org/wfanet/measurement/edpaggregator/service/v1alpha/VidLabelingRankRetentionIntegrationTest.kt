/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingRankRetentionIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(initiallyVisibleModelLines = setOf(MEMOIZED_MODEL_LINE))
  ) {
  @Test
  fun `historical backfill keeps the live snapshot date`() = runBlocking {
    setToday(EVENT_DATE)
    processSingleEventUpload("history-d1", EVENT_DATE, "alice-event", "alice")

    setToday(EVENT_DATE.plusDays(2))
    val d3 = processSingleEventUpload("history-d3", EVENT_DATE.plusDays(2), "alice-event", "alice")
    val d3Snapshot = snapshotFor(d3)
    val d3Output = outputGenerations(MEMOIZED_MODEL_LINE)

    val d2 =
      processSingleEventUpload(
        "history-d2-backfill",
        EVENT_DATE.plusDays(1),
        "alice-event",
        "alice",
      )

    assertThat(
        readLabeledPeople(MEMOIZED_MODEL_LINE)
          .single { it.eventDate == EVENT_DATE.plusDays(1) }
          .personId
      )
      .isEqualTo("alice")
    assertThat(snapshotFor(d2).maxEventDate.toLocalDate()).isEqualTo(EVENT_DATE.plusDays(2))
    assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).containsAtLeastEntriesIn(d3Output)
    assertThat(d3Snapshot.hasDeleteTime()).isFalse()
    assertAvailabilityPublished(
      setOf(MEMOIZED_MODEL_LINE),
      setOf(EVENT_DATE, EVENT_DATE.plusDays(1), EVENT_DATE.plusDays(2)),
    )
  }

  @Test
  fun `expired rank is reallocated without colliding with a returning fingerprint`() = runBlocking {
    setToday(EVENT_DATE)
    val first = processSingleEventUpload("expiry-d1", EVENT_DATE, "alice-event", "alice")
    val aliceRank = rankFor(first, "alice-event", RankIndexBlob.BlobType.SNAPSHOT)

    setToday(EVENT_DATE.plusDays(1))
    val renewal =
      processSingleEventUpload("expiry-d2", EVENT_DATE.plusDays(1), "alice-event", "alice")
    assertThat(rankFor(renewal, "alice-event", RankIndexBlob.BlobType.SNAPSHOT).rank)
      .isEqualTo(aliceRank.rank)
    assertThat(rankFor(renewal, "alice-event", RankIndexBlob.BlobType.SNAPSHOT).lastSeen)
      .isEqualTo(EVENT_DATE.plusDays(1))

    val reuseDate = EVENT_DATE.plusDays(3)
    setToday(EVENT_DATE.plusDays(32))
    val candidates =
      (0 until 90).map { index ->
        RawEventFixture("reuser-event-$index", "reuser-$index", reuseDate)
      }
    val reused = processUpload("expiry-d33", candidates)
    val candidateIdsByDigest = candidates.associate { digest(it.eventId) to it.eventId }
    val reuser =
      rankEntries(reused, RankIndexBlob.BlobType.SNAPSHOT).single {
        it.poolOffset == aliceRank.poolOffset &&
          it.rank == aliceRank.rank &&
          it.digest in candidateIdsByDigest
      }
    val reuserPerson = candidates.single { digest(it.eventId) == reuser.digest }.personId
    val reuserVid = vidFor(reuseDate, reuserPerson)

    val returnDate = EVENT_DATE.plusDays(4)
    setToday(EVENT_DATE.plusDays(33))
    processSingleEventUpload("expiry-d34", returnDate, "alice-event", "alice")

    assertThat(vidFor(returnDate, "alice")).isNotEqualTo(reuserVid)
  }

  @Test
  fun `ranked pool exhaustion falls back to the unranked VID range`() = runBlocking {
    val events =
      (0 until 601).map { index ->
        RawEventFixture("overflow-event-$index", "overflow-person-$index", EVENT_DATE)
      }
    val upload = processUpload("overflow", events)

    val output = readLabeledPeople(MEMOIZED_MODEL_LINE)
    val rankedEntries = rankEntries(upload, RankIndexBlob.BlobType.DAY_ONLY)
    assertThat(output).hasSize(601)
    assertThat(rankedEntries.size).isLessThan(601)
    assertThat(output.any { Math.floorMod(it.vid, 100L) >= 90L }).isTrue()
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `missing cumulative snapshot fails without publishing a successor`() = runBlocking {
    assertBrokenPriorSnapshot(Breakage.MISSING)
  }

  @Test
  fun `corrupt cumulative snapshot fails without publishing a successor`() = runBlocking {
    assertBrokenPriorSnapshot(Breakage.CORRUPT_CIPHERTEXT)
  }

  @Test
  fun `checksum invalid cumulative snapshot fails without publishing a successor`() = runBlocking {
    assertBrokenPriorSnapshot(Breakage.CHECKSUM_MISMATCH)
  }

  private suspend fun processSingleEventUpload(
    folder: String,
    date: java.time.LocalDate,
    eventId: String,
    personId: String,
  ): RawImpressionUpload = processUpload(folder, listOf(RawEventFixture(eventId, personId, date)))

  private suspend fun processUpload(
    folder: String,
    events: List<RawEventFixture>,
  ): RawImpressionUpload {
    writeRawEvents(folder, "input.parquet", events)
    val generation = finalizeRawUpload(folder)
    awaitPipelineIdle()
    return listUploads().single { it.doneBlobGeneration == generation }
  }

  private suspend fun snapshotFor(upload: RawImpressionUpload): RankIndexBlob =
    listRankIndexBlobs(upload.name).single {
      it.cmmsModelLine == MEMOIZED_MODEL_LINE && it.blobType == RankIndexBlob.BlobType.SNAPSHOT
    }

  private suspend fun rankFor(
    upload: RawImpressionUpload,
    eventId: String,
    blobType: RankIndexBlob.BlobType,
  ): RankEntryFixture = rankEntries(upload, blobType).single { it.digest == digest(eventId) }

  private suspend fun vidFor(date: java.time.LocalDate, personId: String): Long =
    readLabeledPeople(MEMOIZED_MODEL_LINE)
      .single { it.eventDate == date && it.personId == personId }
      .vid

  private suspend fun assertBrokenPriorSnapshot(breakage: Breakage) {
    val prior = processSingleEventUpload("integrity-prior", EVENT_DATE, "prior-event", "prior")
    val snapshot = snapshotFor(prior)
    val priorRows = listRankIndexBlobs(prior.name)
    when (breakage) {
      Breakage.MISSING -> checkNotNull(mapStorage.getBlob(snapshot.blobUri)).delete()
      Breakage.CORRUPT_CIPHERTEXT ->
        mapStorage.writeBlob(snapshot.blobUri, flowOf(ByteString.copyFromUtf8("corrupt")))
      Breakage.CHECKSUM_MISMATCH ->
        RankIndexStore(mapStorage, kmsClient)
          .writeBlob(
            snapshot.blobUri,
            snapshot.encryptedDek,
            flowOf(
              rankIndexMap {
                poolOffset = snapshot.poolOffset
                rankedSize = 90
                fingerprints = ByteString.copyFrom(ByteArray(12))
                ranks += 0
                lastSeenDays = ByteString.copyFrom(byteArrayOf(0, 1))
              }
            ),
          )
    }

    val previousRankWorkItems = rankBuilderWorkItems().mapTo(mutableSetOf()) { it.name }
    setToday(EVENT_DATE.plusDays(1))
    writeRawEvents(
      "integrity-next",
      "input.parquet",
      listOf(RawEventFixture("prior-event", "prior", EVENT_DATE.plusDays(1))),
    )
    val generation = finalizeRawUpload("integrity-next")
    workItemTransport.awaitIdle(allowFailures = true)
    val next = listUploads().single { it.doneBlobGeneration == generation }
    val nextRankWorkItems = rankBuilderWorkItems().filter { it.name !in previousRankWorkItems }

    assertThat(nextRankWorkItems.map { it.state }.toSet()).containsExactly(WorkItem.State.FAILED)
    assertThat(listRankIndexBlobs(next.name)).isEmpty()
    assertThat(listAvailabilityTasks(next.name)).isEmpty()
    assertThat(listMetadata().none { it.rawImpressionUpload == next.name }).isTrue()
    assertThat(listRankIndexBlobs(prior.name)).containsExactlyElementsIn(priorRows)
  }

  private enum class Breakage {
    MISSING,
    CORRUPT_CIPHERTEXT,
    CHECKSUM_MISMATCH,
  }
}
