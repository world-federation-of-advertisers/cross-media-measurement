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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.cloud.ByteArray as SpannerByteArray
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry
import org.wfanet.virtualpeople.core.labeler.Labeler

data class RankOnlyBatchResult(
  val batchIndex: Int,
  val fileCount: Int,
  val uniqueAccounts: Int,
  val newAccounts: Int,
  val ranksWritten: Int,
  val readGcsMs: Long,
  val fingerprintMs: Long,
  val bulkLookupMs: Long,
  val pass1Ms: Long,
  val rankAllocMs: Long,
  val writeRanksMs: Long,
) {
  val totalMs: Long
    get() = readGcsMs + fingerprintMs + bulkLookupMs + pass1Ms + rankAllocMs + writeRanksMs
}

class RankOnlyPipeline(
  private val storage: RankTableStorage,
  private val labeler: Labeler,
  private val dataProvider: String,
  private val modelRelease: String,
  private val numPools: Int,
  private val dbLookupBatchSize: Int,
  private val dbWriteBatchSize: Int,
  private val parallelism: Int,
  private val dbReadParallelism: Int,
  private val dbWriteParallelism: Int,
  private val ioParallelism: Int,
  private val batchSizeBytes: Long,
) {
  private val cpuDispatcher = Dispatchers.Default.limitedParallelism(parallelism)
  private val dbReadDispatcher = Dispatchers.IO.limitedParallelism(dbReadParallelism)
  private val dbWriteDispatcher = Dispatchers.IO.limitedParallelism(dbWriteParallelism)
  private val ioDispatcher = Dispatchers.IO.limitedParallelism(ioParallelism)
  private val digestPool =
    ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

  suspend fun initializePoolCounters() {
    for (i in 0 until numPools) {
      storage.initializePoolCounter(dataProvider, modelRelease, "pool-$i", 10_000_000)
    }
    println("Pool counters initialized ($numPools pools).")
  }

  suspend fun processDay(
    gcsIo: GcsIo,
    dayPrefix: String,
  ): List<RankOnlyBatchResult> = coroutineScope {
    val fileList = gcsIo.listBlobsWithSize("$dayPrefix/impressions_")
    val batches = groupIntoBatches(fileList, batchSizeBytes)
    val totalSizeMb = fileList.sumOf { it.second } / (1024 * 1024)

    println(
      "  [Rank-Only] ${fileList.size} files (${totalSizeMb}MB)," +
        " ${batches.size} batches (~${batchSizeBytes / (1024 * 1024)}MB target)"
    )

    val results = mutableListOf<RankOnlyBatchResult>()

    for ((batchIndex, batchFiles) in batches.withIndex()) {
      val filePaths = batchFiles.map { it.first }
      val batchMb = batchFiles.sumOf { it.second } / (1024 * 1024)
      println(
        "  --- Batch ${batchIndex + 1}/${batches.size}:" +
          " ${filePaths.size} files (${batchMb}MB) ---"
      )

      val result = processBatch(gcsIo, filePaths, batchIndex)
      results.add(result)
      println(
        "    [Batch ${batchIndex + 1}] Done: ${result.newAccounts} new," +
          " ${result.ranksWritten} ranked in ${result.totalMs}ms"
      )
    }

    results
  }

  private suspend fun processBatch(
    gcsIo: GcsIo,
    filePaths: List<String>,
    batchIndex: Int,
  ): RankOnlyBatchResult {
    // --- Read GCS files and extract only unique account IDs ---
    // Protos are discarded immediately to avoid holding ~20GB in memory.
    val uniqueAccountIds = ConcurrentHashMap.newKeySet<String>()
    val filesRead = AtomicInteger(0)

    val readGcsMs = timedMs {
      for (window in filePaths.chunked(ioParallelism)) {
        coroutineScope {
          window
            .map { path ->
              async(ioDispatcher) {
                val impressions = gcsIo.readImpressions(path)
                for (input in impressions) {
                  uniqueAccountIds.add(input.profileInfo.emailUserInfo.userId)
                }
                val done = filesRead.incrementAndGet()
                if (done % 10 == 0 || done == filePaths.size) {
                  print(
                    "\r    GCS Scan: $done/${filePaths.size} files" +
                      " (${uniqueAccountIds.size} unique)      "
                  )
                  System.out.flush()
                }
              }
            }
            .awaitAll()
        }
      }
      if (filePaths.isNotEmpty()) println()
    }
    println(
      "    [GCS Scan] ${uniqueAccountIds.size} unique accounts in ${readGcsMs}ms"
    )

    // --- Fingerprint, skip already-processed from prior batches ---
    val accountFingerprints = ConcurrentHashMap<String, SpannerByteArray>()
    val accountList = uniqueAccountIds.toList()
    val fpChunks = accountList.chunked(maxOf(1, accountList.size / parallelism))

    val fingerprintMs = timedMs {
      coroutineScope {
        fpChunks
          .map { chunk ->
            async(cpuDispatcher) {
              for (userId in chunk) {
                val digest = digestPool.get()
                digest.reset()
                accountFingerprints[userId] =
                  SpannerByteArray.copyFrom(
                    digest.digest(userId.toByteArray(Charsets.UTF_8))
                  )
              }
            }
          }
          .awaitAll()
      }
    }
    println(
      "    [Fingerprints] ${accountFingerprints.size} in ${fingerprintMs}ms"
    )

    if (accountFingerprints.isEmpty()) {
      return RankOnlyBatchResult(
        batchIndex, filePaths.size, uniqueAccountIds.size,
        0, 0, readGcsMs, fingerprintMs, 0, 0, 0, 0,
      )
    }

    // --- Bulk Lookup ---
    val allFps = accountFingerprints.values.toList()
    val knownFps = ConcurrentHashMap.newKeySet<SpannerByteArray>()
    val lookupBatches = allFps.chunked(dbLookupBatchSize)
    val completedLookups = AtomicInteger(0)

    val bulkLookupMs = timedMs {
      for (window in lookupBatches.chunked(dbReadParallelism)) {
        coroutineScope {
          window
            .map { batch ->
              async(dbReadDispatcher) {
                val found =
                  storage.lookupKnownFingerprints(
                    dataProvider, modelRelease, batch,
                  )
                knownFps.addAll(found)
                val done = completedLookups.incrementAndGet()
                if (done % 1 == 0 || done == lookupBatches.size) {
                  print(
                    "\r    Bulk Lookup: $done/${lookupBatches.size}" +
                      " (${knownFps.size} known)      "
                  )
                  System.out.flush()
                }
              }
            }
            .awaitAll()
        }
      }
      if (lookupBatches.isNotEmpty()) println()
    }
    println(
      "    [Bulk Lookup] ${knownFps.size} known," +
        " ${allFps.size - knownFps.size} new in ${bulkLookupMs}ms"
    )

    val newAccountIds =
      accountFingerprints.entries
        .filter { (_, fp) -> fp !in knownFps }
        .map { (userId, _) -> userId }

    if (newAccountIds.isEmpty()) {
      return RankOnlyBatchResult(
        batchIndex, filePaths.size, uniqueAccountIds.size,
        0, 0, readGcsMs, fingerprintMs, bulkLookupMs, 0, 0, 0,
      )
    }

    // --- Pass 1: Label new accounts for pool assignment ---
    val poolAssignments = ConcurrentHashMap<String, String>()
    val completedLabels = AtomicInteger(0)

    val pass1Ms = timedMs {
      val chunks =
        newAccountIds.chunked(maxOf(1, newAccountIds.size / parallelism))
      coroutineScope {
        chunks
          .map { chunk ->
            async(cpuDispatcher) {
              for (userId in chunk) {
                val accountIndex = userId.removePrefix("user_").toInt()
                val input =
                  DataGenerator.buildLabelerInput(userId, accountIndex)
                val output = labeler.label(input)
                val vid =
                  if (output.peopleList.isNotEmpty()) {
                    output.peopleList[0].virtualPersonId
                  } else {
                    0L
                  }
                poolAssignments[userId] =
                  "pool-${Math.abs(vid.hashCode()) % numPools}"
                val done = completedLabels.incrementAndGet()
                if (done % 100_000 == 0 || done == newAccountIds.size) {
                  print(
                    "\r    Pass 1: $done/${newAccountIds.size}" +
                      " (${done.toLong() * 100 / newAccountIds.size}%)      "
                  )
                  System.out.flush()
                }
              }
            }
          }
          .awaitAll()
      }
      println()
    }
    println("    [Pass 1] ${newAccountIds.size} labels in ${pass1Ms}ms")

    // --- Rank Allocation ---
    val poolGroups =
      poolAssignments.entries.groupBy({ it.value }, { it.key })
    val poolToStartRank = mutableMapOf<String, Long>()

    val rankAllocMs = timedMs {
      for ((poolId, userIds) in poolGroups) {
        poolToStartRank[poolId] =
          storage.allocateRanks(
            dataProvider, modelRelease, poolId, userIds.size,
          )
      }
    }
    println("    [Rank Alloc] ${poolGroups.size} pools in ${rankAllocMs}ms")

    // --- Write Ranks ---
    val newRankEntries = mutableListOf<RankEntry>()
    for ((poolId, userIds) in poolGroups) {
      val startRank = poolToStartRank[poolId]!!
      userIds.forEachIndexed { index, userId ->
        newRankEntries.add(
          RankEntry(
            dataProviderResourceId = dataProvider,
            modelRelease = modelRelease,
            encryptedFingerprint = accountFingerprints[userId]!!,
            poolId = poolId,
            rankValue = startRank + index,
          )
        )
      }
    }

    val writeBatches = newRankEntries.chunked(dbWriteBatchSize)
    val completedWrites = AtomicInteger(0)

    val writeRanksMs = timedMs {
      for (window in writeBatches.chunked(dbWriteParallelism)) {
        coroutineScope {
          window
            .map { batch ->
              async(dbWriteDispatcher) {
                storage.writeRanks(batch)
                val done = completedWrites.incrementAndGet()
                if (done % 1 == 0 || done == writeBatches.size) {
                  print(
                    "\r    Write Ranks: $done/${writeBatches.size}      "
                  )
                  System.out.flush()
                }
              }
            }
            .awaitAll()
        }
      }
      if (writeBatches.isNotEmpty()) println()
    }
    println(
      "    [Write Ranks] ${newRankEntries.size} entries in ${writeRanksMs}ms"
    )

    return RankOnlyBatchResult(
      batchIndex = batchIndex,
      fileCount = filePaths.size,
      uniqueAccounts = uniqueAccountIds.size,
      newAccounts = newAccountIds.size,
      ranksWritten = newRankEntries.size,
      readGcsMs = readGcsMs,
      fingerprintMs = fingerprintMs,
      bulkLookupMs = bulkLookupMs,
      pass1Ms = pass1Ms,
      rankAllocMs = rankAllocMs,
      writeRanksMs = writeRanksMs,
    )
  }

  companion object {
    fun groupIntoBatches(
      files: List<Pair<String, Long>>,
      maxBatchBytes: Long,
    ): List<List<Pair<String, Long>>> {
      val batches = mutableListOf<List<Pair<String, Long>>>()
      var currentBatch = mutableListOf<Pair<String, Long>>()
      var currentSize = 0L

      for (file in files) {
        if (currentSize + file.second > maxBatchBytes &&
          currentBatch.isNotEmpty()
        ) {
          batches.add(currentBatch)
          currentBatch = mutableListOf()
          currentSize = 0L
        }
        currentBatch.add(file)
        currentSize += file.second
      }
      if (currentBatch.isNotEmpty()) {
        batches.add(currentBatch)
      }
      return batches
    }
  }
}

private suspend inline fun timedMs(block: suspend () -> Unit): Long {
  val startNs = System.nanoTime()
  block()
  return (System.nanoTime() - startNs) / 1_000_000
}
