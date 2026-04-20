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
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankEntry
import org.wfanet.virtualpeople.core.labeler.Labeler

data class DayResult(
  val readGcsMs: Long,
  val readGcsFileCount: Int,
  val fingerprintMs: Long,
  val fingerprintCount: Int,
  val bulkLookupMs: Long,
  val lookupCount: Int,
  val pass1Ms: Long,
  val pass1Count: Int,
  val rankAllocMs: Long,
  val rankAllocPoolCount: Int,
  val writeRanksMs: Long,
  val writeCount: Int,
  val pass2Ms: Long,
  val pass2Count: Int,
  val pass2CacheHits: Int,
  val pass2CacheMisses: Int,
  val writeGcsMs: Long,
  val writeGcsFileCount: Int,
) {
  val totalMs: Long
    get() =
      readGcsMs +
        fingerprintMs +
        bulkLookupMs +
        pass1Ms +
        rankAllocMs +
        writeRanksMs +
        pass2Ms +
        writeGcsMs
}

class Pipeline(
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

  suspend fun runDay(dayData: DayData, gcsIo: GcsIo, dayGcsPrefix: String): DayResult {

    // =========================================================================
    // STAGE 1: Account Discovery (streaming GCS scan, no impressions stored)
    // =========================================================================
    println("  +-- STAGE 1: Account Discovery --+")

    // --- 1a: Read GCS files, extract unique accounts ---
    val impressionFiles = gcsIo.listBlobs("$dayGcsPrefix/impressions_")
    val uniqueAccountIds = ConcurrentHashMap.newKeySet<String>()
    val totalImpressionsScanned = AtomicInteger(0)
    val filesScanned = AtomicInteger(0)

    println("    [GCS Scan] Reading ${impressionFiles.size} files ($ioParallelism concurrent)...")
    val readGcsMs = timed {
      for (window in impressionFiles.chunked(ioParallelism)) {
        coroutineScope {
          window.map { filePath ->
            async(ioDispatcher) {
              val impressions = gcsIo.readImpressions(filePath)
              for (input in impressions) {
                uniqueAccountIds.add(input.profileInfo.emailUserInfo.userId)
              }
              totalImpressionsScanned.addAndGet(impressions.size)
              val done = filesScanned.incrementAndGet()
              if (done % 10 == 0 || done == impressionFiles.size) {
                print(
                  "\r    GCS Scan: $done/${impressionFiles.size} files" +
                    " (${totalImpressionsScanned.get()} impressions," +
                    " ${uniqueAccountIds.size} unique)      "
                )
                System.out.flush()
              }
            }
          }.awaitAll()
        }
      }
      if (impressionFiles.isNotEmpty()) println()
    }
    println(
      "    [GCS Scan] Done: ${uniqueAccountIds.size} unique accounts" +
        " from ${totalImpressionsScanned.get()} impressions in ${readGcsMs}ms"
    )

    // --- 1b: Compute fingerprints (parallel, ThreadLocal SHA-256) ---
    val allAccountIds = uniqueAccountIds.toList()
    val accountFingerprints = ConcurrentHashMap<String, SpannerByteArray>(allAccountIds.size)
    val completedFingerprints = AtomicInteger(0)
    val fpChunks = allAccountIds.chunked(maxOf(1, allAccountIds.size / parallelism))

    println(
      "    [Fingerprints] ${allAccountIds.size} accounts" +
        " ($parallelism workers, ThreadLocal SHA-256)..."
    )
    val fingerprintMs = timed {
      coroutineScope {
        fpChunks.map { chunk ->
          async(cpuDispatcher) {
            for (userId in chunk) {
              val digest = digestPool.get()
              digest.reset()
              accountFingerprints[userId] =
                SpannerByteArray.copyFrom(
                  digest.digest(userId.toByteArray(Charsets.UTF_8))
                )
              val done = completedFingerprints.incrementAndGet()
              if (done % 500_000 == 0 || done == allAccountIds.size) {
                print(
                  "\r    Fingerprints: $done/${allAccountIds.size}" +
                    " (${done.toLong() * 100 / allAccountIds.size}%)      "
                )
                System.out.flush()
              }
            }
          }
        }.awaitAll()
      }
      println()
    }
    println("    [Fingerprints] Done: ${allAccountIds.size} in ${fingerprintMs}ms")

    // =========================================================================
    // STAGE 2: Rank Resolution (strict sequential dependency chain)
    // =========================================================================
    println("  +-- STAGE 2: Rank Resolution --+")

    // --- 2a: Bulk Lookup (via storage backend) ---
    val knownFingerprints = ConcurrentHashMap.newKeySet<SpannerByteArray>()
    val allFingerprints = accountFingerprints.values.toList()
    val lookupBatches = allFingerprints.chunked(dbLookupBatchSize)

    println(
      "    [Bulk Lookup] ${lookupBatches.size} batches of $dbLookupBatchSize" +
        " ($dbReadParallelism concurrent)"
    )
    val bulkLookupMs = timed {
      val completedBatches = AtomicInteger(0)
      for (window in lookupBatches.chunked(dbReadParallelism)) {
        coroutineScope {
          window.map { batch ->
            async(dbReadDispatcher) {
              retryWithTimeout("Bulk Lookup") {
                val found =
                  storage.lookupKnownFingerprints(dataProvider, modelRelease, batch)
                for (fp in batch) {
                  if (fp in found) knownFingerprints.add(fp)
                }
              }
              val done = completedBatches.incrementAndGet()
              print(
                "\r    Bulk Lookup: $done/${lookupBatches.size} batches" +
                  " (${knownFingerprints.size} found)      "
              )
              System.out.flush()
            }
          }.awaitAll()
        }
      }
      if (lookupBatches.isNotEmpty()) println()
    }
    println(
      "    [Bulk Lookup] Done: ${knownFingerprints.size} known accounts in ${bulkLookupMs}ms"
    )

    // --- 2b: Split known vs new ---
    val newAccountIds =
      allAccountIds.filter { userId ->
        accountFingerprints[userId]!! !in knownFingerprints
      }
    println(
      "    [Split] ${knownFingerprints.size} known, ${newAccountIds.size} new accounts"
    )

    // --- 2c: Pass 1 — Label NEW accounts for pool assignment (parallel) ---
    val newAccountPoolAssignments = ConcurrentHashMap<String, String>()

    println(
      "    [Pass 1] Labeling ${newAccountIds.size} new accounts" +
        " ($parallelism workers)..."
    )
    val pass1Ms = timed {
      if (newAccountIds.isNotEmpty()) {
        val chunks =
          newAccountIds.chunked(maxOf(1, newAccountIds.size / parallelism))
        val completedLabels = AtomicInteger(0)
        coroutineScope {
          chunks.map { chunk ->
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
                newAccountPoolAssignments[userId] =
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
          }.awaitAll()
        }
        println()
      }
    }
    println("    [Pass 1] Done: ${newAccountIds.size} labels in ${pass1Ms}ms")

    // --- 2d: Rank Allocation (SEQUENTIAL per pool — no parallelism) ---
    val poolGroups =
      newAccountPoolAssignments.entries.groupBy({ it.value }, { it.key })
    val poolToStartRank = mutableMapOf<String, Long>()

    println(
      "    [Rank Alloc] ${poolGroups.size} pools (sequential, atomic counter)..."
    )
    val rankAllocMs = timed {
      var poolsDone = 0
      for ((poolId, userIds) in poolGroups) {
        poolToStartRank[poolId] =
          storage.allocateRanks(dataProvider, modelRelease, poolId, userIds.size)
        poolsDone++
        println(
          "    Rank Alloc: $poolsDone/${poolGroups.size}" +
            " -- $poolId: ${userIds.size} ranks (start=${poolToStartRank[poolId]})"
        )
      }
    }
    println("    [Rank Alloc] Done: ${poolGroups.size} pools in ${rankAllocMs}ms")

    // --- 2e: Write Ranks (blind mutations, parallel, non-overlapping ranges) ---
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

    println(
      "    [Write Ranks] ${writeBatches.size} batches of $dbWriteBatchSize" +
        " ($dbWriteParallelism concurrent, blind mutations)"
    )
    val writeRanksMs = timed {
      val completedBatches = AtomicInteger(0)
      for (window in writeBatches.chunked(dbWriteParallelism)) {
        coroutineScope {
          window.map { batch ->
            async(dbWriteDispatcher) {
              retryWithTimeout("Write Ranks") {
                storage.writeRanks(batch)
              }
              val done = completedBatches.incrementAndGet()
              print(
                "\r    Write Ranks: $done/${writeBatches.size} batches" +
                  " (${done * dbWriteBatchSize} entries)      "
              )
              System.out.flush()
            }
          }.awaitAll()
        }
      }
      if (writeBatches.isNotEmpty()) println()
    }
    val writeRankCount = newRankEntries.size
    println(
      "    [Write Ranks] Done: $writeRankCount entries in ${writeRanksMs}ms"
    )

    // Free Stage 1/2 data structures before Stage 3 to reduce memory pressure.
    // These maps hold millions of entries (~4GB+) that are no longer needed.
    val lookupCount = allAccountIds.size
    val pass1Count = newAccountIds.size
    val rankAllocPoolCount = poolGroups.size
    accountFingerprints.clear()
    knownFingerprints.clear()
    newAccountPoolAssignments.clear()
    newRankEntries.clear()
    @Suppress("UNUSED_VALUE")
    uniqueAccountIds.clear()
    println("    [Memory] Cleared Stage 1/2 data structures")

    // =========================================================================
    // STAGE 3: Streaming Label + Output
    //   Re-read GCS -> label every impression -> write output (parallel)
    //   Memory: O(file_size) per concurrent worker, NOT O(all_impressions)
    // =========================================================================
    println("  +-- STAGE 3: Streaming Label + Output --+")

    val pass2ImpressionCount = AtomicInteger(0)
    val outputFilesWritten = AtomicInteger(0)

    println(
      "    [Pass 2 + Write] Streaming ${impressionFiles.size} files" +
        " ($ioParallelism concurrent)"
    )

    val pass2Ms = timed {
      for (window in impressionFiles.chunked(ioParallelism)) {
        coroutineScope {
          window.map { filePath ->
            async(ioDispatcher) {
              val impressions = gcsIo.readImpressions(filePath)

              val baos = java.io.ByteArrayOutputStream(256)
              val serializedOutputs =
                impressions.map { input ->
                  baos.reset()
                  labeler.label(input).writeDelimitedTo(baos)
                  baos.toByteArray()
                }

              val outputPath = filePath.replace("impressions_", "labeled_")
              gcsIo.writeLabeledEventsRaw(outputPath, serializedOutputs)

              pass2ImpressionCount.addAndGet(impressions.size)
              val filesDone = outputFilesWritten.incrementAndGet()
              if (filesDone % 5 == 0 || filesDone == impressionFiles.size) {
                print(
                  "\r    Pass 2 + Write: $filesDone/${impressionFiles.size} files" +
                    " (${pass2ImpressionCount.get()} impressions)      "
                )
                System.out.flush()
              }
            }
          }.awaitAll()
        }
      }
      if (impressionFiles.isNotEmpty()) println()
    }

    val dayTotalImpressions = pass2ImpressionCount.get()

    println(
      "    [Pass 2 + Write] Done: $dayTotalImpressions impressions in ${pass2Ms}ms"
    )

    return DayResult(
      readGcsMs = readGcsMs,
      readGcsFileCount = impressionFiles.size,
      fingerprintMs = fingerprintMs,
      fingerprintCount = lookupCount,
      bulkLookupMs = bulkLookupMs,
      lookupCount = lookupCount,
      pass1Ms = pass1Ms,
      pass1Count = pass1Count,
      rankAllocMs = rankAllocMs,
      rankAllocPoolCount = rankAllocPoolCount,
      writeRanksMs = writeRanksMs,
      writeCount = writeRankCount,
      pass2Ms = pass2Ms,
      pass2Count = dayTotalImpressions,
      pass2CacheHits = 0,
      pass2CacheMisses = 0,
      writeGcsMs = 0,
      writeGcsFileCount = outputFilesWritten.get(),
    )
  }
}

private suspend inline fun timed(block: suspend () -> Unit): Long {
  val startNs = System.nanoTime()
  block()
  return (System.nanoTime() - startNs) / 1_000_000
}

private const val DB_TIMEOUT_MS = 300_000L
private const val MAX_RETRIES = 3

private suspend inline fun retryWithTimeout(
  phase: String,
  crossinline block: suspend () -> Unit,
) {
  for (attempt in 1..MAX_RETRIES) {
    try {
      withTimeout(DB_TIMEOUT_MS) { block() }
      return
    } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
      if (attempt < MAX_RETRIES) {
        println(
          "\n    [$phase] Timeout on attempt $attempt/$MAX_RETRIES, retrying..."
        )
      } else {
        println(
          "\n    [$phase] ERROR: timed out after $MAX_RETRIES attempts"
        )
        throw e
      }
    } catch (e: Exception) {
      println(
        "\n    [$phase] ERROR: ${e.javaClass.simpleName}: ${e.message}"
      )
      throw e
    }
  }
}
