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
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.wfanet.virtualpeople.core.labeler.Labeler

data class LabelOnlyWorkerResult(
  val workerIndex: Int,
  val fileCount: Int,
  val impressions: Int,
  val uniqueAccounts: Int,
  val ranksFound: Int,
  val gcsReadMs: Long,
  val fingerprintMs: Long,
  val dbLookupMs: Long,
  val feistelMs: Long,
  val labelWriteMs: Long,
) {
  val totalMs: Long
    get() = gcsReadMs + fingerprintMs + dbLookupMs + feistelMs + labelWriteMs
}

data class LabelOnlyResult(
  val workerResults: List<LabelOnlyWorkerResult>,
  val wallClockMs: Long,
) {
  val totalFiles get() = workerResults.sumOf { it.fileCount }
  val totalImpressions get() = workerResults.sumOf { it.impressions }
  val uniqueAccounts get() = workerResults.sumOf { it.uniqueAccounts }
  val ranksFound get() = workerResults.sumOf { it.ranksFound }
  val gcsScanMs get() = workerResults.sumOf { it.gcsReadMs }
  val fingerprintMs get() = workerResults.sumOf { it.fingerprintMs }
  val dbLookupMs get() = workerResults.sumOf { it.dbLookupMs }
  val feistelMs get() = workerResults.sumOf { it.feistelMs }
  val labelWriteMs get() = workerResults.sumOf { it.labelWriteMs }
  val totalMs get() = wallClockMs
}

private const val FP_BATCH_SIZE = 5000
private const val LABEL_FLUSH_SIZE = 1000

/**
 * Per-file pipeline with bounded memory. Each worker runs 2 coroutines, each
 * processing one file at a time. Fingerprints are batched (5K) and DB-looked-up
 * immediately then discarded. Label output streams to GCS in 1K-impression
 * chunks (~500KB) — no large in-memory buffer.
 */
class LabelOnlyPipeline(
  private val storage: RankTableStorage,
  private val labeler: Labeler,
  private val dataProvider: String,
  private val modelRelease: String,
  @Suppress("unused") private val dbLookupBatchSize: Int,
  private val parallelism: Int,
  private val dbReadParallelism: Int,
  private val ioParallelism: Int,
  @Suppress("unused") private val batchSizeBytes: Long,
) {
  private val cpuDispatcher = Dispatchers.Default.limitedParallelism(parallelism)
  private val ioDispatcher = Dispatchers.IO.limitedParallelism(ioParallelism)

  private val digestPool =
    ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

  private fun log(workerIdx: Int, msg: String) {
    println("    W$workerIdx: $msg")
    System.out.flush()
  }

  suspend fun processDay(
    gcsIo: GcsIo,
    dayPrefix: String,
  ): LabelOnlyResult = coroutineScope {
    val fileList = gcsIo.listBlobsWithSize("$dayPrefix/impressions_")
    val totalSizeMb = fileList.sumOf { it.second } / (1024 * 1024)
    val numWorkers = parallelism
    val filesPerWorker = (fileList.size + numWorkers - 1) / numWorkers
    val perWorkerFiles = fileList.chunked(filesPerWorker).filter { it.isNotEmpty() }
    val actualWorkers = perWorkerFiles.size
    val coroutinesPerWorker = 2

    println(
      "  [Label-Only] ${fileList.size} files (${totalSizeMb}MB)," +
        " $actualWorkers workers x $coroutinesPerWorker coroutines/worker" +
        " (${actualWorkers * coroutinesPerWorker} total)"
    )

    val wallStart = System.nanoTime()

    val workerResults = perWorkerFiles.mapIndexed { workerIdx, workerFiles ->
      async(cpuDispatcher) {
        processWorker(gcsIo, workerFiles, workerIdx, coroutinesPerWorker)
      }
    }.awaitAll()

    val wallClockMs = (System.nanoTime() - wallStart) / 1_000_000

    LabelOnlyResult(
      workerResults = workerResults,
      wallClockMs = wallClockMs,
    )
  }

  private suspend fun processWorker(
    gcsIo: GcsIo,
    files: List<Pair<String, Long>>,
    workerIdx: Int,
    coroutinesPerWorker: Int,
  ): LabelOnlyWorkerResult = coroutineScope {
    val gcsReadNs = AtomicLong(0)
    val fingerprintNs = AtomicLong(0)
    val dbLookupNs = AtomicLong(0)
    val labelWriteNs = AtomicLong(0)
    val totalImpressions = AtomicInteger(0)
    val totalAccounts = AtomicInteger(0)
    val totalRanksFound = AtomicInteger(0)
    val filesCompleted = AtomicInteger(0)

    val totalFiles = files.size
    val logInterval = maxOf(1, totalFiles / 20)

    val workQueue = Channel<Pair<String, Long>>(Channel.UNLIMITED)
    for (file in files) workQueue.send(file)
    workQueue.close()

    log(workerIdx, "Starting: $totalFiles files, $coroutinesPerWorker coroutines")

    val jobs = (0 until coroutinesPerWorker).map {
      launch(ioDispatcher) {
        val fpBatch = ArrayList<SpannerByteArray>(FP_BATCH_SIZE)

        for ((path, _) in workQueue) {

          // --- Phase 1: Download + Parse ---
          val gcsStartNs = System.nanoTime()
          val raw = gcsIo.readRawBytes(path)
          val impressions = gcsIo.parseImpressionsFromBytes(raw)
          gcsReadNs.addAndGet(System.nanoTime() - gcsStartNs)
          totalImpressions.addAndGet(impressions.size)

          // --- Phase 2+3: Fingerprint in batches of 5K, DB lookup each batch immediately ---
          var fpNs = 0L
          var dbNs = 0L
          var chunkStartNs = System.nanoTime()
          for (imp in impressions) {
            val digest = digestPool.get()
            digest.reset()
            fpBatch.add(
              SpannerByteArray.copyFrom(
                digest.digest(
                  imp.profileInfo.emailUserInfo.userId.toByteArray(Charsets.UTF_8)
                )
              )
            )
            if (fpBatch.size >= FP_BATCH_SIZE) {
              fpNs += System.nanoTime() - chunkStartNs
              val dbStartNs = System.nanoTime()
              val found = storage.lookupRankValuesStale(
                dataProvider, modelRelease, fpBatch,
              )
              totalRanksFound.addAndGet(found.size)
              dbNs += System.nanoTime() - dbStartNs
              fpBatch.clear()
              chunkStartNs = System.nanoTime()
            }
          }
          fpNs += System.nanoTime() - chunkStartNs
          if (fpBatch.isNotEmpty()) {
            val dbStartNs = System.nanoTime()
            val found = storage.lookupRankValuesStale(
              dataProvider, modelRelease, fpBatch,
            )
            totalRanksFound.addAndGet(found.size)
            dbNs += System.nanoTime() - dbStartNs
            fpBatch.clear()
          }
          fingerprintNs.addAndGet(fpNs)
          dbLookupNs.addAndGet(dbNs)
          totalAccounts.addAndGet(impressions.size)

          // --- Phase 4: Label + stream to GCS ---
          val labelStartNs = System.nanoTime()
          val outputPath = path.replace("impressions_", "labeled_")

          withContext(cpuDispatcher) {
            gcsIo.withWriter(outputPath) { writer ->
              val buf = ByteArrayOutputStream(LABEL_FLUSH_SIZE * 512)
              var count = 0
              for (input in impressions) {
                labeler.label(input).writeDelimitedTo(buf)
                count++
                if (count >= LABEL_FLUSH_SIZE) {
                  writer.write(ByteBuffer.wrap(buf.toByteArray()))
                  buf.reset()
                  count = 0
                }
              }
              if (buf.size() > 0) {
                writer.write(ByteBuffer.wrap(buf.toByteArray()))
              }
            }
          }
          labelWriteNs.addAndGet(System.nanoTime() - labelStartNs)

          // --- Progress ---
          val done = filesCompleted.incrementAndGet()
          if (done % logInterval == 0 || done == totalFiles) {
            log(workerIdx, "$done/$totalFiles files" +
              " (${totalImpressions.get()} imp," +
              " ${totalRanksFound.get()} ranks)")
          }
        }
      }
    }

    jobs.forEach { it.join() }

    // Feistel measurement (post-pipeline, benchmark only)
    val ranksFound = totalRanksFound.get()
    val feistelStartNs = System.nanoTime()
    for (i in 0 until ranksFound) {
      feistelHash(i.toLong(), 10_000_000L)
    }
    val feistelNs = System.nanoTime() - feistelStartNs

    log(workerIdx, "DONE: $totalFiles files, ${totalImpressions.get()} imp" +
      " | GCS:${gcsReadNs.get() / 1_000_000}ms" +
      " FP:${fingerprintNs.get() / 1_000_000}ms" +
      " DB:${dbLookupNs.get() / 1_000_000}ms" +
      " Label:${labelWriteNs.get() / 1_000_000}ms" +
      " Feistel:${feistelNs / 1_000_000}ms")

    LabelOnlyWorkerResult(
      workerIndex = workerIdx,
      fileCount = files.size,
      impressions = totalImpressions.get(),
      uniqueAccounts = totalAccounts.get(),
      ranksFound = ranksFound,
      gcsReadMs = gcsReadNs.get() / 1_000_000,
      fingerprintMs = fingerprintNs.get() / 1_000_000,
      dbLookupMs = dbLookupNs.get() / 1_000_000,
      feistelMs = feistelNs / 1_000_000,
      labelWriteMs = labelWriteNs.get() / 1_000_000,
    )
  }

  companion object {
    fun feistelHash(rankValue: Long, poolSize: Long): Long {
      val halfBits = 32
      var left = (rankValue ushr halfBits).toInt()
      var right = rankValue.toInt()
      for (round in 0 until 4) {
        val f = ((right.toLong() * 2654435761L + round + 0xDEADBEEFL) ushr 16).toInt()
        val newRight = left xor f
        left = right
        right = newRight
      }
      return ((left.toLong() and 0xFFFFFFFFL) shl halfBits) or
        (right.toLong() and 0xFFFFFFFFL)
    }
  }
}
