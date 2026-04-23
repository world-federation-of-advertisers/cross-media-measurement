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
import java.nio.channels.WritableByteChannel
import java.security.MessageDigest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.wfanet.virtualpeople.common.LabelerInput
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

private const val IMPRESSION_BATCH_SIZE = 5000
private const val LABEL_BUFFER_INIT_BYTES = 512 * 1024
private const val FEISTEL_POOL_SIZE = 10_000_000L
private const val PROGRESS_LOG_INTERVAL = 10

/**
 * Impression-batch-driven pipeline with per-file 1:1 output mapping.
 *
 * Each consumer coroutine pulls input files from a shared queue and, within a
 * single file, accumulates impressions into a batch of [IMPRESSION_BATCH_SIZE].
 * When the batch fills, it runs fingerprint -> DB lookup -> feistel -> label,
 * then appends the labeled impressions to the output writer for the source
 * file. Batches never span input files, so each input file produces exactly
 * one output file containing the labeled form of all its impressions in input
 * order.
 */
class LabelOnlyPipeline(
  private val storage: RankTableStorage,
  private val labeler: Labeler,
  private val dataProvider: String,
  private val modelRelease: String,
  private val parallelism: Int,
  private val ioParallelism: Int,
) {
  private val cpuDispatcher = Dispatchers.Default.limitedParallelism(parallelism)
  private val ioDispatcher = Dispatchers.IO.limitedParallelism(ioParallelism)

  private val digestPool =
    ThreadLocal.withInitial { MessageDigest.getInstance("SHA-256") }

  private val startMillis = System.currentTimeMillis()

  private fun timestamp(): String {
    val elapsedMs = System.currentTimeMillis() - startMillis
    val mins = elapsedMs / 60_000
    val secs = (elapsedMs / 1000) % 60
    val ms = elapsedMs % 1000
    return "%02d:%02d.%03d".format(mins, secs, ms)
  }

  private fun log(consumerIdx: Int, msg: String) {
    println("    [${timestamp()}] W$consumerIdx: $msg")
    System.out.flush()
  }

  suspend fun processDay(
    gcsIo: GcsIo,
    dayPrefix: String,
  ): LabelOnlyResult = coroutineScope {
    val fileList = gcsIo.listBlobsWithSize("$dayPrefix/impressions_")
    val totalSizeMb = fileList.sumOf { it.second } / (1024 * 1024)
    // Cap concurrent in-flight files to 2 per CPU worker to bound peak memory
    // (each consumer holds at most one file's impressions at a time).
    val numConsumers =
      minOf(parallelism * 2, fileList.size).coerceAtLeast(1)

    println(
      "  [Label-Only] ${fileList.size} files (${totalSizeMb}MB)," +
        " $numConsumers consumers, batch=$IMPRESSION_BATCH_SIZE impressions"
    )

    val workQueue = Channel<String>(Channel.UNLIMITED)
    for ((path, _) in fileList) workQueue.send(path)
    workQueue.close()

    val wallStart = System.nanoTime()

    val consumerResults =
      (0 until numConsumers)
        .map { idx ->
          async(ioDispatcher) { processConsumer(gcsIo, workQueue, idx) }
        }
        .awaitAll()

    val wallClockMs = (System.nanoTime() - wallStart) / 1_000_000

    val totalImp = consumerResults.sumOf { it.impressions.toLong() }
    val totalRanks = consumerResults.sumOf { it.ranksFound.toLong() }
    val totalFilesDone = consumerResults.sumOf { it.fileCount }
    val gcsMs = consumerResults.sumOf { it.gcsReadMs }
    val fpMs = consumerResults.sumOf { it.fingerprintMs }
    val dbMs = consumerResults.sumOf { it.dbLookupMs }
    val feistelTotalMs = consumerResults.sumOf { it.feistelMs }
    val labelMs = consumerResults.sumOf { it.labelWriteMs }
    val throughput = if (wallClockMs > 0) totalImp * 1000 / wallClockMs else 0L

    println()
    println("  [Label-Only] RECAP [${timestamp()}]")
    println("    Files:        $totalFilesDone")
    println("    Impressions:  $totalImp")
    println("    Ranks found:  $totalRanks")
    println("    Wall clock:   ${wallClockMs}ms (${wallClockMs / 1000}s)")
    println("    Throughput:   $throughput imp/sec")
    println("    Accumulated phase time (sum across $numConsumers consumers):")
    println("      GCS Read:    ${gcsMs}ms")
    println("      Fingerprint: ${fpMs}ms")
    println("      DB Lookup:   ${dbMs}ms")
    println("      Feistel:     ${feistelTotalMs}ms")
    println("      Label+Write: ${labelMs}ms")
    println()
    System.out.flush()

    LabelOnlyResult(workerResults = consumerResults, wallClockMs = wallClockMs)
  }

  private suspend fun processConsumer(
    gcsIo: GcsIo,
    workQueue: Channel<String>,
    consumerIdx: Int,
  ): LabelOnlyWorkerResult {
    var gcsReadNs = 0L
    var fingerprintNs = 0L
    var dbLookupNs = 0L
    var feistelNs = 0L
    var labelWriteNs = 0L
    var totalImpressions = 0
    var totalAccounts = 0
    var totalRanksFound = 0
    var fileCount = 0

    val impBatch = ArrayList<LabelerInput>(IMPRESSION_BATCH_SIZE)
    val fpBatch = ArrayList<SpannerByteArray>(IMPRESSION_BATCH_SIZE)
    val writeBuf = ReusableByteOutputStream(LABEL_BUFFER_INIT_BYTES)

    log(consumerIdx, "START")

    for (path in workQueue) {
      fileCount++

      val gcsStart = System.nanoTime()
      val impressions =
        gcsIo.parseImpressionsFromBytes(gcsIo.readRawBytes(path))
      gcsReadNs += System.nanoTime() - gcsStart
      totalImpressions += impressions.size
      totalAccounts += impressions.size

      val outputPath = path.replace("impressions_", "labeled_")

      gcsIo.withWriter(outputPath) { writer ->
        for (imp in impressions) {
          impBatch.add(imp)
          if (impBatch.size >= IMPRESSION_BATCH_SIZE) {
            val t = runBatch(impBatch, fpBatch, writeBuf, writer)
            fingerprintNs += t.fpNs
            dbLookupNs += t.dbNs
            feistelNs += t.feistelNs
            labelWriteNs += t.labelWriteNs
            totalRanksFound += t.ranksFound
            impBatch.clear()
            fpBatch.clear()
          }
        }
        if (impBatch.isNotEmpty()) {
          val t = runBatch(impBatch, fpBatch, writeBuf, writer)
          fingerprintNs += t.fpNs
          dbLookupNs += t.dbNs
          feistelNs += t.feistelNs
          labelWriteNs += t.labelWriteNs
          totalRanksFound += t.ranksFound
          impBatch.clear()
          fpBatch.clear()
        }
      }

      if (fileCount % PROGRESS_LOG_INTERVAL == 0) {
        log(
          consumerIdx,
          "$fileCount files, $totalImpressions imp" +
            " (ranks=$totalRanksFound)" +
            " | GCS:${gcsReadNs / 1_000_000}ms" +
            " FP:${fingerprintNs / 1_000_000}ms" +
            " DB:${dbLookupNs / 1_000_000}ms" +
            " Feistel:${feistelNs / 1_000_000}ms" +
            " Label:${labelWriteNs / 1_000_000}ms",
        )
      }
    }

    log(
      consumerIdx,
      "DONE: $fileCount files, $totalImpressions imp" +
        " | GCS:${gcsReadNs / 1_000_000}ms" +
        " FP:${fingerprintNs / 1_000_000}ms" +
        " DB:${dbLookupNs / 1_000_000}ms" +
        " Feistel:${feistelNs / 1_000_000}ms" +
        " Label:${labelWriteNs / 1_000_000}ms",
    )

    return LabelOnlyWorkerResult(
      workerIndex = consumerIdx,
      fileCount = fileCount,
      impressions = totalImpressions,
      uniqueAccounts = totalAccounts,
      ranksFound = totalRanksFound,
      gcsReadMs = gcsReadNs / 1_000_000,
      fingerprintMs = fingerprintNs / 1_000_000,
      dbLookupMs = dbLookupNs / 1_000_000,
      feistelMs = feistelNs / 1_000_000,
      labelWriteMs = labelWriteNs / 1_000_000,
    )
  }

  private suspend fun runBatch(
    impBatch: List<LabelerInput>,
    fpBatch: ArrayList<SpannerByteArray>,
    writeBuf: ReusableByteOutputStream,
    writer: WritableByteChannel,
  ): BatchTiming {
    val fpStart = System.nanoTime()
    withContext(cpuDispatcher) {
      val digest = digestPool.get()
      for (imp in impBatch) {
        fpBatch.add(
          SpannerByteArray.copyFrom(
            digest.digest(
              imp.profileInfo.emailUserInfo.userId.toByteArray(Charsets.UTF_8)
            )
          )
        )
      }
    }
    val fpNs = System.nanoTime() - fpStart

    val dbStart = System.nanoTime()
    val found = storage.lookupRankValuesStale(dataProvider, modelRelease, fpBatch)
    val dbNs = System.nanoTime() - dbStart

    var feistelDurationNs = 0L
    var labelCpuNs = 0L
    withContext(cpuDispatcher) {
      val fStart = System.nanoTime()
      for (rank in found.values) {
        feistelHash(rank, FEISTEL_POOL_SIZE)
      }
      feistelDurationNs = System.nanoTime() - fStart

      val labelStart = System.nanoTime()
      writeBuf.reset()
      for (imp in impBatch) {
        labeler.label(imp).writeDelimitedTo(writeBuf)
      }
      labelCpuNs = System.nanoTime() - labelStart
    }

    val writeStart = System.nanoTime()
    writeBuf.flushTo(writer)
    val writeNs = System.nanoTime() - writeStart

    return BatchTiming(
      fpNs = fpNs,
      dbNs = dbNs,
      feistelNs = feistelDurationNs,
      labelWriteNs = labelCpuNs + writeNs,
      ranksFound = found.size,
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

private data class BatchTiming(
  val fpNs: Long,
  val dbNs: Long,
  val feistelNs: Long,
  val labelWriteNs: Long,
  val ranksFound: Int,
)

/**
 * [ByteArrayOutputStream] that can flush its contents to a
 * [WritableByteChannel] without allocating a copy of the internal buffer.
 * Reuse across batches via [reset].
 */
private class ReusableByteOutputStream(initialCapacity: Int) :
  ByteArrayOutputStream(initialCapacity) {
  fun flushTo(channel: WritableByteChannel) {
    val buffer = ByteBuffer.wrap(buf, 0, count)
    while (buffer.hasRemaining()) {
      channel.write(buffer)
    }
  }
}
