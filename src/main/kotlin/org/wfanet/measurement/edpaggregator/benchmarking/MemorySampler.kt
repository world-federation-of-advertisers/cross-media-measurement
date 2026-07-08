/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.benchmarking

import java.io.File
import java.lang.management.ManagementFactory
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * BENCHMARK-ONLY (throwaway) in-process memory + step-timing sampler.
 *
 * Emits two log families that the benchmark tooling parses offline:
 * * `BENCHMEM ...` every [intervalMillis] ms: heap/non-heap/RSS/GC keyed by the current step.
 * * `BENCHSTEP ...` on each [setStep]: the previous step name and its wall-clock duration.
 *
 * Safe inside a Confidential Space TEE: reads only in-process JMX beans + /proc/self/status; no
 * profiler attach or SSH needed. One mutable step per VM (each TEE app processes one WorkItem at a
 * time), tagged with the GCE instance id so autoscaled VMs stay distinguishable in the logs.
 */
object MemorySampler {
  private val logger = Logger.getLogger(MemorySampler::class.java.name)
  private val memoryBean = ManagementFactory.getMemoryMXBean()
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans()
  private val instanceId: String =
    (System.getenv("HOSTNAME") ?: System.getenv("GCE_INSTANCE"))?.takeIf { it.isNotEmpty() }
      ?: "unknown"

  @Volatile private var component: String = "unknown"
  @Volatile private var step: String = "init"
  @Volatile private var stepStartMs: Long = 0L
  @Volatile private var started: Boolean = false

  @Synchronized
  fun start(component: String, intervalMillis: Long = 5_000L) {
    if (started) return
    started = true
    this.component = component
    stepStartMs = System.currentTimeMillis()
    thread(isDaemon = true, name = "bench-mem-sampler") {
      while (true) {
        try {
          sample()
          Thread.sleep(intervalMillis)
        } catch (e: InterruptedException) {
          break
        } catch (e: Throwable) {
          logger.warning("BENCH sampler error: ${e.message}")
        }
      }
    }
    logger.info("BENCH sampler started component=$component instance=$instanceId")
  }

  @Synchronized
  fun setStep(newStep: String) {
    val now = System.currentTimeMillis()
    logger.info(
      "BENCHSTEP component=$component instance=$instanceId prevStep=$step " +
        "prevDurationMs=${now - stepStartMs} newStep=$newStep"
    )
    step = newStep
    stepStartMs = now
    sample()
  }

  fun sample() {
    val heap = memoryBean.heapMemoryUsage
    val nonHeap = memoryBean.nonHeapMemoryUsage
    val rssMb = readRssBytes() / MB
    val gcMs = gcBeans.sumOf { it.collectionTime }
    logger.info(
      "BENCHMEM component=$component instance=$instanceId step=$step " +
        "heapUsedMb=${heap.used / MB} heapCommittedMb=${heap.committed / MB} " +
        "heapMaxMb=${heap.max / MB} nonHeapMb=${nonHeap.used / MB} rssMb=$rssMb gcTimeMs=$gcMs"
    )
  }

  private fun readRssBytes(): Long =
    try {
      File("/proc/self/status").useLines { lines ->
        lines
          .firstOrNull { it.startsWith("VmRSS:") }
          ?.let { Regex("(\\d+)").find(it)?.value?.toLong()?.times(1024L) } ?: -1L
      }
    } catch (e: Exception) {
      -1L
    }

  private const val MB: Long = 1024L * 1024L
}
