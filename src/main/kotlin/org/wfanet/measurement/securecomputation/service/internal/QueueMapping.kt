/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.service.internal

import com.google.common.hash.Hashing
import org.wfanet.measurement.config.securecomputation.QueuesConfig

class QueueMapping(config: QueuesConfig) {
  data class Queue(
    val queueId: Long,
    val queueResourceId: String,
    val deadLetterQueueResourceId: String,
    val maxWorkItemAttempts: Int,
  )

  /** Queues sorted by resource ID. */
  val queues: List<Queue> =
    buildList {
        for (queue in config.queueInfosList) {
          check(QUEUE_RESOURCE_ID_REGEX.matches(queue.queueResourceId)) {
            "Invalid queue resource ID ${queue.queueResourceId}"
          }
          val deadLetterQueueResourceId =
            queue.deadLetterQueueResourceId.ifEmpty { "${queue.queueResourceId}-dlq" }
          check(QUEUE_RESOURCE_ID_REGEX.matches(deadLetterQueueResourceId)) {
            "Invalid dead-letter queue resource ID $deadLetterQueueResourceId"
          }
          val maxWorkItemAttempts =
            queue.maxWorkItemAttempts.takeUnless { it == 0 } ?: DEFAULT_MAX_WORK_ITEM_ATTEMPTS
          check(maxWorkItemAttempts > 0) {
            "max_work_item_attempts must be positive for ${queue.queueResourceId}"
          }
          val queueId = fingerprint(queue.queueResourceId)
          add(Queue(queueId, queue.queueResourceId, deadLetterQueueResourceId, maxWorkItemAttempts))
        }
      }
      .sortedBy { it.queueResourceId }

  private val queuesById: Map<Long, Queue> =
    buildMap(queues.size) {
      for (queue in queues) {
        val existingQueue = get(queue.queueId)
        if (existingQueue != null) {
          error(
            "Fingerprinting collision between queues " +
              "${existingQueue.queueResourceId} and ${queue.queueResourceId}"
          )
        }
        put(queue.queueId, queue)
      }
    }

  private val queuesByResourceId: Map<String, Queue> = queues.associateBy { it.queueResourceId }

  fun getQueueById(queueId: Long) = queuesById[queueId]

  fun getQueueByResourceId(queueResourceId: String) = queuesByResourceId[queueResourceId]

  companion object {
    private fun fingerprint(input: String): Long =
      Hashing.farmHashFingerprint64().hashString(input, Charsets.UTF_8).asLong()

    private val QUEUE_RESOURCE_ID_REGEX =
      Regex("^[a-zA-Z]([a-zA-Z0-9.-]{0,61}[a-zA-Z0-9])?(/[a-zA-Z0-9_-]+)*$")
    private const val DEFAULT_MAX_WORK_ITEM_ATTEMPTS = 5
  }
}
