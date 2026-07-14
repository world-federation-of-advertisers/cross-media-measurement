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

package org.wfanet.measurement.edpaggregator.tools

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.queue.QueuePublisher
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

/**
 * Redelivers dead-lettered `WorkItem`s from a Pub/Sub dead-letter subscription back onto their
 * original work queue, so the pipeline resumes processing after the operator has fixed the
 * underlying issue.
 *
 * A `WorkItem` records its origin queue in [WorkItem.getQueue] (the `queue_resource_id`, which is
 * also the Pub/Sub topic id), so each message is republished to the queue it came from without any
 * external lookup. Republishing is safe because job creation and the per-phase processors are
 * idempotent (deterministic request ids; SUCCEEDED work short-circuits).
 *
 * @param subscriber subscriber for the dead-letter subscription.
 * @param publisher publisher used to republish to the original work queue topic.
 */
class DlqRedeliverer(
  private val subscriber: QueueSubscriber,
  private val publisher: QueuePublisher<WorkItem>,
) {
  /**
   * Pulls dead-lettered `WorkItem`s from [dlqSubscription] and republishes each to its origin queue
   * (or [topicOverride] when set), acknowledging it off the dead-letter subscription only after a
   * successful republish.
   *
   * When [queueFilter] is set, only messages whose origin [WorkItem.getQueue] matches are
   * redelivered; non-matching messages are nacked back onto the dead-letter subscription (never
   * dropped) for a later, differently-filtered run. Pub/Sub message *attributes* are not exposed by
   * [QueueSubscriber] (only the parsed body), so filtering is by origin queue, not by attribute.
   *
   * Stops after [maxMessages] have been redelivered, once [idleTimeoutMillis] elapses with no new
   * message (the subscription is drained), or once a nacked non-matching message is redelivered
   * back to us (a full pass over the non-matching messages — the matching ones are drained). The
   * underlying subscriber pulls indefinitely, so one of these bounds is what ends the run.
   *
   * @return the number of messages redelivered.
   */
  suspend fun redeliver(
    dlqSubscription: String,
    maxMessages: Int,
    idleTimeoutMillis: Long,
    topicOverride: String? = null,
    queueFilter: String? = null,
  ): Int {
    require(maxMessages > 0) { "maxMessages must be positive" }
    val channel = subscriber.subscribe(dlqSubscription, WorkItem.parser())
    var redelivered = 0
    // Non-matching WorkItems nacked back onto the subscription this run. Seeing one again means we
    // have made a full pass and the matching messages are drained, so we stop.
    val skipped = mutableSetOf<ByteString>()
    while (redelivered < maxMessages) {
      val message =
        withTimeoutOrNull(idleTimeoutMillis) { channel.receiveCatching().getOrNull() } ?: break
      val workItem: WorkItem = message.body
      if (queueFilter != null && workItem.queue != queueFilter) {
        message.nack()
        if (!skipped.add(workItem.toByteString())) break
        continue
      }
      val topic = topicOverride ?: workItem.queue
      require(topic.isNotEmpty()) {
        "WorkItem ${workItem.name} has no origin queue; rerun with --topic-override"
      }
      publisher.publishMessage(topic, workItem)
      message.ack()
      redelivered++
      logger.info("Redelivered ${workItem.name} to $topic")
    }
    return redelivered
  }

  companion object {
    private val logger: Logger = Logger.getLogger(DlqRedeliverer::class.java.name)
  }
}
