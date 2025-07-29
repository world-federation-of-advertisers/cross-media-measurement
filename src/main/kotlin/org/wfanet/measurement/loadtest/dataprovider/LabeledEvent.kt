/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import java.time.Instant
import java.time.LocalDate

/** An event [message] with [timestamp] and [vid] labels. */
data class LabeledEvent<T : Message>(val timestamp: Instant, val vid: Long, val message: T)

/**
 * A single shard of [LabeledEvent]s for a specific date.
 *
 * @param localDate the date associated with this shard
 * @param labeledEvents labeled events for this shard
 */
data class LabeledEventDateShard<T : Message>(
  val localDate: LocalDate,
  val labeledEvents: Sequence<LabeledEvent<T>>,
)
