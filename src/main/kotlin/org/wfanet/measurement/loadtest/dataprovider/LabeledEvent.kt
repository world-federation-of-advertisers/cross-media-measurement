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
 * Domain-level identity for an entity associated with a labeled impression.
 *
 * Mapped to the wire-format `LabeledImpression.EntityKey` at write time so that producers and
 * generators can stay decoupled from the proto.
 */
data class EntityKey(val entityType: String, val entityId: String)

/**
 * A group of [LabeledEvent]s that share the same [entityKeys].
 *
 * Multiple groups may live inside a single [LabeledEventDateShard]. When the shard is written, all
 * groups land in the same impressions blob, but each event is stamped with the [entityKeys] from
 * its containing group, allowing different impressions in the same file to carry different entity
 * keys.
 */
data class EntityKeysWithLabeledEvents<T : Message>(
  val entityKeys: List<EntityKey>,
  val labeledEvents: Sequence<LabeledEvent<T>>,
)

/**
 * A single shard of [LabeledEvent]s for a specific date.
 *
 * @param localDate the date associated with this shard
 * @param entityKeysWithLabeledEvents groups of labeled events for this shard, each annotated with
 *   the [EntityKey]s to attach to every event in the group. A shard with a single group containing
 *   `entityKeys = emptyList()` represents the legacy "no entity keys" behavior.
 */
data class LabeledEventDateShard<T : Message>(
  val localDate: LocalDate,
  val entityKeysWithLabeledEvents: List<EntityKeysWithLabeledEvents<T>>,
)
