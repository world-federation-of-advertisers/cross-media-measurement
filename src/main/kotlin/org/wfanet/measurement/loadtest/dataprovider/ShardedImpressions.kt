// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow

/**
 * A data class representing a sharded collection of labeled impressions, partitioned by date.
 *
 * @param localDate The date associated with this shard of impressions.
 * @param impressions A flow of labeled events of type [T], where each event represents an
 *   impression.
 */
data class DateShardedLabeledImpression<T : Message>(
  val localDate: LocalDate,
  val impressions: Flow<LabeledEvent<T>>,
)
