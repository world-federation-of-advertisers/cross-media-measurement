/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.privacybudgetmanager

import java.time.LocalDate

/**
 * A key for a given row of the underlying Backing store. PBM is opinionated about this structure.
 */
data class RowKey(
  val measurementConsumerId: String,
  val eventGroupId: String,
  val date: LocalDate,
)

/** Represents a PrivacyBucket. PopulationIndexes are created by adhering to a PrivacyLandscape. */
data class PrivacyBucket(
  val rowKey: RowKey,
  val populationIndex: Int,
  val vidIntervalIndex: Int,
)

/** Represents a list of the Privacy Buckets together with their charges. */
class Slice {
  /** Returns the row keys for this slice. */
  fun getRowKeys(): List<RowKey> = TODO("uakyol: implement this")

  /** Adds the given Privacy Buckets to this Slice */
  fun add(
    privacyBuckets: List<PrivacyBucket>,
    charge: AcdpCharge,
  ): Unit = TODO("uakyol: implement this")
}
