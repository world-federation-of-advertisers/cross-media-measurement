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

/**
 * A key for a given row of the underlying Backing store. PBM is opinionated about this structure.
 */
data class RowKey(
  measurementConsumerId: String,
  eventGroupId: String,
  date: Date,
)

/** Represents a PrivacyBucket. PopulationIndexes are created by adhering to a PrivacyLandscape. */
data class PrivacyBucket(
  rowKey: RowKey,
  populationIndex: Int,
  vidIntervalIndex: Int,
)

/** Represents a slice of the Privacy Landscape together with charges. */
class Slice {
  /** Returns the row keys for this slice. */
  fun getRowKeys(): List<RowKey> = TODO("uakyol: implement this")

  /** Adds the given Privacy Buckets to this Slice */
  fun add(
    privacyBuckets: List<PrivacyBucket>,
    charge: AcdpCharge,
  ): Unit = TODO("uakyol: implement this")
}
