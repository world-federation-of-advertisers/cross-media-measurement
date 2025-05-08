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

/** Key for a row of the Ledger. PBM is opinionated about this structure. */
data class LedgerRowKey(
  val measurementConsumerId: String,
  val eventGroupId: String,
  val date: LocalDate,
)

/**
 * Represents a PrivacyBucket. [populationIndex]s are created by adhering to a [PrivacyLandscape]. A
 * [populationIndex] is a stable index of a combination of population fields values that is derived
 * from a [PrivacyLandscape]. e.g. if a [PrivacyLandscape] has 2 population fields: Age and Gender
 * with possible values Age: [18_24, 25+] and Gender : [M,F] then there are 4 possible population
 * combinations with [18_24_M, 25+_M, 18_24_F, 25+_F] [populationIndex]s are the indexes in this
 * above array so for 18_24_M, population index is 0, 25+_M, population index is 1 and so on
 */
data class PrivacyBucket(
  val rowKey: LedgerRowKey,
  val populationIndex: Int,
  val vidIntervalIndex: Int,
)

/**
 * Represents a set of [PrivacyBucket] together with their charges. Makes aggregation of
 * [PrivacyBucket]s, charges and interacion with the [Ledger] easy.
 */
class Slice {
  /** Returns the row keys for this slice. */
  fun getLedgerRowKeys(): List<LedgerRowKey> = TODO("uakyol: implement this")

  /** Adds the given Privacy Buckets to this Slice */
  fun add(privacyBuckets: List<PrivacyBucket>, charge: AcdpCharge): Unit =
    TODO("uakyol: implement this")
}
