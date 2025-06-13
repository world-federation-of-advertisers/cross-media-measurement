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
import org.wfanet.measurement.privacybudgetmanager.Charges.IntervalCharges
import org.wfanet.measurement.privacybudgetmanager.ChargesKt.intervalCharges

/** Key for a row of the Ledger. PBM is opinionated about this structure. */
data class LedgerRowKey(
  val eventDataProviderName: String,
  val measurementConsumerName: String,
  val eventGroupReferenceId: String,
  val date: LocalDate,
)

/**
 * [populationIndex]s are created by adhering to a [PrivacyLandscape]. A [populationIndex] is a
 * stable index of a combination of population fields values that is derived from a
 * [PrivacyLandscape]. e.g. if a [PrivacyLandscape] has 2 population fields: Age and Gender with
 * possible values Age: [18_24, 25+] and Gender : [M,F] then there are 4 possible population
 * combinations with [18_24_M, 25+_M, 18_24_F, 25+_F] [populationIndex]s are the indexes in this
 * above array so for 18_24_M, population index is 0, 25+_M, population index is 1 and so on
 */
data class BucketIndex(val populationIndex: Int, val vidIntervalIndex: Int)

/** Represents a PrivacyBucket. */
data class PrivacyBucket(val rowKey: LedgerRowKey, val bucketIndex: BucketIndex)

/**
 * Represents a set of [PrivacyBucket] together with their charges. Makes aggregation of
 * [PrivacyBucket]s, charges and interacion with the [Ledger] easy.
 */
class Slice {

  private val ledgerRowKeyToCharges: MutableMap<LedgerRowKey, Charges> = mutableMapOf()

  /** Returns the row keys for this slice. */
  fun getLedgerRowKeys(): List<LedgerRowKey> = ledgerRowKeyToCharges.keys.toList()

  /** Returns the [Charges] proto associated with the [key] */
  fun get(key: LedgerRowKey): Charges? = ledgerRowKeyToCharges.get(key)

  /** Adds the given Privacy Buckets to this Slice */
  fun add(privacyBuckets: List<PrivacyBucket>, charge: AcdpCharge): Unit =
    TODO("uakyol: implement this")

  private fun add(first: AcdpCharge, second: AcdpCharge) = acdpCharge {
    rho = first.rho + second.rho
    theta = first.theta + second.theta
  }

  private fun mergeIntervalCharges(
    first: IntervalCharges,
    second: IntervalCharges,
  ): IntervalCharges {
    val intervalIndexes =
      (first.vidIntervalIndexToChargesMap.keys + second.vidIntervalIndexToChargesMap.keys)
        .distinct()
    return intervalCharges {
      intervalIndexes.forEach { intervalIndex ->
        val firstCharge: AcdpCharge? = first.vidIntervalIndexToChargesMap[intervalIndex]
        val secondCharge: AcdpCharge? = second.vidIntervalIndexToChargesMap[intervalIndex]

        if (firstCharge != null && secondCharge != null) {
          vidIntervalIndexToCharges[intervalIndex] = add(firstCharge, secondCharge)
        } else if (firstCharge != null) {
          vidIntervalIndexToCharges[intervalIndex] = firstCharge
        } else if (secondCharge != null) {
          vidIntervalIndexToCharges[intervalIndex] = secondCharge
        } else {
          throw IllegalArgumentException("Both AcdpChages are null. This should never happen.")
        }
      }
    }
  }

  private fun mergeCharges(first: Charges, second: Charges): Charges {
    val populationIndexes =
      (first.populationIndexToChargesMap.keys + second.populationIndexToChargesMap.keys).distinct()
    return charges {
      populationIndexes.forEach { populationIndex ->
        val firstIntervalCharges: IntervalCharges? =
          first.populationIndexToChargesMap[populationIndex]
        val secondIntervalCharges: IntervalCharges? =
          second.populationIndexToChargesMap[populationIndex]

        if (firstIntervalCharges != null && secondIntervalCharges != null) {
          populationIndexToCharges[populationIndex] =
            mergeIntervalCharges(firstIntervalCharges, secondIntervalCharges)
        } else if (firstIntervalCharges != null) {
          populationIndexToCharges[populationIndex] = firstIntervalCharges
        } else if (secondIntervalCharges != null) {
          populationIndexToCharges[populationIndex] = secondIntervalCharges
        } else {
          throw IllegalArgumentException("Both IntervalCharges are null. This should never happen.")
        }
      }
    }
  }

  /**
   * Adds [ledgerRowKey] as a key if it doesn't exist and [charges] as its value, if the key exists,
   * merges [charges] with that key's value.
   */
  fun merge(ledgerRowKey: LedgerRowKey, charges: Charges): Unit {
    val existingCharges = ledgerRowKeyToCharges[ledgerRowKey]
    if (existingCharges == null) {
      ledgerRowKeyToCharges[ledgerRowKey] = charges
    } else {
      ledgerRowKeyToCharges[ledgerRowKey] = mergeCharges(existingCharges, charges)
    }
  }
}
