/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.protobuf.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.MeasurementTypeCase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * This is the default value for the total amount that can be charged to a single privacy bucket.
 */
private const val MAXIMUM_PRIVACY_USAGE_PER_BUCKET = 1.0f
private const val MAXIMUM_DELTA_PER_BUCKET = 1.0e-9f

/**
 * Instantiates a privacy budget manager.
 *
 * @param filter: An object that maps [PrivacyBucketGroup]s to Event messages.
 * @param backingStore: An object that provides persistent storage of privacy budget data in a
 * consistent and atomic manner.
 * @param maximumPrivacyBudget: The maximum privacy budget that can be used in any privacy bucket.
 * @param maximumTotalDelta: Maximum total value of the delta parameter that can be used in any
 * privacy bucket.
 */
class PrivacyBudgetManager(
  val filter: PrivacyBucketFilter,
  val backingStore: PrivacyBudgetLedgerBackingStore,
  val maximumPrivacyBudget: Float = MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
  val maximumTotalDelta: Float = MAXIMUM_DELTA_PER_BUCKET,
) {

  val ledger = PrivacyBudgetLedger(backingStore, maximumPrivacyBudget, maximumTotalDelta)

  /**
   * Constructs a pbm specific [PrivacyQuery] from given proto messages.
   *
   * @param privacyReference representing the reference key and if the charge is a refund.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   * sampling interval is obtained from from this.
   * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
   * range and demo groups are obtained from this.
   * @throws PrivacyBudgetManagerException if an error occurs in handling this request. Possible
   * exceptions could include running out of privacy budget or a failure to commit the transaction
   * to the database.
   */
  private fun getPrivacyQuery(
    privacyReference: PrivacyReference,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ): PrivacyQuery {
    val charge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH_AND_FREQUENCY ->
          PrivacyCharge(
            measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon.toFloat() +
              measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon.toFloat(),
            measurementSpec.reachAndFrequency.reachPrivacyParams.delta.toFloat() +
              measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta.toFloat()
          )
        // TODO(@uakyol): After the privacy budget accounting is switched to using the Gaussian
        // mechanism, replace the above lines with the following.  This will further improve the
        // efficiency of privacy budget usage for reach and frequency queries.
        //
        // {
        //
        // chargeList.add(PrivacyCharge(
        //   measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon.toFloat(),
        //  measurementSpec.reachAndFrequency.reachPrivacyParams.delta.toFloat()))
        //
        // chargeList.add(PrivacyCharge(
        //   measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon.toFloat(),
        //  measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta.toFloat()))
        // }

        MeasurementTypeCase.IMPRESSION ->
          PrivacyCharge(
            measurementSpec.impression.privacyParams.epsilon.toFloat(),
            measurementSpec.impression.privacyParams.delta.toFloat()
          )
        MeasurementTypeCase.DURATION ->
          PrivacyCharge(
            measurementSpec.duration.privacyParams.epsilon.toFloat(),
            measurementSpec.duration.privacyParams.delta.toFloat()
          )
        else -> throw IllegalArgumentException("Measurement type not supported")
      }
    return PrivacyQuery(
      privacyReference,
      PrivacyLandscapeMask(
        requisitionSpec.getEventGroupsList().map {
          PrivacyEventGroupSpec(
            it.value.filter.expression,
            it.value.collectionInterval.startTime.toLocalDate("UTC"),
            it.value.collectionInterval.endTime.toLocalDate("UTC")
          )
        },
        measurementSpec.reachAndFrequency.vidSamplingInterval.start,
        measurementSpec.reachAndFrequency.vidSamplingInterval.width
      ),
      charge
    )
  }

  // TODO(@uakyol) : make this function public for then purpose of replays.
  private fun chargePrivacyBudget(measurementConsumerId: String, privacyQuery: PrivacyQuery) =
    ledger.chargePrivacyBucketGroups(
      privacyQuery.privacyReference,
      filter.getPrivacyBucketGroups(measurementConsumerId, privacyQuery.privacyLandscapeMask),
      setOf(privacyQuery.privacyCharge)
    )

  /**
   * Charges all of the privacy buckets identified by the given measurementSpec and requisitionSpec,
   * if possible.
   *
   * @param privacyReference representing the reference key and if the charge is a refund.
   * @param measurementConsumerId that the charges are for.
   * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
   * range and demo groups are obtained from this.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   * sampling interval is obtained from from this.
   * @throws PrivacyBudgetManagerException if an error occurs in handling this request. Possible
   * exceptions could include running out of privacy budget or a failure to commit the transaction
   * to the database.
   */
  fun chargePrivacyBudget(
    privacyReference: PrivacyReference,
    measurementConsumerId: String,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ) =
    chargePrivacyBudget(
      measurementConsumerId,
      getPrivacyQuery(privacyReference, requisitionSpec, measurementSpec)
    )
}

// TODO(@uakyol): Update time conversion after getting alignment on civil calendar days.
private fun Timestamp.toLocalDate(timeZone: String): LocalDate =
  Instant.ofEpochSecond(this.getSeconds(), this.getNanos().toLong())
    .atZone(ZoneId.of(timeZone))
    .toLocalDate()
