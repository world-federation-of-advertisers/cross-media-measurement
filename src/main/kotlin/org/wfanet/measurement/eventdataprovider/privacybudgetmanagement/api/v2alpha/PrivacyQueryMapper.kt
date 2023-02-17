/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.MeasurementTypeCase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.toRange
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Charge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.EventGroupSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.LandscapeMask
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Query
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

object PrivacyQueryMapper {
  /**
   * Constructs a pbm specific [Query] from given proto messages.
   *
   * @param reference representing the reference key and if the charge is a refund.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   *   sampling interval is obtained from from this.
   * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
   *   range and demo groups are obtained from this.
   * @throws PrivacyBudgetManagerException if an error occurs in handling this request. Possible
   *   exceptions could include running out of privacy budget or a failure to commit the transaction
   *   to the database.
   */
  fun getPrivacyQuery(
    reference: Reference,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ): Query {
    val charge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH_AND_FREQUENCY ->
          Charge(
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
          Charge(
            measurementSpec.impression.privacyParams.epsilon.toFloat(),
            measurementSpec.impression.privacyParams.delta.toFloat()
          )
        MeasurementTypeCase.DURATION ->
          Charge(
            measurementSpec.duration.privacyParams.epsilon.toFloat(),
            measurementSpec.duration.privacyParams.delta.toFloat()
          )
        else -> throw IllegalArgumentException("Measurement type not supported")
      }
    return Query(
      reference,
      LandscapeMask(
        requisitionSpec.eventGroupsList.map {
          EventGroupSpec(it.value.filter.expression, it.value.collectionInterval.toRange())
        },
        measurementSpec.vidSamplingInterval.start,
        measurementSpec.vidSamplingInterval.width
      ),
      charge
    )
  }
}
