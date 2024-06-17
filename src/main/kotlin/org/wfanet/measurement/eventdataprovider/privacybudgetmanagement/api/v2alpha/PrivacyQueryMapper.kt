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
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.EventGroupSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.LandscapeMask
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

object PrivacyQueryMapper {
  private const val SENSITIVITY = 1.0

  /**
   * Constructs a pbm specific [AcdpQuery] from given proto messages for Mpc protocols.
   *
   * @param reference representing the reference key and if the charge is a refund.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   *   sampling interval is obtained from this.
   * @param eventSpecs event specs from the Requisition. The date range and demo groups are obtained
   *   from this.
   * @param contributorCount number of Duchies
   * @throws
   *   org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
   *   if an error occurs in handling this request. Possible exceptions could include running out of
   *   privacy budget or a failure to commit the transaction to the database.
   */
  fun getMpcAcdpQuery(
    reference: Reference,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    contributorCount: Int,
  ): AcdpQuery {
    val acdpCharge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH -> {
          AcdpParamsConverter.getMpcAcdpCharge(
            DpParams(
              measurementSpec.reach.privacyParams.epsilon,
              measurementSpec.reach.privacyParams.delta,
            ),
            contributorCount,
          )
        }
        MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          val acdpChargeForReach =
            AcdpParamsConverter.getMpcAcdpCharge(
              DpParams(
                measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon,
                measurementSpec.reachAndFrequency.reachPrivacyParams.delta,
              ),
              contributorCount,
            )
          val acdpChargeForFrequency =
            AcdpParamsConverter.getMpcAcdpCharge(
              DpParams(
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon,
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta,
              ),
              contributorCount,
            )
          AcdpCharge(
            acdpChargeForReach.rho + acdpChargeForFrequency.rho,
            acdpChargeForReach.theta + acdpChargeForFrequency.theta,
          )
        }
        else ->
          throw IllegalArgumentException(
            "Measurement type ${measurementSpec.measurementTypeCase} is not supported in getMpcAcdpQuery()"
          )
      }

    return AcdpQuery(
      reference,
      LandscapeMask(
        eventSpecs.map { EventGroupSpec(it.filter.expression, it.collectionInterval.toRange()) },
        measurementSpec.vidSamplingInterval.start,
        measurementSpec.vidSamplingInterval.width,
      ),
      acdpCharge,
    )
  }

  /**
   * Constructs a pbm specific [AcdpQuery] from given proto messages for direct measurements.
   *
   * @param reference representing the reference key and if the charge is a refund.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   *   sampling interval is obtained from this.
   * @param eventSpecs event specs from the Requisition. The date range and demo groups are obtained
   *   from this.
   * @throws
   *   org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
   *   if an error occurs in handling this request. Possible exceptions could include running out of
   *   privacy budget or a failure to commit the transaction to the database.
   */
  fun getDirectAcdpQuery(
    reference: Reference,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
  ): AcdpQuery {
    val acdpCharge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH -> {
          AcdpParamsConverter.getDirectAcdpCharge(
            DpParams(
              measurementSpec.reach.privacyParams.epsilon,
              measurementSpec.reach.privacyParams.delta,
            ),
            SENSITIVITY,
          )
        }
        MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          val acdpChargeForReach =
            AcdpParamsConverter.getDirectAcdpCharge(
              DpParams(
                measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon,
                measurementSpec.reachAndFrequency.reachPrivacyParams.delta,
              ),
              SENSITIVITY,
            )
          val acdpChargeForFrequency =
            AcdpParamsConverter.getDirectAcdpCharge(
              DpParams(
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon,
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta,
              ),
              SENSITIVITY,
            )
          AcdpCharge(
            acdpChargeForReach.rho + acdpChargeForFrequency.rho,
            acdpChargeForReach.theta + acdpChargeForFrequency.theta,
          )
        }
        MeasurementTypeCase.IMPRESSION ->
          AcdpParamsConverter.getDirectAcdpCharge(
            DpParams(
              measurementSpec.impression.privacyParams.epsilon,
              measurementSpec.impression.privacyParams.delta,
            ),
            SENSITIVITY,
          )
        MeasurementTypeCase.DURATION ->
          AcdpParamsConverter.getDirectAcdpCharge(
            DpParams(
              measurementSpec.duration.privacyParams.epsilon,
              measurementSpec.duration.privacyParams.delta,
            ),
            SENSITIVITY,
          )
        else -> throw IllegalArgumentException("Measurement type not supported")
      }

    return AcdpQuery(
      reference,
      LandscapeMask(
        eventSpecs.map { EventGroupSpec(it.filter.expression, it.collectionInterval.toRange()) },
        measurementSpec.vidSamplingInterval.start,
        measurementSpec.vidSamplingInterval.width,
      ),
      acdpCharge,
    )
  }
}
