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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.EventGroupSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.LandscapeMask
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

object PrivacyQueryMapper {
  private const val SENSITIVITY = 1.0

  /**
   * Constructs a pbm specific [AcdpQuery] from given proto messages for LiquidLegionsV2 protocol.
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
  fun getLiquidLegionsV2AcdpQuery(
    reference: Reference,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    contributorCount: Int,
  ): AcdpQuery {
    val acdpCharge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH -> {
          AcdpParamsConverter.getLlv2AcdpCharge(
            DpParams(
              measurementSpec.reach.privacyParams.epsilon,
              measurementSpec.reach.privacyParams.delta,
            ),
            contributorCount,
          )
        }
        MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          // TODO(@ple13): Optimize the pbm charge by computing the Acdp charge separately for
          // reach and for frequency, then add them up.
          val dpParams =
            DpParams(
              measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon,
              measurementSpec.reachAndFrequency.reachPrivacyParams.delta +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta,
            )

          AcdpParamsConverter.getLlv2AcdpCharge(dpParams, contributorCount)
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
   * Constructs a pbm specific [AcdpQuery] from given proto messages for Hmss protocol.
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
  fun getHmssAcdpQuery(
    reference: Reference,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    contributorCount: Int,
  ): AcdpQuery {
    val acdpCharge =
      when (measurementSpec.measurementTypeCase) {
        MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          // TODO(@ple13): Support different privacy parameters for reach and frequency.
          require(
            (measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon ==
              measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon) &&
              (measurementSpec.reachAndFrequency.reachPrivacyParams.delta ==
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta)
          ) {
            "Different privacy parameters for reach and for frequency have not been support yet."
          }
          // TODO(@ple13): Optimize the pbm charge by computing the Acdp charge separately for
          // reach and for frequency, then add them up.
          val dpParams =
            DpParams(
              measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon,
              measurementSpec.reachAndFrequency.reachPrivacyParams.delta +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta,
            )

          // TODO(@ple13): Update the code when AcdpParamsConverter has been extended to support
          // binomial noise and HMSS switchs to binomial noise.

          // Uses the function getLlv2AcdpCharge to compute the ACDP charge for this query as HMSS
          // and LLV2 use the same approach when adding differential private noise.
          AcdpParamsConverter.getLlv2AcdpCharge(dpParams, contributorCount)
        }
        else ->
          throw IllegalArgumentException(
            "Measurement type ${measurementSpec.measurementTypeCase} is not supported in getHmssAcdpQuery()"
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
          val dpParams =
            DpParams(
              measurementSpec.reachAndFrequency.reachPrivacyParams.epsilon +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.epsilon,
              measurementSpec.reachAndFrequency.reachPrivacyParams.delta +
                measurementSpec.reachAndFrequency.frequencyPrivacyParams.delta,
            )

          AcdpParamsConverter.getDirectAcdpCharge(dpParams, SENSITIVITY)
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
