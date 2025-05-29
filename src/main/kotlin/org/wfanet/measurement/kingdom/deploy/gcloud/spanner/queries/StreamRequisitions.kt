// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

class StreamRequisitions(requestFilter: StreamRequisitionsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<RequisitionReader.Result>() {

  private val parent: RequisitionReader.Parent =
    if (requestFilter.externalMeasurementId != 0L) {
      RequisitionReader.Parent.MEASUREMENT
    } else if (requestFilter.externalDataProviderId != 0L) {
      RequisitionReader.Parent.DATA_PROVIDER
    } else {
      RequisitionReader.Parent.NONE
    }

  override val reader =
    RequisitionReader.build(parent) {
      setWhereClause(requestFilter)
      orderByClause = ORDER_BY_CLAUSE
      if (limit > 0) {
        limitClause = "LIMIT @$LIMIT"
        bind(LIMIT).to(limit.toLong())
      }
    }

  private fun RequisitionReader.Builder.setWhereClause(filter: StreamRequisitionsRequest.Filter) {
    val conjuncts = mutableListOf<String>()
    if (filter.externalMeasurementConsumerId != 0L) {
      conjuncts.add("ExternalMeasurementConsumerId = @$EXTERNAL_MEASUREMENT_CONSUMER_ID")
      bind(EXTERNAL_MEASUREMENT_CONSUMER_ID).to(filter.externalMeasurementConsumerId)
    }
    if (filter.externalMeasurementId != 0L) {
      conjuncts.add("ExternalMeasurementId = @$EXTERNAL_MEASUREMENT_ID")
      bind(EXTERNAL_MEASUREMENT_ID).to(filter.externalMeasurementId)
    }
    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @$EXTERNAL_DATA_PROVIDER_ID")
      bind(EXTERNAL_DATA_PROVIDER_ID).to(filter.externalDataProviderId)
    }
    if (filter.statesValueList.isNotEmpty()) {
      conjuncts.add("Requisitions.State IN UNNEST(@$STATES)")
      bind(STATES).toInt64Array(filter.statesList)
    }
    if (filter.hasUpdatedAfter()) {
      conjuncts.add("Requisitions.UpdateTime > @$UPDATE_TIME")
      conjuncts.add("RequisitionIndexShardId >= 0")
      bind(UPDATE_TIME).to(filter.updatedAfter.toGcloudTimestamp())
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
        RequisitionIdentity IN (
          SELECT
            RequisitionIdentity
          FROM
            Requisitions
          WHERE
            RequisitionIndexShardId >= 0
            AND (
              Requisitions.UpdateTime > @$UPDATED_AFTER
              OR (
                Requisitions.UpdateTime = @$UPDATED_AFTER
                AND RequisitionIdentity > @$REQUISITION_IDENTITY_AFTER
              )
            )
        )
        """
          .trimIndent()
      )

      bind(UPDATED_AFTER).to(filter.after.updateTime.toGcloudTimestamp())
      bind(REQUISITION_IDENTITY_AFTER).to(filter.after.requisitionIdentity)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    whereClause = "WHERE " + conjuncts.joinToString(" AND ")
  }

  companion object {
    private const val ORDER_BY_CLAUSE = "ORDER BY UpdateTime, RequisitionIdentity"

    const val LIMIT = "limit"
    const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
    const val EXTERNAL_MEASUREMENT_ID = "externalMeasurementId"
    const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    const val STATES = "states"
    const val UPDATE_TIME = "updateTime"
    const val UPDATED_AFTER = "updatedAfter"
    const val REQUISITION_IDENTITY_AFTER = "requisitionIdentityAfter"
  }
}
