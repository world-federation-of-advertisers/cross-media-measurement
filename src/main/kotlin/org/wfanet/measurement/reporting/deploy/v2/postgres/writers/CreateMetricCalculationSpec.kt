/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.internal.reporting.v2.CreateMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricCalculationSpecReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecAlreadyExistsException

/**
 * Inserts a Metric Calculation Spec into the database.
 *
 * Throws the following on [execute]:
 * * [MetricCalculationSpecAlreadyExistsException] MetricCalculationSpec already exists
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateMetricCalculationSpec(private val request: CreateMetricCalculationSpecRequest) :
  PostgresWriter<MetricCalculationSpec>() {
  override suspend fun TransactionScope.runTransaction(): MetricCalculationSpec {
    val metricCalculationSpec = request.metricCalculationSpec
    val externalMetricCalculationSpecId = request.externalMetricCalculationSpecId

    val measurementConsumerId: InternalId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(metricCalculationSpec.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(
            metricCalculationSpec.cmmsMeasurementConsumerId
          ))
        .measurementConsumerId

    if (
      MetricCalculationSpecReader(transactionContext)
        .readMetricCalculationSpecByExternalId(
          metricCalculationSpec.cmmsMeasurementConsumerId,
          externalMetricCalculationSpecId,
        ) != null
    ) {
      throw MetricCalculationSpecAlreadyExistsException(
        metricCalculationSpec.cmmsMeasurementConsumerId,
        externalMetricCalculationSpecId,
      )
    }

    val campaignGroupId: InternalId? =
      if (metricCalculationSpec.externalCampaignGroupId.isNotEmpty()) {
        ReportingSetReader(transactionContext)
          .readCampaignGroup(measurementConsumerId, metricCalculationSpec.externalCampaignGroupId)
          ?.reportingSetId
      } else {
        null
      }

    val metricCalculationSpecId: InternalId = idGenerator.generateInternalId()

    val statement =
      boundStatement(
        """
      INSERT INTO MetricCalculationSpecs
        (
          MeasurementConsumerId,
          MetricCalculationSpecId,
          ExternalMetricCalculationSpecId,
          MetricCalculationSpecDetails,
          MetricCalculationSpecDetailsJson,
          CmmsModelLineName,
          CampaignGroupId
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", metricCalculationSpecId)
        bind("$3", externalMetricCalculationSpecId)
        bind("$4", metricCalculationSpec.details)
        bind("$5", metricCalculationSpec.details.toJson())
        if (metricCalculationSpec.cmmsModelLine.isNotEmpty()) {
          bind("$6", metricCalculationSpec.cmmsModelLine)
        } else {
          bind<String?>("$6", null)
        }
        bind("$7", campaignGroupId)
      }

    transactionContext.run { executeStatement(statement) }

    return metricCalculationSpec.copy {
      this.externalMetricCalculationSpecId = externalMetricCalculationSpecId
    }
  }
}
