// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey

/** Iterates the multimeasurement [BasicReport]s the BasicReports jobs process. */
object MultimeasurementBasicReports {
  private val logger: Logger = Logger.getLogger(MultimeasurementBasicReports::class.java.name)
  private const val BATCH_SIZE = 10

  /**
   * Calls [process] for each [BasicReport] in [state] of each `MeasurementConsumer` in
   * [measurementConsumerConfigs] that has an API key.
   *
   * A [BasicReport] whose processing fails is logged and skipped, so that it is retried on the next
   * execution without holding up the others.
   */
  suspend fun forEach(
    measurementConsumerConfigs: MeasurementConsumerConfigs,
    internalBasicReportsStub: BasicReportsCoroutineStub,
    state: BasicReport.State,
    process:
      suspend (
        basicReport: BasicReport,
        measurementConsumerName: String,
        measurementConsumerConfig: MeasurementConsumerConfig,
      ) -> Unit,
  ) {
    for ((measurementConsumerName, measurementConsumerConfig) in
      measurementConsumerConfigs.configsMap) {
      if (measurementConsumerConfig.apiKey.isEmpty()) {
        continue
      }
      val cmmsMeasurementConsumerId: String =
        requireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
          .measurementConsumerId

      val resourceLists =
        internalBasicReportsStub.listResources { pageToken: ListBasicReportsPageToken? ->
          val response =
            try {
              internalBasicReportsStub.listBasicReports(
                listBasicReportsRequest {
                  pageSize = BATCH_SIZE
                  if (pageToken != null) {
                    this.pageToken = pageToken
                  }
                  filter =
                    ListBasicReportsRequestKt.filter {
                      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                      this.state = state
                    }
                }
              )
            } catch (e: StatusException) {
              throw Exception("Unable to list BasicReports of $measurementConsumerName", e)
            }
          val nextPageToken = if (response.hasNextPageToken()) response.nextPageToken else null
          ResourceList(response.basicReportsList, nextPageToken)
        }

      resourceLists.collect { resourceList ->
        for (basicReport in resourceList.resources) {
          try {
            process(basicReport, measurementConsumerName, measurementConsumerConfig)
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            val basicReportName: String =
              BasicReportKey(
                  basicReport.cmmsMeasurementConsumerId,
                  basicReport.externalBasicReportId,
                )
                .toName()
            logger.log(Level.WARNING, "Unable to process BasicReport $basicReportName", e)
          }
        }
      }
    }
  }
}
