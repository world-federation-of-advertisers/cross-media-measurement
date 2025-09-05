/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import java.util.logging.Logger
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.setStateRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.getReportRequest

class BasicReportsReportsPollJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalBasicReportsStub: InternalBasicReportsCoroutineStub,
  private val reportsStub: ReportsCoroutineStub,
) {

  /**
   * For every MeasurementConsumer, all BasicReports with State REPORT_CREATED are retrieved. For
   * each of those BasicReports, the Report is retrieved.
   */
  suspend fun execute() {
    val measurementConsumerConfigByName =
      measurementConsumerConfigs.configsMap.filterValues { it.offlinePrincipal.isNotEmpty() }

    for ((measurementConsumerName, measurementConsumerConfig) in measurementConsumerConfigByName.entries) {
      val cmmsMeasurementConsumerId =
        MeasurementConsumerKey.fromName(measurementConsumerName)!!.measurementConsumerId

      val resourceLists =
        internalBasicReportsStub.listResources(
          BATCH_SIZE,
          null
        ) { pageToken: ListBasicReportsPageToken?, remaining ->
          val listBasicReportsResponse =
            internalBasicReportsStub.listBasicReports(listBasicReportsRequest {
              pageSize = remaining
              if (pageToken != null) {
                this.pageToken = pageToken
              }
              filter = ListBasicReportsRequestKt.filter {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                state = BasicReport.State.REPORT_CREATED
              }
            })

          val nextPageToken =
            if (listBasicReportsResponse.hasNextPageToken()) {
              listBasicReportsResponse.nextPageToken
            } else {
              null
            }

          ResourceList(listBasicReportsResponse.basicReportsList, nextPageToken)
        }

      resourceLists.collect { resourceList ->
        for (basicReport in resourceList.resources) {
          try {
            val report =
              reportsStub
                .withCallCredentials(
                  TrustedPrincipalAuthInterceptor.Credentials(
                    // TODO(@SanjayVas): Read full Principal from Access.
                    principal { name = measurementConsumerConfig.offlinePrincipal },
                    setOf("reporting.reports.get"),
                  )
                )
                .getReport(getReportRequest {
                  name = ReportKey(
                    cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                    reportId = basicReport.externalReportId,
                  ).toName()
                })

            if (report.state == Report.State.SUCCEEDED) {
              // TODO(@tristanvuong2021#2607): Transform Report Results and persist in Spanner
            } else if (report.state == Report.State.FAILED) {
              internalBasicReportsStub.setState(setStateRequest {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                externalBasicReportId = basicReport.externalBasicReportId
                state = BasicReport.State.FAILED
              })
            }
          } catch (e: Exception) {
            logger.warning("Failed to get Report Results for BasicReports: $e")
          }
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BATCH_SIZE = 10
  }
}
