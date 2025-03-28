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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup
import org.wfanet.measurement.reporting.service.internal.Errors

@RunWith(JUnit4::class)
abstract class BasicReportsServiceTest<T : BasicReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val basicReportsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.basicReportsService
    measurementConsumersService = services.measurementConsumersService
    reportingSetsService = services.reportingSetsService
  }

  @Test
  fun `insertBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val resultGroup = resultGroup { title = "title" }

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails { resultGroups += resultGroup }
    }

    val response =
      service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })

    assertThat(response).ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER).isEqualTo(basicReport)

    assertThat(response.hasCreateTime())
  }

  @Test
  fun `insertBasicReport throws FAILED_PRECONDITION when reporting set not found`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val resultGroup = resultGroup { title = "title" }

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        campaignGroupDisplayName = REPORTING_SET.displayName
        details = basicReportDetails { title = "title" }
        resultDetails = basicReportResultDetails { resultGroups += resultGroup }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `insertBasicReport throws ALREADY_EXISTS when external id found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val resultGroup = resultGroup { title = "title" }

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails { resultGroups += resultGroup }
    }

    service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_ALREADY_EXISTS.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            basicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] = basicReport.externalBasicReportId
        }
      )
  }

  @Test
  fun `insertBasicReport throws IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND when impression qualification filter not found`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val resultGroup = resultGroup { title = "title" }

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId = "abc"
        }
      }
      resultDetails = basicReportResultDetails { resultGroups += resultGroup }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
          metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID.key] = "abc"
        }
      )
  }

  @Test
  fun `getBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(retrievedBasicReport).isEqualTo(createdBasicReport)
  }

  @Test
  fun `getBasicReport throws NOT_FOUND when basic report not found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            createdBasicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
            createdBasicReport.externalBasicReportId + "b"
        }
      )
  }

  @Test
  fun `listBasicReport without page_size succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(3)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[2]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport with page_size succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    service.insertBasicReport(
      insertBasicReportRequest {
        basicReport =
          BASIC_REPORT.copy {
            externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
          }
      }
    )

    val listBasicReportsResponse =
      service.listBasicReports(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
          pageSize = 2
        }
      )

    assertThat(listBasicReportsResponse)
      .isEqualTo(
        listBasicReportsResponse {
          basicReports += createdBasicReport
          basicReports += createdBasicReport2
          nextPageToken = listBasicReportsPageToken {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = createdBasicReport2.createTime
                externalBasicReportId = createdBasicReport2.externalBasicReportId
              }
          }
        }
      )
  }

  @Test
  fun `listBasicReport with create_time_after succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                createTimeAfter = createdBasicReport.createTime
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(2)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport with page_token succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(2)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport looks at external id if createTime matches`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId[0].toString()
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(3)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[2]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport returns 0 results if no matches`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = BASIC_REPORT })

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(0)
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when missing cmms_measurement_consumer_id`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listBasicReports(listBasicReportsRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "filter.cmms_measurement_consumer_id"
          }
        )
    }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when page_size is negative`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listBasicReports(
          listBasicReportsRequest {
            pageSize = -1
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
        }
      )
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
    private val REPORTING_SET_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")

    private val REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "2345"
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1235"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    private val BASIC_REPORT = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails { resultGroups += resultGroup { title = "title" } }
    }
  }
}
