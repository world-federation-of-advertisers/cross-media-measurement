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

package org.wfanet.measurement.reporting.job

import com.google.protobuf.Duration
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.DateTime
import com.google.type.TimeZone
import com.google.type.copy
import io.grpc.Status
import io.grpc.StatusException
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.TemporalAdjusters
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesResponse
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub as InternalReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub as InternalReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import com.google.type.interval
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.setReportScheduleIterationStateRequest
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.MetadataPrincipalServerInterceptor.Companion.withPrincipalName
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor.Companion.withReportScheduleInfo
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportSchedulesService
import org.wfanet.measurement.reporting.service.api.v2alpha.toPublic
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

class ReportSchedulingJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalReportScheduleIterationsStub: InternalReportScheduleIterationsCoroutineStub,
  private val internalReportSchedulesStub: InternalReportSchedulesCoroutineStub,
  private val reportsStub: ReportsCoroutineStub,
) {

  suspend fun execute() {
    // map of resource name to resource
    val dataProvidersMap: MutableMap<String, DataProvider> = mutableMapOf()
    for (measurementConsumerConfig in measurementConsumerConfigs.configsMap.entries) {
      // map of resource name to resource
      val eventGroupsMap: MutableMap<String, EventGroup> = mutableMapOf()

      val measurementConsumerId =
        MeasurementConsumerKey.fromName(measurementConsumerConfig.key)!!.measurementConsumerId
      var listReportSchedulesResponse: ListReportSchedulesResponse =
        ListReportSchedulesResponse.getDefaultInstance()
      do {
        listReportSchedulesResponse =
          internalReportSchedulesStub.listReportSchedules(
            listReportSchedulesRequest {
              filter =
                ListReportSchedulesRequestKt.filter {
                  cmmsMeasurementConsumerId = measurementConsumerId
                  if (listReportSchedulesResponse.reportSchedulesList.size > 0) {
                    externalReportScheduleIdAfter =
                      listReportSchedulesResponse.reportSchedulesList
                        .last()
                        .externalReportScheduleId
                  }
                  state = ReportSchedule.State.ACTIVE
                }
              limit = BATCH_SIZE
            }
          )

        for (reportSchedule in
          listReportSchedulesResponse.reportSchedulesList.sortedBy {
            it.nextReportCreationTime.seconds
          }) {
          val reportScheduleIteration =
            if (
              !reportSchedule.hasLatestIteration() ||
                reportSchedule.latestIteration.state == ReportScheduleIteration.State.REPORT_CREATED
            ) {
              internalReportScheduleIterationsStub.createReportScheduleIteration(
                reportScheduleIteration {
                  cmmsMeasurementConsumerId = measurementConsumerId
                  externalReportScheduleId = reportSchedule.externalReportScheduleId
                  createReportRequestId = UUID.randomUUID().toString()
                  reportEventTime = reportSchedule.nextReportCreationTime
                }
              )
            } else {
              reportSchedule.latestIteration
            }

          val publicReportSchedule = reportSchedule.toPublic()

          try {
            val internalReportingSets: List<ReportingSet> =
              ReportSchedulesService.getInternalReportingSets(
                publicReportSchedule.reportTemplate,
                measurementConsumerId,
                internalReportingSetsStub
              )

            val eventGroupKeys: List<ReportingSet.Primitive.EventGroupKey> =
              internalReportingSets
                .filter { it.hasPrimitive() }
                .flatMap { it.primitive.eventGroupKeysList }

            val windowStart: Timestamp =
              ReportSchedulesService.buildReportWindowStartTimestamp(
                publicReportSchedule,
                reportSchedule.nextReportCreationTime
              )

            val isDataAvailable: Boolean =
              isDataAvailable(
                windowStart,
                reportSchedule.nextReportCreationTime,
                eventGroupKeys,
                dataProvidersStub,
                dataProvidersMap,
                eventGroupsStub,
                eventGroupsMap,
                measurementConsumerConfig.value.apiKey
              )

            if (isDataAvailable) {
              val nextReportCreationTime: Timestamp =
                if (reportSchedule.details.eventStart.hasUtcOffset()) {
                  val offsetDateTime =
                    reportSchedule.nextReportCreationTime.toOffsetDateTime(
                      reportSchedule.details.eventStart.utcOffset
                    )
                  getNextReportCreationTime(offsetDateTime, reportSchedule.details.frequency)
                } else {
                  val zonedDateTime =
                    reportSchedule.nextReportCreationTime.toZonedDateTime(
                      reportSchedule.details.eventStart.timeZone
                    )
                  getNextReportCreationTime(zonedDateTime, reportSchedule.details.frequency)
                }

              val eventEndTimestamp =
                publicReportSchedule.eventStart
                  .copy {
                    day = reportSchedule.details.eventEnd.day
                    month = reportSchedule.details.eventEnd.month
                    year = reportSchedule.details.eventEnd.year
                  }
                  .toTimestamp()

              val stopReportScheduleRequest = stopReportScheduleRequest {
                cmmsMeasurementConsumerId = measurementConsumerId
                externalReportScheduleId = reportSchedule.externalReportScheduleId
              }

              val setReportScheduleIterationStateRequest = setReportScheduleIterationStateRequest {
                cmmsMeasurementConsumerId = measurementConsumerId
                externalReportScheduleId = reportSchedule.externalReportScheduleId
                externalReportScheduleIterationId =
                  reportScheduleIteration.externalReportScheduleIterationId
                state = ReportScheduleIteration.State.REPORT_CREATED
              }

              // If schedule should have been stopped in the last run, but it failed, then this will
              // catch it.
              if (
                Timestamps.compare(reportSchedule.nextReportCreationTime, eventEndTimestamp) > 0
              ) {
                internalReportSchedulesStub.stopReportSchedule(stopReportScheduleRequest)
                internalReportScheduleIterationsStub.setReportScheduleIterationState(
                  setReportScheduleIterationStateRequest
                )
                continue
              }

              try {
                reportsStub
                  .withPrincipalName(measurementConsumerConfig.key)
                  .withReportScheduleInfo(
                    ReportScheduleInfoServerInterceptor.ReportScheduleInfo(
                      publicReportSchedule.name,
                      nextReportCreationTime
                    )
                  )
                  .createReport(
                    createReportRequest {
                      parent = measurementConsumerConfig.key
                      requestId = reportScheduleIteration.createReportRequestId
                      reportId = "a" + reportScheduleIteration.createReportRequestId
                      report =
                        publicReportSchedule.reportTemplate.copy {
                          timeIntervals = timeIntervals {
                            timeIntervals += interval {
                              startTime = windowStart
                              endTime = reportSchedule.nextReportCreationTime
                            }
                          }
                        }
                    }
                  )
              } catch (e: Exception) {
                logger.warning(
                  "Report creation for Report Schedule ${publicReportSchedule.name} failed: ${e.cause}"
                )

                internalReportScheduleIterationsStub.setReportScheduleIterationState(
                  setReportScheduleIterationStateRequest {
                    cmmsMeasurementConsumerId = measurementConsumerId
                    externalReportScheduleId = reportSchedule.externalReportScheduleId
                    externalReportScheduleIterationId =
                      reportScheduleIteration.externalReportScheduleIterationId
                    state = ReportScheduleIteration.State.RETRYING_REPORT_CREATION
                  }
                )

                // Other schedules still have to be processed.
                continue
              }

              if (Timestamps.compare(nextReportCreationTime, eventEndTimestamp) > 0) {
                internalReportSchedulesStub.stopReportSchedule(stopReportScheduleRequest)
              }

              internalReportScheduleIterationsStub.setReportScheduleIterationState(
                setReportScheduleIterationStateRequest
              )
            }
          } catch (e: Exception) {
            logger.warning(
              "Processing failed for Report Schedule ${publicReportSchedule.name}: ${e.cause}"
            )
            // Other schedules still have to be processed.
            continue
          }
        }
      } while (listReportSchedulesResponse.reportSchedulesList.size == BATCH_SIZE)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val BATCH_SIZE = 50

    private suspend fun isDataAvailable(
      windowStart: Timestamp,
      eventTimestamp: Timestamp,
      eventGroupKeys: List<ReportingSet.Primitive.EventGroupKey>,
      dataProvidersStub: DataProvidersCoroutineStub,
      dataProvidersMap: MutableMap<String, DataProvider>,
      eventGroupsStub: EventGroupsCoroutineStub,
      eventGroupsMap: MutableMap<String, EventGroup>,
      apiAuthenticationKey: String,
    ): Boolean {
      for (eventGroupKey in eventGroupKeys) {
        val eventGroupName =
          EventGroupKey(
              dataProviderId = eventGroupKey.cmmsDataProviderId,
              eventGroupId = eventGroupKey.cmmsEventGroupId
            )
            .toName()
        val eventGroup =
          if (eventGroupsMap.containsKey(eventGroupName)) {
            eventGroupsMap[eventGroupName]
          } else {
            val getResponse =
              try {
                eventGroupsStub
                  .withAuthenticationKey(apiAuthenticationKey)
                  .getEventGroup(getEventGroupRequest { name = eventGroupName })
              } catch (e: StatusException) {
                throw when (e.status.code) {
                    Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                    Status.Code.CANCELLED -> Status.CANCELLED
                    Status.Code.NOT_FOUND -> Status.NOT_FOUND
                    else -> Status.UNKNOWN
                  }
                  .withCause(e)
                  .withDescription("Unable to get EventGroup with name $eventGroupName.")
                  .asRuntimeException()
              }
            eventGroupsMap[eventGroupName] = getResponse
            getResponse
          }

        val dataProviderName = DataProviderKey(eventGroupKey.cmmsDataProviderId).toName()
        val dataProvider =
          if (dataProvidersMap.containsKey(dataProviderName)) {
            dataProvidersMap[dataProviderName]
          } else {
            val getResponse =
              try {
                dataProvidersStub
                  .withAuthenticationKey(apiAuthenticationKey)
                  .getDataProvider(getDataProviderRequest { name = dataProviderName })
              } catch (e: StatusException) {
                throw when (e.status.code) {
                    Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                    Status.Code.CANCELLED -> Status.CANCELLED
                    Status.Code.NOT_FOUND -> Status.NOT_FOUND
                    else -> Status.UNKNOWN
                  }
                  .withCause(e)
                  .withDescription("Unable to get DataProvider with name $dataProviderName.")
                  .asRuntimeException()
              }
            dataProvidersMap[dataProviderName] = getResponse
            getResponse
          }

        // The final data availability start time for the event group can only be the same or later
        // than the data provider's data availability start time.
        val dataAvailabilityIntervalStart =
          if (
            Timestamps.compare(
              dataProvider!!.dataAvailabilityInterval.startTime,
              eventGroup!!.dataAvailabilityInterval.startTime
            ) >= 0
          ) {
            dataProvider.dataAvailabilityInterval.startTime
          } else {
            eventGroup.dataAvailabilityInterval.startTime
          }

        // The final data availability end time for the event group can only be the same or earlier
        // than the data provider's data availability end time. If the event group doesn't have a
        // data availability end time, then the data provider's data availability end time is
        // used by default.
        val dataAvailabilityIntervalEnd =
          if (eventGroup.dataAvailabilityInterval.hasEndTime()) {
            if (
              Timestamps.compare(
                dataProvider.dataAvailabilityInterval.endTime,
                eventGroup.dataAvailabilityInterval.endTime
              ) < 0
            ) {
              dataProvider.dataAvailabilityInterval.endTime
            } else {
              eventGroup.dataAvailabilityInterval.endTime
            }
          } else {
            dataProvider.dataAvailabilityInterval.endTime
          }

        // Data can only be available if the start time is before the end time, exclusive.
        if (Timestamps.compare(dataAvailabilityIntervalStart, dataAvailabilityIntervalEnd) >= 0) {
          return false
        }

        // The report window end time has to be before the data availability end time, inclusive.
        if (Timestamps.compare(eventTimestamp, dataAvailabilityIntervalEnd) > 0) {
          return false
        }

        // The report window start time has to be after the data availability start time, inclusive.
        if (Timestamps.compare(windowStart, dataAvailabilityIntervalStart) < 0) {
          return false
        }
      }

      return true
    }

    private fun Timestamp.toOffsetDateTime(utcOffset: Duration): OffsetDateTime {
      val source = this
      val offset = ZoneOffset.ofTotalSeconds(utcOffset.seconds.toInt())
      val localDateTime = LocalDateTime.ofEpochSecond(source.seconds, source.nanos, offset)

      return OffsetDateTime.of(localDateTime, offset)
    }

    private fun Timestamp.toZonedDateTime(timeZone: TimeZone): ZonedDateTime {
      val source = this
      val id = ZoneId.of(timeZone.id)

      return ZonedDateTime.ofInstant(source.toInstant(), id)
    }

    private fun getNextReportCreationTime(
      offsetDateTime: OffsetDateTime,
      frequency: ReportSchedule.Frequency
    ): Timestamp {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      return when (frequency.frequencyCase) {
        ReportSchedule.Frequency.FrequencyCase.DAILY -> {
          offsetDateTime.plusDays(1).toInstant().toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.WEEKLY -> {
          offsetDateTime
            .with(
              TemporalAdjusters.next(java.time.DayOfWeek.valueOf(frequency.weekly.dayOfWeek.name))
            )
            .toInstant()
            .toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.MONTHLY -> {
          val offsetDateTimeNextMonthEnd =
            offsetDateTime.plusMonths(1).with(TemporalAdjusters.lastDayOfMonth())
          offsetDateTimeNextMonthEnd
            .withDayOfMonth(
              minOf(offsetDateTimeNextMonthEnd.dayOfMonth, frequency.monthly.dayOfMonth)
            )
            .toInstant()
            .toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.FREQUENCY_NOT_SET -> {
          throw Status.FAILED_PRECONDITION.withDescription("frequency is not set")
            .asRuntimeException()
        }
      }
    }

    private fun getNextReportCreationTime(
      zonedDateTime: ZonedDateTime,
      frequency: ReportSchedule.Frequency
    ): Timestamp {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      return when (frequency.frequencyCase) {
        ReportSchedule.Frequency.FrequencyCase.DAILY -> {
          zonedDateTime.plusDays(1).toInstant().toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.WEEKLY -> {
          zonedDateTime
            .with(
              TemporalAdjusters.next(java.time.DayOfWeek.valueOf(frequency.weekly.dayOfWeek.name))
            )
            .toInstant()
            .toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.MONTHLY -> {
          val zonedDateTimeNextMonthEnd =
            zonedDateTime.plusMonths(1).with(TemporalAdjusters.lastDayOfMonth())
          zonedDateTimeNextMonthEnd
            .withDayOfMonth(
              minOf(zonedDateTimeNextMonthEnd.dayOfMonth, frequency.monthly.dayOfMonth)
            )
            .toInstant()
            .toProtoTime()
        }
        ReportSchedule.Frequency.FrequencyCase.FREQUENCY_NOT_SET -> {
          throw Status.FAILED_PRECONDITION.withDescription("frequency is not set")
            .asRuntimeException()
        }
      }
    }

    private fun DateTime.toTimestamp(): Timestamp {
      val source = this
      return if (source.hasUtcOffset()) {
        val offset = ZoneOffset.ofTotalSeconds(source.utcOffset.seconds.toInt())
        val offsetDateTime =
          OffsetDateTime.of(
            source.year,
            source.month,
            source.day,
            source.hours,
            source.minutes,
            source.seconds,
            source.nanos,
            offset
          )
        offsetDateTime.toInstant().toProtoTime()
      } else {
        val id = ZoneId.of(source.timeZone.id)
        val zonedDateTime =
          ZonedDateTime.of(
            source.year,
            source.month,
            source.day,
            source.hours,
            source.minutes,
            source.seconds,
            source.nanos,
            id
          )
        zonedDateTime.toInstant().toProtoTime()
      }
    }
  }
}
