// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.type.Interval
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

/** Implementation of MeasurementConsumerSimulator for use with the EDP Aggregator. */
class EDPAggregatorMeasurementConsumerSimulator(
  measurementConsumerData: MeasurementConsumerData,
  outputDpParams: DifferentialPrivacyParams,
  dataProvidersClient: DataProvidersCoroutineStub,
  eventGroupsClient: EventGroupsCoroutineStub,
  measurementsClient: MeasurementsCoroutineStub,
  measurementConsumersClient: MeasurementConsumersCoroutineStub,
  certificatesClient: CertificatesCoroutineStub,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val messageInstance: Message,
  expectedDirectNoiseMechanism: NoiseMechanism,
  private val syntheticPopulationSpec: SyntheticPopulationSpec,
  private val syntheticEventGroupMap: Map<String, SyntheticEventGroupSpec>,
  filterExpression: String = DEFAULT_FILTER_EXPRESSION,
  eventRange: OpenEndTimeRange = DEFAULT_EVENT_RANGE,
  initialResultPollingDelay: Duration = Duration.ofSeconds(1),
  maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
) :
  MeasurementConsumerSimulator(
    measurementConsumerData,
    outputDpParams,
    dataProvidersClient,
    eventGroupsClient,
    measurementsClient,
    measurementConsumersClient,
    certificatesClient,
    trustedCertificates,
    expectedDirectNoiseMechanism,
    filterExpression,
    eventRange,
    initialResultPollingDelay,
    maximumResultPollingDelay,
  ) {

  override fun Flow<EventGroup>.filterEventGroups(): Flow<EventGroup> {
    return filter { it.eventGroupReferenceId in syntheticEventGroupMap.keys }
  }

  override fun getFilteredVids(measurementInfo: MeasurementInfo): Flow<Long> {
    val eventGroupSpecs: List<Triple<SyntheticEventGroupSpec, String, Interval>> =
      measurementInfo.requisitions.flatMap { requisitionInfo ->
        requisitionInfo.eventGroups
          .zip(requisitionInfo.requisitionSpec.events.eventGroupsList)
          .map { (eventGroup, eventGroupEntry) ->
            Triple(
              syntheticEventGroupMap.getValue(eventGroup.eventGroupReferenceId),
              eventGroupEntry.value.filter.expression,
              eventGroupEntry.value.collectionInterval,
            )
          }
      }

    return eventGroupSpecs
      .flatMap { (syntheticEventGroupSpec, expression, collectionInterval) ->
        runBlocking {
          val program: Program =
            EventFilters.compileProgram(messageInstance.descriptorForType, expression)
          SyntheticDataGeneration.generateEvents(
              messageInstance,
              syntheticPopulationSpec,
              syntheticEventGroupSpec,
            )
            .toList()
            .flatMap { it.impressions.toList() }
            .filter { impression -> EventFilters.matches(impression.message, program) }
            .filter { impression ->
              impression.timestamp >= collectionInterval.startTime.toInstant() &&
                impression.timestamp < collectionInterval.endTime.toInstant()
            }
        }
      }
      .map { it.vid }
      .asFlow()
  }

  override fun getFilteredVids(
    measurementInfo: MeasurementInfo,
    targetDataProviderId: String,
  ): Flow<Long> {
    val eventGroupSpecs: List<Pair<SyntheticEventGroupSpec, String>> =
      measurementInfo.requisitions.flatMap { requisitionInfo ->
        requisitionInfo.eventGroups
          .zip(requisitionInfo.requisitionSpec.events.eventGroupsList)
          .filter { (eventGroup, eventGroupEntry) ->
            targetDataProviderId ==
              requireNotNull(EventGroupKey.fromName(eventGroup.name)).dataProviderId
          }
          .map { (eventGroup, eventGroupEntry) ->
            Pair(
              syntheticEventGroupMap.getValue(eventGroup.eventGroupReferenceId),
              eventGroupEntry.value.filter.expression,
            )
          }
      }
    return eventGroupSpecs
      .flatMap { (syntheticEventGroupSpec, expression) ->
        runBlocking {
          val program: Program =
            EventFilters.compileProgram(messageInstance.descriptorForType, expression)
          SyntheticDataGeneration.generateEvents(
              messageInstance,
              syntheticPopulationSpec,
              syntheticEventGroupSpec,
            )
            .toList()
            .flatMap { it.impressions.toList() }
            .filter { impression -> EventFilters.matches(impression.message, program) }
        }
      }
      .map { it.vid }
      .asFlow()
  }

  companion object {
    private const val DEFAULT_FILTER_EXPRESSION =
      "person.gender == ${Person.Gender.MALE_VALUE} && " +
        "(video_ad.viewed_fraction > 0.25 || video_ad.viewed_fraction == 0.25)"
    /** Default time range for events. */
    private val DEFAULT_EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
