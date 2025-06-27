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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

/** Implementation of MeasurementConsumerSimulator for use with the EDP Aggregator. */
class EdpAggregatorMeasurementConsumerSimulator(
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

  /**
   * Filters a list of vids for a [MeasurementConsumerSimulator.MeasurementInfo]. Filters by
   * targetDataProviderId, if provided. Otherwise, filters by collection interval.
   */
  private fun getMeasurementFilteredVids(
    measurementInfo: MeasurementInfo,
    targetDataProviderId: String? = null,
  ): Sequence<Long> {
    val eventGroupSpecs: Sequence<Triple<SyntheticEventGroupSpec, String, Interval>> =
      measurementInfo.requisitions.asSequence().flatMap { requisitionInfo ->
        requisitionInfo.eventGroups
          .zip(requisitionInfo.requisitionSpec.events.eventGroupsList)
          .filter { (eventGroup, _) ->
            targetDataProviderId == null ||
              targetDataProviderId ==
                requireNotNull(EventGroupKey.fromName(eventGroup.name)).dataProviderId
          }
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
        val program: Program =
          EventFilters.compileProgram(messageInstance.descriptorForType, expression)
        SyntheticDataGeneration.generateEvents(
            messageInstance,
            syntheticPopulationSpec,
            syntheticEventGroupSpec,
          )
          .flatMap { it.labeledEvents }
          .filter { impression -> EventFilters.matches(impression.message, program) }
          .filter { impression ->
            targetDataProviderId != null ||
              (impression.timestamp >= collectionInterval.startTime.toInstant() &&
                impression.timestamp < collectionInterval.endTime.toInstant())
          }
      }
      .map { it.vid }
  }

  override fun getFilteredVids(measurementInfo: MeasurementInfo): Sequence<Long> {
    return getMeasurementFilteredVids(measurementInfo, null)
  }

  override fun getFilteredVids(
    measurementInfo: MeasurementInfo,
    targetDataProviderId: String,
  ): Sequence<Long> {
    return getMeasurementFilteredVids(measurementInfo, targetDataProviderId)
  }

  companion object {
    private const val DEFAULT_FILTER_EXPRESSION =
      "person.gender == ${Person.Gender.MALE_VALUE} && " +
        "(video_ad.viewed_fraction > 0.25 || video_ad.viewed_fraction == 0.25)"
    /** Default time range for events. */
    private val DEFAULT_EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))
  }
}
