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
import com.google.type.interval
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

/** Implementation of MeasurementConsumerSimulator for use with the EDP Aggregator. */
class EdpAggregatorMeasurementConsumerSimulator(
  private val measurementConsumerData: MeasurementConsumerData,
  outputDpParams: DifferentialPrivacyParams,
  dataProvidersClient: DataProvidersCoroutineStub,
  eventGroupsClient: EventGroupsCoroutineStub,
  measurementsClient: MeasurementsCoroutineStub,
  measurementConsumersClient: MeasurementConsumersCoroutineStub,
  certificatesClient: CertificatesCoroutineStub,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val messageInstance: Message,
  expectedDirectNoiseMechanism: NoiseMechanism,
  private val populationSpec: PopulationSpec,
  private val syntheticEventGroupMap: Map<String, EventGroupConfig>,
  reportName: String,
  modelLineName: String,
  private val filterExpression: String = DEFAULT_FILTER_EXPRESSION,
  initialResultPollingDelay: Duration = Duration.ofSeconds(1),
  maximumResultPollingDelay: Duration = Duration.ofMinutes(1),
  listEventGroupsEntityTypes: List<String>,
  onMeasurementsCreated: (() -> Unit)? = null,
  /**
   * Optional override that assigns the VID for each generated event when computing the expected
   * result. When null (default), the raw synthetic VID is used. The EDPA cloud test injects the
   * non-memoized labeler here so the expected reach/frequency is computed from the same VIDs the
   * pipeline assigns, matching the pre-staged and pipelined labeled data.
   */
  private val vidLabeler: ((LabeledEvent<*>) -> Long)? = null,
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
    initialResultPollingDelay,
    maximumResultPollingDelay,
    onMeasurementsCreated = onMeasurementsCreated,
    reportName = reportName,
    modelLineName = modelLineName,
    listEventGroupsEntityTypes = listEventGroupsEntityTypes,
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
    data class EventGroupRefInfo(
      val config: EventGroupConfig,
      val expression: String,
      val collectionInterval: Interval,
    )

    val eventGroupsByRefId: Map<String, EventGroupRefInfo> = buildMap {
      for (requisitionInfo in measurementInfo.requisitions) {
        for ((eventGroup, eventGroupEntry) in
          requisitionInfo.eventGroups.zip(requisitionInfo.requisitionSpec.events.eventGroupsList)) {
          if (
            targetDataProviderId != null &&
              targetDataProviderId !=
                requireNotNull(EventGroupKey.fromName(eventGroup.name)).dataProviderId
          ) {
            continue
          }
          val refId = eventGroup.eventGroupReferenceId
          if (refId in this) continue
          putIfAbsent(
            refId,
            EventGroupRefInfo(
              syntheticEventGroupMap.getValue(refId),
              eventGroupEntry.value.filter.expression,
              eventGroupEntry.value.collectionInterval,
            ),
          )
        }
      }
    }
    return eventGroupsByRefId.values
      .asSequence()
      .flatMap { (config, expression, collectionInterval) ->
        val program: Program =
          EventFilters.compileProgram(messageInstance.descriptorForType, expression)
        val specs: List<SyntheticEventGroupSpec> =
          when (config) {
            is EventGroupConfig.LegacySpec -> listOf(config.spec)
            is EventGroupConfig.MultiEntityKey -> config.entityKeySpecs.map { it.spec }
          }
        specs
          .asSequence()
          .flatMap { spec ->
            SyntheticDataGeneration.generateEvents(messageInstance, populationSpec, spec).flatMap {
              shard ->
              shard.labeledEvents
            }
          }
          .filter { impression -> EventFilters.matches(impression.message, program) }
          .filter { impression ->
            targetDataProviderId != null ||
              (impression.timestamp >= collectionInterval.startTime.toInstant() &&
                impression.timestamp < collectionInterval.endTime.toInstant())
          }
      }
      .map { vidLabeler?.invoke(it) ?: it.vid }
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

  override fun buildRequisitionInfo(
    dataProvider: DataProvider,
    eventGroups: List<EventGroup>,
    measurementConsumer: MeasurementConsumer,
    nonce: Long,
    percentage: Double,
  ): RequisitionInfo {
    val requisitionSpec = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          for (eventGroup in eventGroups) {
            this.eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = eventGroup.name
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = eventGroup.dataAvailabilityInterval.startTime
                      val durationMillis =
                        Duration.between(
                            eventGroup.dataAvailabilityInterval.startTime.toInstant(),
                            eventGroup.dataAvailabilityInterval.endTime.toInstant(),
                          )
                          .toMillis() * percentage
                      val requisitionEndTime =
                        (eventGroup.dataAvailabilityInterval.startTime
                            .toInstant()
                            .plusMillis(durationMillis.toLong()))
                          .toProtoTime()

                      endTime = requisitionEndTime
                    }
                    filter = RequisitionSpecKt.eventFilter { expression = filterExpression }
                  }
              }
          }
        }
      measurementPublicKey = measurementConsumer.publicKey.message
      this.nonce = nonce
    }
    val signedRequisitionSpec =
      signRequisitionSpec(requisitionSpec, measurementConsumerData.signingKey)
    val dataProviderEntry =
      dataProvider.toDataProviderEntry(signedRequisitionSpec, Hashing.hashSha256(nonce))

    return RequisitionInfo(dataProviderEntry, requisitionSpec, eventGroups)
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
