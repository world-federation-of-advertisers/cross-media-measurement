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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.query
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.vectorSet
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Tests a TrusTEE v2 multimeasurement end to end: the `MeasurementConsumer` creates it through the
 * Kingdom public API, the EDP Aggregator fulfills one `Requisition` per base vector, and the
 * TrusTEE Duchy evaluates the `Query` over those vectors.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessEdpAggregatorMultimeasurementTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
) :
  InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    hmssEnabled = false,
    trusTeeEnabled = true,
    // Makes every DataProvider fulfill without noise, so the expected result is exact.
    resultMinimumThresholdsByEdp = SUPPRESSING_RESULT_MINIMUM_THRESHOLDS_BY_EDP,
  ) {

  private val measurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withAuthenticationKey(mcApiKey)
  }
  private val measurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withAuthenticationKey(mcApiKey)
  }

  @Test
  fun `createMeasurement for a TrusTEE v2 multimeasurement completes with exact results`() =
    runBlocking {
      val measurementConsumer: MeasurementConsumer =
        measurementConsumersClient.getMeasurementConsumer(
          getMeasurementConsumerRequest { name = mcName }
        )
      val eventGroups: List<EventGroup> = listVectorEventGroups()
      val request = createMeasurementRequest {
        parent = mcName
        measurement = measurement {
          measurementConsumerCertificate = measurementConsumer.certificate
          unsignedMeasurementSpec = measurementSpec {
            measurementPublicKey = measurementConsumer.publicKey.message
            multi = MeasurementSpecKt.multiMeasurementSpec {}
            vidSamplingInterval =
              MeasurementSpecKt.vidSamplingInterval {
                start = 0.0f
                width = 1.0f
              }
            modelLine = modelLineName
            // The EDP Aggregator groups requisitions by report.
            reportingMetadata =
              MeasurementSpecKt.reportingMetadata { report = "$mcName/reports/$REPORT_ID" }
          }
          multimeasurement =
            MeasurementKt.multimeasurement {
              for ((index, eventGroup) in eventGroups.withIndex()) {
                dataProviderVectors +=
                  MeasurementKt.MultimeasurementKt.dataProviderVectors {
                    dataProvider =
                      checkNotNull(EventGroupKey.fromName(eventGroup.name)).parentKey.toName()
                    vectors +=
                      MeasurementKt.MultimeasurementKt.vector {
                        this.index = index
                        requisitionSpec = buildRequisitionSpec(measurementConsumer, eventGroup)
                      }
                  }
              }
              queries += query {
                id = QUERY_ID
                summands = vectorSet { baseVectors += eventGroups.indices }
                maxFrequency = MAX_FREQUENCY
              }
            }
          measurementReferenceId = "trustee-v2-multimeasurement-1234"
        }
      }

      val created: Measurement = measurementsClient.createMeasurement(request)
      val succeeded: Measurement = pollUntilSucceeded(created.name)

      val result: Measurement.Result.MultimeasurementResult =
        succeeded.resultsList.single().result.multimeasurementResult
      val queryResult = result.queryResultsList.single()
      assertThat(queryResult.queryId).isEqualTo(QUERY_ID)
      assertThat(queryResult.cellsList.single().reach).isEqualTo(computeExpectedReach(eventGroups))
    }

  /** Lists the `EventGroup`s backing the base vectors, in base vector index order. */
  private suspend fun listVectorEventGroups(): List<EventGroup> {
    val response =
      publicEventGroupsClient
        .withAuthenticationKey(mcApiKey)
        .listEventGroups(
          listEventGroupsRequest {
            parent = mcName
            pageSize = EVENT_GROUP_PAGE_SIZE
            filter = ListEventGroupsRequestKt.filter { entityTypeIn += EVENT_GROUP_ENTITY_TYPES }
          }
        )
    val eventGroupsByReferenceId: Map<String, EventGroup> =
      response.eventGroupsList.associateBy { it.eventGroupReferenceId }
    return VECTOR_EVENT_GROUP_REF_IDS.map { eventGroupsByReferenceId.getValue(it) }
  }

  private fun buildRequisitionSpec(
    measurementConsumer: MeasurementConsumer,
    eventGroup: EventGroup,
  ): RequisitionSpec {
    return requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups +=
            RequisitionSpecKt.eventGroupEntry {
              key = eventGroup.name
              value =
                RequisitionSpecKt.EventGroupEntryKt.value {
                  collectionInterval = eventGroup.dataAvailabilityInterval
                }
            }
        }
      measurementPublicKey = measurementConsumer.publicKey.message
    }
  }

  private suspend fun pollUntilSucceeded(measurementName: String): Measurement {
    while (true) {
      val measurement: Measurement =
        measurementsClient.getMeasurement(getMeasurementRequest { name = measurementName })
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
      when (measurement.state) {
        Measurement.State.SUCCEEDED -> return measurement
        Measurement.State.FAILED ->
          error(
            "Measurement failed with reason ${measurement.failure.reason}: " +
              measurement.failure.message
          )
        Measurement.State.CANCELLED -> error("Measurement cancelled")
        Measurement.State.AWAITING_REQUISITION_FULFILLMENT,
        Measurement.State.COMPUTING -> delay(POLLING_DELAY.toMillis())
        Measurement.State.STATE_UNSPECIFIED,
        Measurement.State.UNRECOGNIZED -> error("Unexpected Measurement state ${measurement.state}")
      }
    }
  }

  /**
   * Returns the number of distinct VIDs across [eventGroups] within their collection intervals.
   *
   * Every vector [EventGroup] is generated from [syntheticEventGroupSpec].
   */
  private fun computeExpectedReach(eventGroups: List<EventGroup>): Long {
    val vids: Set<Long> = buildSet {
      for (eventGroup in eventGroups) {
        val startTime = eventGroup.dataAvailabilityInterval.startTime.toInstant()
        val endTime = eventGroup.dataAvailabilityInterval.endTime.toInstant()
        SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            syntheticEventGroupSpec,
          )
          .flatMap { it.labeledEvents }
          .filter { it.timestamp >= startTime && it.timestamp < endTime }
          .forEach { add(it.vid) }
      }
    }
    return vids.size.toLong()
  }

  companion object {
    private const val REPORT_ID = "trustee-v2-multimeasurement"
    private const val QUERY_ID = "reach-of-all-vectors"
    private const val MAX_FREQUENCY = 10
    private const val EVENT_GROUP_PAGE_SIZE = 100
    private val POLLING_DELAY: Duration = Duration.ofSeconds(1)
    private val EVENT_GROUP_ENTITY_TYPES = listOf("campaign", "ad_group", "creative-id")

    /** One base vector per entry, from two TrusTEE-capable DataProviders (edp2 and edp3). */
    private val VECTOR_EVENT_GROUP_REF_IDS =
      listOf("ad_group/edpa-eg-reference-id-2", "ad_group/edpa-eg-reference-id-3")
  }
}
