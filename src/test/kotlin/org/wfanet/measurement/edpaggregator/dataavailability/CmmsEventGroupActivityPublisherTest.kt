/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import com.google.type.date
import com.google.type.interval
import io.grpc.Status
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.time.ZoneId
import java.util.Collections
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.BatchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesResponse
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.asRuntimeException
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as mappedEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class CmmsEventGroupActivityPublisherTest {
  private val throttler = RecordingThrottler()
  private val persistedActivityNames = Collections.synchronizedSet(mutableSetOf<String>())
  private val eventGroupActivitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { batchUpdateEventGroupActivities(any<BatchUpdateEventGroupActivitiesRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<BatchUpdateEventGroupActivitiesRequest>(0)
        for (child in request.requestsList) {
          persistedActivityNames += child.eventGroupActivity.name
        }
        batchUpdateEventGroupActivitiesResponse {}
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupActivitiesServiceMock) }

  private val eventGroupActivitiesStub: EventGroupActivitiesCoroutineStub by lazy {
    EventGroupActivitiesCoroutineStub(grpcTestServerRule.channel)
  }

  private val mapStorageClient = MesosRecordIoStorageClient(InMemoryStorageClient())

  @Test
  fun `publish writes every market date for every mapped EventGroup`() = runBlocking {
    seedMap(
      mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg1"),
      mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg2"),
    )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T07:30:00Z")
        endTime = Timestamps.parse("2026-01-02T08:30:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }

    val published =
      newPublisher(
          ZoneId.of("America/Los_Angeles"),
          EventGroupActivityPublisherMetrics(Instrumentation.meter),
        )
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(4)
    val updateRequest = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      batchUpdateEventGroupActivities(updateRequest.capture())
    }
    assertThat(updateRequest.firstValue.parent).isEqualTo("$DATA_PROVIDER/eventGroups/-")
    assertThat(updateRequest.firstValue.requestsList.map { it.eventGroupActivity.name })
      .containsExactly(
        "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-01",
        "$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-02",
        "$DATA_PROVIDER/eventGroups/eg2/eventGroupActivities/2026-01-01",
        "$DATA_PROVIDER/eventGroups/eg2/eventGroupActivities/2026-01-02",
      )
      .inOrder()
    assertThat(updateRequest.firstValue.requestsList.all { it.allowMissing }).isTrue()
  }

  @Test
  fun `publish deduplicates activity across metadata and entity keys`() = runBlocking {
    seedMap(
      mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg1"),
      mapping("ad", "456", "$DATA_PROVIDER/eventGroups/eg1"),
    )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T12:00:00Z")
        endTime = Timestamps.parse("2026-01-02T13:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
      entityKeys += entityKey {
        entityType = "ad"
        entityId = "456"
      }
    }

    val published =
      newPublisher(ZoneId.of("UTC"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata, metadata))

    assertThat(published).isEqualTo(1)
    val updateRequest = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      batchUpdateEventGroupActivities(updateRequest.capture())
    }
    assertThat(updateRequest.firstValue.requestsList.map { it.eventGroupActivity.name })
      .containsExactly("$DATA_PROVIDER/eventGroups/eg1/eventGroupActivities/2026-01-02")
    Unit
  }

  @Test
  fun `publish handles every mapped entity hierarchy level`() = runBlocking {
    seedMap(
      mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/campaign-eg"),
      mapping("ad_set", "456", "$DATA_PROVIDER/eventGroups/ad-set-eg"),
      mapping("ad", "789", "$DATA_PROVIDER/eventGroups/ad-eg"),
    )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T12:00:00Z")
        endTime = Timestamps.parse("2026-01-02T13:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
      entityKeys += entityKey {
        entityType = "ad_set"
        entityId = "456"
      }
      entityKeys += entityKey {
        entityType = "ad"
        entityId = "789"
      }
    }

    val published =
      newPublisher(ZoneId.of("UTC"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(3)
    val updateRequest = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      batchUpdateEventGroupActivities(updateRequest.capture())
    }
    assertThat(updateRequest.firstValue.requestsList.map { it.eventGroupActivity.name })
      .containsExactly(
        "$DATA_PROVIDER/eventGroups/campaign-eg/eventGroupActivities/2026-01-02",
        "$DATA_PROVIDER/eventGroups/ad-set-eg/eventGroupActivities/2026-01-02",
        "$DATA_PROVIDER/eventGroups/ad-eg/eventGroupActivities/2026-01-02",
      )
    Unit
  }

  @Test
  fun `publish batches EventGroups without exceeding activity pair limit`() = runBlocking {
    seedMap(
      *Array(501) { index -> mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg-$index") }
    )
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-01T23:30:00Z")
        endTime = Timestamps.parse("2026-01-02T00:30:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }

    val published =
      newPublisher(ZoneId.of("UTC"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(1002)
    val requests = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock, times(2)) {
      batchUpdateEventGroupActivities(requests.capture())
    }
    assertThat(requests.allValues.sumOf { it.requestsCount }).isEqualTo(1002)
    assertThat(requests.allValues.all { it.requestsCount <= 1000 }).isTrue()
    assertThat(throttler.onReadyCalls).isEqualTo(2)
  }

  @Test
  fun `publish skips stale EventGroup and retries remaining activities`() = runBlocking {
    val staleEventGroup = "$DATA_PROVIDER/eventGroups/stale-eg"
    val validEventGroup = "$DATA_PROVIDER/eventGroups/valid-eg"
    seedMap(
      mapping("campaign", "123", staleEventGroup),
      mapping("campaign", "123", validEventGroup),
    )
    val requests = mutableListOf<BatchUpdateEventGroupActivitiesRequest>()
    wheneverBlocking {
        eventGroupActivitiesServiceMock.batchUpdateEventGroupActivities(
          any<BatchUpdateEventGroupActivitiesRequest>()
        )
      }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<BatchUpdateEventGroupActivitiesRequest>(0)
        requests += request
        assertThat(throttler.onReadyCalls).isEqualTo(requests.size)
        if (
          request.requestsList.any {
            it.eventGroupActivity.name.startsWith("$staleEventGroup/eventGroupActivities/")
          }
        ) {
          throw Status.NOT_FOUND.asRuntimeException(
            errorInfo {
              domain = CMMS_ERROR_DOMAIN
              reason = EVENT_GROUP_NOT_FOUND_REASON
              metadata[EVENT_GROUP_NAME_METADATA_KEY] = staleEventGroup
            }
          )
        }
        batchUpdateEventGroupActivitiesResponse {}
      }
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T12:00:00Z")
        endTime = Timestamps.parse("2026-01-02T13:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }
    val metricsEnvironment = createMetricsEnvironment()

    try {
      val published =
        newPublisher(ZoneId.of("UTC"), metricsEnvironment.metrics).publish(listOf(metadata))

      assertThat(published).isEqualTo(1)
      assertThat(requests).hasSize(2)
      assertThat(throttler.onReadyCalls).isEqualTo(2)
      assertThat(requests[0].requestsList.map { it.eventGroupActivity.name })
        .containsExactly(
          "$staleEventGroup/eventGroupActivities/2026-01-02",
          "$validEventGroup/eventGroupActivities/2026-01-02",
        )
      assertThat(requests[1].requestsList.map { it.eventGroupActivity.name })
        .containsExactly("$validEventGroup/eventGroupActivities/2026-01-02")
      metricsEnvironment.metricReader.forceFlush()
      val metric =
        metricsEnvironment.metricExporter.finishedMetricItems.single {
          it.name == EVENT_GROUP_NOT_FOUND_METRIC
        }
      assertThat(metric.longSumData.points.single().value).isEqualTo(1)
    } finally {
      metricsEnvironment.close()
    }
  }

  @Test
  fun `publish includes both dates across daylight saving transition`() = runBlocking {
    seedMap(mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg1"))
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-03-08T07:30:00Z")
        endTime = Timestamps.parse("2026-03-08T10:30:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }

    newPublisher(
        ZoneId.of("America/Los_Angeles"),
        EventGroupActivityPublisherMetrics(Instrumentation.meter),
      )
      .publish(listOf(metadata))

    val request = argumentCaptor<BatchUpdateEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      batchUpdateEventGroupActivities(request.capture())
    }
    assertThat(request.firstValue.requestsList.map { it.eventGroupActivity.date })
      .containsExactly(
        date {
          year = 2026
          month = 3
          day = 7
        },
        date {
          year = 2026
          month = 3
          day = 8
        },
      )
      .inOrder()
  }

  @Test
  fun `publish preserves existing activity when adding a later date`() = runBlocking {
    seedMap(mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg1"))
    persistedActivityNames += EventGroupActivityKey("dp1", "eg1", "2026-01-01").toName()
    val metadata = impressionMetadata {
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T12:00:00Z")
        endTime = Timestamps.parse("2026-01-02T13:00:00Z")
      }
      entityKeys += entityKey {
        entityType = "campaign"
        entityId = "123"
      }
    }

    newPublisher(ZoneId.of("UTC"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
      .publish(listOf(metadata))

    assertThat(persistedActivityNames)
      .containsExactly(
        EventGroupActivityKey("dp1", "eg1", "2026-01-01").toName(),
        EventGroupActivityKey("dp1", "eg1", "2026-01-02").toName(),
      )
    Unit
  }

  @Test
  fun `publish skips map records without entity keys and records metric`() = runBlocking {
    seedMap(
      mappedEventGroup {
        eventGroupReferenceId = "legacy-reference-id"
        eventGroupResource = "$DATA_PROVIDER/eventGroups/legacy-eg"
      },
      mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg1"),
    )
    val metricsEnvironment = createMetricsEnvironment()
    try {
      val metadata = impressionMetadata {
        interval = interval {
          startTime = Timestamps.parse("2026-01-02T12:00:00Z")
          endTime = Timestamps.parse("2026-01-02T13:00:00Z")
        }
        entityKeys += entityKey {
          entityType = "campaign"
          entityId = "123"
        }
      }

      val published =
        newPublisher(ZoneId.of("UTC"), metricsEnvironment.metrics).publish(listOf(metadata))

      assertThat(published).isEqualTo(1)
      metricsEnvironment.metricReader.forceFlush()
      val metric =
        metricsEnvironment.metricExporter.finishedMetricItems.single {
          it.name == MISSING_MAP_ENTITY_KEY_METRIC
        }
      val point = metric.longSumData.points.single()
      assertThat(point.value).isEqualTo(1)
    } finally {
      metricsEnvironment.close()
    }
  }

  @Test
  fun `publish skips entity keys absent from map and records metric`() = runBlocking {
    seedMap(mapping("campaign", "other", "$DATA_PROVIDER/eventGroups/other-eg"))
    val metricsEnvironment = createMetricsEnvironment()
    try {
      val metadata = impressionMetadata {
        interval = interval {
          startTime = Timestamps.parse("2026-01-02T12:00:00Z")
          endTime = Timestamps.parse("2026-01-02T13:00:00Z")
        }
        entityKeys += entityKey {
          entityType = "campaign"
          entityId = "missing"
        }
      }

      val published =
        newPublisher(ZoneId.of("UTC"), metricsEnvironment.metrics).publish(listOf(metadata))

      assertThat(published).isEqualTo(0)
      metricsEnvironment.metricReader.forceFlush()
      val metric =
        metricsEnvironment.metricExporter.finishedMetricItems.single {
          it.name == UNMAPPED_METADATA_ENTITY_KEY_METRIC
        }
      assertThat(metric.longSumData.points.single().value).isEqualTo(1)
      verifyBlocking(eventGroupActivitiesServiceMock, never()) {
        batchUpdateEventGroupActivities(any())
      }
    } finally {
      metricsEnvironment.close()
    }
  }

  @Test
  fun `publish ignores unmapped entity key types`() = runBlocking {
    seedMap(mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/campaign-eg"))
    val metricsEnvironment = createMetricsEnvironment()
    try {
      val metadata = impressionMetadata {
        interval = interval {
          startTime = Timestamps.parse("2026-01-02T12:00:00Z")
          endTime = Timestamps.parse("2026-01-02T13:00:00Z")
        }
        entityKeys += entityKey {
          entityType = "unrelated"
          entityId = "456"
        }
      }

      val published =
        newPublisher(ZoneId.of("UTC"), metricsEnvironment.metrics).publish(listOf(metadata))

      assertThat(published).isEqualTo(0)
      metricsEnvironment.metricReader.forceFlush()
      assertThat(metricsEnvironment.metricExporter.finishedMetricItems.map { it.name })
        .doesNotContain(UNMAPPED_METADATA_ENTITY_KEY_METRIC)
      verifyBlocking(eventGroupActivitiesServiceMock, never()) {
        batchUpdateEventGroupActivities(any())
      }
    } finally {
      metricsEnvironment.close()
    }
  }

  @Test
  fun `publish ignores legacy EventGroup reference ID`() = runBlocking {
    seedMap(
      mappedEventGroup {
        eventGroupReferenceId = "legacy-reference-id"
        eventGroupResource = "$DATA_PROVIDER/eventGroups/legacy-eg"
      }
    )
    val metadata = impressionMetadata {
      eventGroupReferenceId = "legacy-reference-id"
      interval = interval {
        startTime = Timestamps.parse("2026-01-02T12:00:00Z")
        endTime = Timestamps.parse("2026-01-02T13:00:00Z")
      }
    }

    val published =
      newPublisher(ZoneId.of("UTC"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(0)
    verifyBlocking(eventGroupActivitiesServiceMock, never()) {
      batchUpdateEventGroupActivities(any())
    }
  }

  private suspend fun seedMap(vararg mappings: MappedEventGroup) {
    mapStorageClient.writeBlob(MAP_BLOB_KEY, flowOf(*mappings).map { it.toByteString() })
  }

  private fun mapping(
    entityType: String,
    entityId: String,
    eventGroupResource: String,
  ): MappedEventGroup = mappedEventGroup {
    this.eventGroupResource = eventGroupResource
    entityKey = mappedEntityKey {
      this.entityType = entityType
      this.entityId = entityId
    }
  }

  private fun newPublisher(
    marketTimeZone: ZoneId,
    metrics: EventGroupActivityPublisherMetrics,
  ): CmmsEventGroupActivityPublisher =
    CmmsEventGroupActivityPublisher(
      eventGroupActivitiesClient = eventGroupActivitiesStub,
      eventGroupMapStorageClient = mapStorageClient,
      eventGroupMapBlobKey = MAP_BLOB_KEY,
      dataProviderName = DATA_PROVIDER,
      marketTimeZone = marketTimeZone,
      metrics = metrics,
      throttler = throttler,
    )

  private class RecordingThrottler : Throttler {
    var onReadyCalls = 0
      private set

    override suspend fun <T> onReady(block: suspend () -> T): T {
      onReadyCalls++
      return block()
    }
  }

  private fun createMetricsEnvironment(): MetricsEnvironment {
    val metricExporter = InMemoryMetricExporter.create()
    val metricReader = PeriodicMetricReader.create(metricExporter)
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    return MetricsEnvironment(
      EventGroupActivityPublisherMetrics(meterProvider.get("event-group-activity-test")),
      metricExporter,
      metricReader,
      meterProvider,
    )
  }

  private data class MetricsEnvironment(
    val metrics: EventGroupActivityPublisherMetrics,
    val metricExporter: InMemoryMetricExporter,
    val metricReader: PeriodicMetricReader,
    val meterProvider: SdkMeterProvider,
  ) {
    fun close() {
      meterProvider.close()
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val MAP_BLOB_KEY = "event-groups/map.bin"
    private const val MISSING_MAP_ENTITY_KEY_METRIC =
      "edpa.event_group_activity.map_records_missing_entity_key"
    private const val UNMAPPED_METADATA_ENTITY_KEY_METRIC =
      "edpa.event_group_activity.unmapped_entity_keys"
    private const val EVENT_GROUP_NOT_FOUND_METRIC =
      "edpa.event_group_activity.event_groups_not_found"
    private const val CMMS_ERROR_DOMAIN = "halo.wfanet.org"
    private const val EVENT_GROUP_NOT_FOUND_REASON = "EVENT_GROUP_NOT_FOUND"
    private const val EVENT_GROUP_NAME_METADATA_KEY = "eventGroup"
  }
}
