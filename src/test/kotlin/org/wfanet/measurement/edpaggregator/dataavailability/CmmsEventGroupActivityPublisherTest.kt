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
import com.google.type.date
import com.google.type.interval
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
import org.wfanet.measurement.api.v2alpha.AddEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.addEventGroupActivitiesResponse
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as mappedEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class CmmsEventGroupActivityPublisherTest {
  private val persistedActivityNames = Collections.synchronizedSet(mutableSetOf<String>())
  private val eventGroupActivitiesServiceMock: EventGroupActivitiesCoroutineImplBase = mockService {
    onBlocking { addEventGroupActivities(any<AddEventGroupActivitiesRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<AddEventGroupActivitiesRequest>(0)
        for (eventGroupName in request.eventGroupsList) {
          val eventGroupKey = checkNotNull(EventGroupKey.fromName(eventGroupName))
          for (activityDate in request.datesList) {
            persistedActivityNames +=
              EventGroupActivityKey(
                  eventGroupKey.dataProviderId,
                  eventGroupKey.eventGroupId,
                  "%04d-%02d-%02d"
                    .format(activityDate.year, activityDate.month, activityDate.day),
                )
                .toName()
          }
        }
        addEventGroupActivitiesResponse {}
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
          setOf("campaign"),
          EventGroupActivityPublisherMetrics(Instrumentation.meter),
        )
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(4)
    val addRequest = argumentCaptor<AddEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      addEventGroupActivities(addRequest.capture())
    }
    assertThat(addRequest.firstValue.parent).isEqualTo(DATA_PROVIDER)
    assertThat(addRequest.firstValue.eventGroupsList)
      .containsExactly("$DATA_PROVIDER/eventGroups/eg1", "$DATA_PROVIDER/eventGroups/eg2")
      .inOrder()
    assertThat(addRequest.firstValue.datesList)
      .containsExactly(
        date {
          year = 2026
          month = 1
          day = 1
        },
        date {
          year = 2026
          month = 1
          day = 2
        },
      )
      .inOrder()
  }

  @Test
  fun `publish handles every configured entity hierarchy level`() = runBlocking {
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
      newPublisher(
          ZoneId.of("UTC"),
          setOf("campaign", "ad_set", "ad"),
          EventGroupActivityPublisherMetrics(Instrumentation.meter),
        )
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(3)
    val addRequest = argumentCaptor<AddEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      addEventGroupActivities(addRequest.capture())
    }
    assertThat(addRequest.firstValue.eventGroupsList)
      .containsExactly(
        "$DATA_PROVIDER/eventGroups/campaign-eg",
        "$DATA_PROVIDER/eventGroups/ad-set-eg",
        "$DATA_PROVIDER/eventGroups/ad-eg",
      )
    Unit
  }

  @Test
  fun `publish batches EventGroups without exceeding activity pair limit`() = runBlocking {
    seedMap(
      *Array(501) { index ->
        mapping("campaign", "123", "$DATA_PROVIDER/eventGroups/eg-$index")
      }
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
      newPublisher(
          ZoneId.of("UTC"),
          setOf("campaign"),
          EventGroupActivityPublisherMetrics(Instrumentation.meter),
        )
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(1002)
    val requests = argumentCaptor<AddEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock, times(2)) {
      addEventGroupActivities(requests.capture())
    }
    assertThat(requests.allValues.sumOf { it.eventGroupsCount }).isEqualTo(501)
    assertThat(requests.allValues.all { it.eventGroupsCount * it.datesCount <= 1000 }).isTrue()
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
        setOf("campaign"),
        EventGroupActivityPublisherMetrics(Instrumentation.meter),
      )
      .publish(listOf(metadata))

    val request = argumentCaptor<AddEventGroupActivitiesRequest>()
    verifyBlocking(eventGroupActivitiesServiceMock) {
      addEventGroupActivities(request.capture())
    }
    assertThat(request.firstValue.datesList)
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

    newPublisher(ZoneId.of("UTC"), setOf("campaign"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
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
        newPublisher(ZoneId.of("UTC"), setOf("campaign"), metricsEnvironment.metrics)
          .publish(listOf(metadata))

      assertThat(published).isEqualTo(1)
      metricsEnvironment.metricReader.forceFlush()
      val metric =
        metricsEnvironment.metricExporter.finishedMetricItems.single {
          it.name == MISSING_ENTITY_KEY_METRIC
        }
      val point = metric.longSumData.points.single()
      assertThat(point.value).isEqualTo(1)
    } finally {
      metricsEnvironment.close()
    }
  }

  @Test
  fun `publish skips entity keys absent from map`() = runBlocking {
    seedMap(mapping("campaign", "other", "$DATA_PROVIDER/eventGroups/other-eg"))
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
      newPublisher(ZoneId.of("UTC"), setOf("campaign"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(0)
    verifyBlocking(eventGroupActivitiesServiceMock, never()) {
      addEventGroupActivities(any())
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
      newPublisher(ZoneId.of("UTC"), setOf("campaign"), EventGroupActivityPublisherMetrics(Instrumentation.meter))
        .publish(listOf(metadata))

    assertThat(published).isEqualTo(0)
    verifyBlocking(eventGroupActivitiesServiceMock, never()) {
      addEventGroupActivities(any())
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
    entityKeyTypes: Set<String>,
    metrics: EventGroupActivityPublisherMetrics,
  ): CmmsEventGroupActivityPublisher =
    CmmsEventGroupActivityPublisher(
      eventGroupActivitiesClient = eventGroupActivitiesStub,
      eventGroupMapStorageClient = mapStorageClient,
      eventGroupMapBlobKey = MAP_BLOB_KEY,
      dataProviderName = DATA_PROVIDER,
      entityKeyTypes = entityKeyTypes,
      marketTimeZone = marketTimeZone,
      metrics = metrics,
    )

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
    private const val MISSING_ENTITY_KEY_METRIC =
      "edpa.event_group_activity.map_records_missing_entity_key"
  }
}
