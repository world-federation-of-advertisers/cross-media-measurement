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

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.time.Clock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as cmmsCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as cmmsAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup

@RunWith(JUnit4::class)
class EventGroupSyncTest {

  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader
  private lateinit var spanExporter: InMemorySpanExporter

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
  }

  @After
  fun cleanupTelemetry() {
    openTelemetry.close()
  }

  private fun getMetrics(): List<MetricData> {
    metricReader.forceFlush()
    return metricExporter.finishedMetricItems
  }

  private fun getSpans(): List<SpanData> {
    return spanExporter.finishedSpanItems
  }

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { updateEventGroup(any<UpdateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<UpdateEventGroupRequest>(0).eventGroup }
    onBlocking { createEventGroup(any<CreateEventGroupRequest>()) }
      .thenAnswer { invocation -> invocation.getArgument<CreateEventGroupRequest>(0).eventGroup }
    onBlocking { listEventGroups(any<ListEventGroupsRequest>()) }
      .thenAnswer {
        listEventGroupsResponse {
          eventGroups +=
            listOf(
              cmmsEventGroup {
                name = "dataProviders/data-provider-1/eventGroups/resource-id-1"
                measurementConsumer = "measurementConsumers/measurement-consumer-1"
                eventGroupReferenceId = "reference-id-1"
                mediaTypes += listOf("VIDEO", "DISPLAY").map { CmmsMediaType.valueOf(it) }
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-1"
                      campaignName = "campaign-1"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-2/eventGroups/resource-id-2"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-2"
                mediaTypes += listOf("OTHER").map { CmmsMediaType.valueOf(it) }
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-2"
                      campaignName = "campaign-2"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-3/eventGroups/resource-id-3"
                measurementConsumer = "measurementConsumers/measurement-consumer-2"
                eventGroupReferenceId = "reference-id-3"
                mediaTypes += listOf(CmmsMediaType.valueOf("OTHER"))
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "new-brand-name"
                      campaignName = "campaign-3"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
              cmmsEventGroup {
                name = "dataProviders/data-provider-3/eventGroups/resource-id-4"
                measurementConsumer = "measurementConsumers/measurement-consumer-other"
                eventGroupReferenceId = "reference-id-1"
                mediaTypes += listOf("VIDEO", "DISPLAY").map { CmmsMediaType.valueOf(it) }
                eventGroupMetadata = cmmsEventGroupMetadata {
                  this.adMetadata = cmmsAdMetadata {
                    this.campaignMetadata = cmmsCampaignMetadata {
                      brandName = "brand-1"
                      campaignName = "campaign-1"
                    }
                  }
                }
                dataAvailabilityInterval = interval {
                  startTime = timestamp { seconds = 200 }
                  endTime = timestamp { seconds = 300 }
                }
              },
            )
        }
      }
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(eventGroupsServiceMock) }

  @Test
  fun `sync registersUnregisteredEventGroups`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes +=
        listOf(MediaType.valueOf("OTHER"), MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
    }
    val testCampaigns = CAMPAIGNS + newCampaign
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `delete event group`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes +=
        listOf(MediaType.valueOf("OTHER"), MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
    }
    val testCampaigns = listOf(newCampaign)
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    val createCaptor = argumentCaptor<CreateEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(createCaptor.capture()) }
    assertThat(createCaptor.firstValue.eventGroup.eventGroupReferenceId).isEqualTo("reference-id-4")
    val deleteCaptor = argumentCaptor<DeleteEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(4)) { deleteEventGroup(deleteCaptor.capture()) }
    val deleteRequests = deleteCaptor.allValues
    assertThat(deleteRequests.map { it.name })
      .containsExactly(
        "dataProviders/data-provider-1/eventGroups/resource-id-1",
        "dataProviders/data-provider-2/eventGroups/resource-id-2",
        "dataProviders/data-provider-3/eventGroups/resource-id-3",
        "dataProviders/data-provider-3/eventGroups/resource-id-4",
      )
  }

  @Test
  fun `create new event group when measurement consumer differs`() {
    val newCampaign = eventGroup {
      eventGroupReferenceId = "reference-id-3"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-1"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes +=
        listOf(MediaType.valueOf("OTHER"), MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
    }
    val testCampaigns = listOf(newCampaign)
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(0)) { updateEventGroup(any()) }
  }

  @Test
  fun `sync updatesExistingEventGroups`() {
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        CAMPAIGNS.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { updateEventGroup(any()) }
  }

  @Test
  fun `sync returns Map Of Event Group Reference Ids To Event Groups`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "edp-name",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      val result = runBlocking { eventGroupSync.sync() }
      assertThat(result.toList().map { it.eventGroupResource to it.eventGroupReferenceId })
        .isEqualTo(
          listOf(
            "dataProviders/data-provider-1/eventGroups/resource-id-1" to "reference-id-1",
            "dataProviders/data-provider-2/eventGroups/resource-id-2" to "reference-id-2",
            "dataProviders/data-provider-3/eventGroups/resource-id-3" to "reference-id-3",
            "dataProviders/data-provider-3/eventGroups/resource-id-4" to "reference-id-1",
          )
        )
    }
  }

  @Test
  fun `throws exception if no media types`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id-4"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += emptyList<MediaType>()
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if no event group reference id`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = ""
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throw exceptions if do data availability`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "some-event-group-reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      measurementConsumer = "measurement-consumer-2"
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if no event group metadata`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "event-group-reference-id"
      measurementConsumer = "measurement-consumer-2"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `throws exception if empty measurement consumer`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "some-reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-2"
            campaign = "campaign-2"
          }
        }
      }
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
  }

  @Test
  fun `records sync_attempts metric for all event groups`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-123",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncAttemptsMetric = metrics.find { it.name == "edpa.event_group.sync_attempts" }
      assertThat(syncAttemptsMetric).isNotNull()
      assertThat(syncAttemptsMetric!!.description).isEqualTo("Number of Event Group sync attempts")

      val sumData = syncAttemptsMetric.longSumData
      // Should have 1 aggregated point for the data provider with total of 3 attempts
      assertThat(sumData.points).hasSize(1)
      val totalAttempts = sumData.points.sumOf { it.value }
      assertThat(totalAttempts).isEqualTo(4)

      // Verify attributes
      val firstPoint = sumData.points.first()
      assertThat(firstPoint.attributes.get(AttributeKey.stringKey("data_provider_name")))
        .isEqualTo("dataProviders/test-edp-123")
    }
  }

  @Test
  fun `records sync_success metric for successful syncs`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-456",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncSuccessMetric = metrics.find { it.name == "edpa.event_group.sync_success" }
      assertThat(syncSuccessMetric).isNotNull()
      assertThat(syncSuccessMetric!!.description)
        .isEqualTo("Number of successful Event Group syncs")

      val sumData = syncSuccessMetric.longSumData
      // Should have 1 aggregated point for the data provider with total of 3 successful syncs
      assertThat(sumData.points).hasSize(1)
      val totalSuccess = sumData.points.sumOf { it.value }
      assertThat(totalSuccess).isEqualTo(4)
    }
  }

  @Test
  fun `records sync_latency metric for each sync`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-789",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncLatencyMetric = metrics.find { it.name == "edpa.event_group.sync_latency" }
      assertThat(syncLatencyMetric).isNotNull()
      assertThat(syncLatencyMetric!!.description)
        .isEqualTo("Time to complete Event Group sync operation")
      assertThat(syncLatencyMetric.unit).isEqualTo("s")

      val histogramData = syncLatencyMetric.histogramData
      // Should have 1 aggregated point for the data provider with 3 recorded latencies
      assertThat(histogramData.points).hasSize(1)

      // Verify all latencies are recorded (may be 0 for very fast operations)
      val point = histogramData.points.first()
      assertThat(point.count).isEqualTo(4)
      assertThat(point.sum).isAtLeast(0.0)
      assertThat(point.attributes.get(AttributeKey.stringKey("data_provider_name")))
        .isEqualTo("dataProviders/test-edp-789")
    }
  }

  @Test
  fun `does not record success metrics on validation failure`() {
    runBlocking {
      val invalidEventGroup = eventGroup {
        eventGroupReferenceId = "invalid-ref-id"
        measurementConsumer = "" // Invalid - empty
        mediaTypes += listOf(MediaType.valueOf("OTHER"))
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
      }

      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-error",
          eventGroupsStub,
          listOf(invalidEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncAttemptsMetric = metrics.find { it.name == "edpa.event_group.sync_attempts" }
      val syncSuccessMetric = metrics.find { it.name == "edpa.event_group.sync_success" }
      val syncFailureMetric = metrics.find { it.name == "edpa.event_group.sync_failure" }

      // Attempt should be recorded
      assertThat(syncAttemptsMetric).isNotNull()
      assertThat(syncAttemptsMetric!!.longSumData.points.sumOf { it.value }).isEqualTo(1)

      // Failure should be recorded
      assertThat(syncFailureMetric).isNotNull()
      assertThat(syncFailureMetric!!.description).isEqualTo("Number of failed Event Group syncs")
      assertThat(syncFailureMetric.longSumData.points.sumOf { it.value }).isEqualTo(1)

      // But no success metrics should be recorded (validation failed)
      if (syncSuccessMetric != null) {
        assertThat(syncSuccessMetric.longSumData.points).isEmpty()
      }
    }
  }

  @Test
  fun `creates EventGroupSync span with correct attributes`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-123",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val spans = getSpans()
      val syncSpans = spans.filter { it.name == "EventGroupSync" }

      // Should have exactly one EventGroupSync span
      assertThat(syncSpans).hasSize(1)

      val syncSpan = syncSpans.first()
      assertThat(syncSpan.attributes.get(AttributeKey.stringKey("data_provider_name")))
        .isEqualTo("dataProviders/test-edp-123")
      assertThat(syncSpan.attributes.get(AttributeKey.stringKey("source"))).isEqualTo("kingdom")
      assertThat(syncSpan.status.statusCode).isEqualTo(StatusCode.OK)
    }
  }

  @Test
  fun `creates EventGroupSync Item span for each event group`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-456",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val spans = getSpans()
      val itemSpans = spans.filter { it.name == "EventGroupSync.Item" }

      // Should have one span per event group (3 in CAMPAIGNS)
      assertThat(itemSpans).hasSize(4)

      // All spans should have the correct attributes and status
      itemSpans.forEach { span ->
        assertThat(span.attributes.get(AttributeKey.stringKey("data_provider_name")))
          .isEqualTo("dataProviders/test-edp-456")
        assertThat(span.attributes.get(AttributeKey.stringKey("event_group_reference_id")))
          .isNotEmpty()
        assertThat(span.status.statusCode).isEqualTo(StatusCode.OK)
      }

      // Verify specific event group reference IDs
      val refIds =
        itemSpans
          .map { it.attributes.get(AttributeKey.stringKey("event_group_reference_id")) }
          .toSet()
      assertThat(refIds).containsExactly("reference-id-1", "reference-id-2", "reference-id-3")
    }
  }

  @Test
  fun `sets ERROR status on Item span when event group sync fails`() {
    runBlocking {
      val invalidEventGroup = eventGroup {
        eventGroupReferenceId = "invalid-ref-id"
        measurementConsumer = "" // Invalid - empty
        mediaTypes += listOf(MediaType.valueOf("OTHER"))
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
      }

      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-error",
          eventGroupsStub,
          listOf(invalidEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val spans = getSpans()
      val itemSpans = spans.filter { it.name == "EventGroupSync.Item" }

      assertThat(itemSpans).hasSize(1)

      val itemSpan = itemSpans.first()
      assertThat(itemSpan.status.statusCode).isEqualTo(StatusCode.ERROR)
      assertThat(itemSpan.attributes.get(AttributeKey.stringKey("event_group_reference_id")))
        .isEqualTo("invalid-ref-id")

      // Should have recorded an exception event
      assertThat(itemSpan.events).isNotEmpty()
      val exceptionEvents = itemSpan.events.filter { it.name == "exception" }
      assertThat(exceptionEvents).isNotEmpty()
    }
  }

  @Test
  fun `parent EventGroupSync span remains OK even when item fails`() {
    runBlocking {
      val invalidEventGroup = eventGroup {
        eventGroupReferenceId = "invalid-ref-id"
        measurementConsumer = "" // Invalid - empty
        mediaTypes += listOf(MediaType.valueOf("OTHER"))
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
      }

      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-parent-ok",
          eventGroupsStub,
          listOf(invalidEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val spans = getSpans()
      val syncSpans = spans.filter { it.name == "EventGroupSync" }

      assertThat(syncSpans).hasSize(1)

      // Parent span should be OK even though item failed
      val syncSpan = syncSpans.first()
      assertThat(syncSpan.status.statusCode).isEqualTo(StatusCode.OK)
    }
  }

  @Test
  fun `metrics do not contain event_group_reference_id attribute`() {
    runBlocking {
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-no-high-cardinality",
          eventGroupsStub,
          CAMPAIGNS.asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()

      // Check all EDPA event group metrics
      val edpaMetrics = metrics.filter { it.name.startsWith("edpa.event_group") }

      edpaMetrics.forEach { metric ->
        when (metric.name) {
          "edpa.event_group.sync_attempts",
          "edpa.event_group.sync_success",
          "edpa.event_group.sync_failure" -> {
            metric.longSumData.points.forEach { point ->
              // Should NOT have event_group_reference_id
              assertThat(point.attributes.get(AttributeKey.stringKey("event_group_reference_id")))
                .isNull()
              // Should have data_provider_name
              assertThat(point.attributes.get(AttributeKey.stringKey("data_provider_name")))
                .isNotNull()
            }
          }
          "edpa.event_group.sync_latency" -> {
            metric.histogramData.points.forEach { point ->
              // Should NOT have event_group_reference_id
              assertThat(point.attributes.get(AttributeKey.stringKey("event_group_reference_id")))
                .isNull()
              // Should have data_provider_name
              assertThat(point.attributes.get(AttributeKey.stringKey("data_provider_name")))
                .isNotNull()
            }
          }
        }
      }
    }
  }

  companion object {
    private val CAMPAIGNS =
      listOf(
        eventGroup {
          eventGroupReferenceId = "reference-id-1"
          measurementConsumer = "measurementConsumers/measurement-consumer-1"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-1"
                campaign = "campaign-1"
              }
            }
          }
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-2"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-2"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("OTHER"))
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-3"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-3"
              }
            }
          }
          measurementConsumer = "measurementConsumers/measurement-consumer-2"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("OTHER"))
        },
        eventGroup {
          eventGroupReferenceId = "reference-id-1"
          measurementConsumer = "measurementConsumers/measurement-consumer-other"
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-1"
                campaign = "campaign-1"
              }
            }
          }
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          mediaTypes += listOf(MediaType.valueOf("VIDEO"), MediaType.valueOf("DISPLAY"))
        },
      )
  }
}
