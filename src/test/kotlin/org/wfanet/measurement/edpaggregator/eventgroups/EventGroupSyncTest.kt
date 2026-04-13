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
import org.wfanet.measurement.api.v2alpha.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ClientAccountsGrpcKt.ClientAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as cmmsCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata as cmmsAdMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerClientAccountKey
import org.wfanet.measurement.api.v2alpha.MediaType as CmmsMediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.clientAccount
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata as cmmsEventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.State
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

  private val clientAccountsServiceMock: ClientAccountsCoroutineImplBase = mockService {
    onBlocking { listClientAccounts(any<ListClientAccountsRequest>()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<ListClientAccountsRequest>(0)
        when (request.filter.clientAccountReferenceId) {
          "client-ref-1" ->
            listClientAccountsResponse {
              clientAccounts += clientAccount {
                name =
                  MeasurementConsumerClientAccountKey("measurement-consumer-1", "client-account-1")
                    .toName()
                clientAccountReferenceId = "client-ref-1"
              }
            }
          "client-ref-multiple" ->
            listClientAccountsResponse {
              clientAccounts +=
                listOf(
                  clientAccount {
                    name =
                      MeasurementConsumerClientAccountKey(
                          "measurement-consumer-1",
                          "client-account-1",
                        )
                        .toName()
                    clientAccountReferenceId = "client-ref-multiple"
                  },
                  clientAccount {
                    name =
                      MeasurementConsumerClientAccountKey(
                          "measurement-consumer-2",
                          "client-account-2",
                        )
                        .toName()
                    clientAccountReferenceId = "client-ref-multiple"
                  },
                )
            }
          else -> listClientAccountsResponse {}
        }
      }
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val clientAccountsStub: ClientAccountsCoroutineStub by lazy {
    ClientAccountsCoroutineStub(grpcTestServerRule.channel)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(eventGroupsServiceMock)
    addService(clientAccountsServiceMock)
  }

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
      measurementConsumer = "measurementConsumers/measurement-consumer-2"
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
        clientAccountsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(any()) }
  }

  @Test
  fun `sync does not delete event groups missing from input`() {
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
      measurementConsumer = "measurementConsumers/measurement-consumer-2"
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
        clientAccountsStub,
        testCampaigns.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    runBlocking { eventGroupSync.sync().collect() }
    val createCaptor = argumentCaptor<CreateEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(createCaptor.capture()) }
    assertThat(createCaptor.firstValue.eventGroup.eventGroupReferenceId).isEqualTo("reference-id-4")
    verifyBlocking(eventGroupsServiceMock, times(0)) { deleteEventGroup(any()) }
  }

  @Test
  fun `sync deletes event groups with DELETED state`() {
    val deletedEventGroup = eventGroup {
      eventGroupReferenceId = "reference-id-1"
      measurementConsumer = "measurementConsumers/measurement-consumer-1"
      state = State.DELETED
    }
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(deletedEventGroup).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    val result = runBlocking { eventGroupSync.sync().toList() }
    val deleteCaptor = argumentCaptor<DeleteEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(1)) { deleteEventGroup(deleteCaptor.capture()) }
    assertThat(deleteCaptor.firstValue.name)
      .isEqualTo("dataProviders/data-provider-1/eventGroups/resource-id-1")
    verifyBlocking(eventGroupsServiceMock, times(0)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(0)) { updateEventGroup(any()) }
    assertThat(result).isEmpty()
  }

  @Test
  fun `sync deletes only the targeted MC event group when same ref id is mapped to multiple MCs`() {
    val deletedEventGroup = eventGroup {
      eventGroupReferenceId = "reference-id-1"
      measurementConsumer = "measurementConsumers/measurement-consumer-1"
      state = State.DELETED
    }
    val activeEventGroup = eventGroup {
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
    }
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(deletedEventGroup, activeEventGroup).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    val result = runBlocking { eventGroupSync.sync().toList() }

    val deleteCaptor = argumentCaptor<DeleteEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(1)) { deleteEventGroup(deleteCaptor.capture()) }
    assertThat(deleteCaptor.firstValue.name)
      .isEqualTo("dataProviders/data-provider-1/eventGroups/resource-id-1")

    verifyBlocking(eventGroupsServiceMock, times(0)) { createEventGroup(any()) }
    verifyBlocking(eventGroupsServiceMock, times(0)) { updateEventGroup(any()) }

    assertThat(result).hasSize(1)
    assertThat(result.first().eventGroupReferenceId).isEqualTo("reference-id-1")
    assertThat(result.first().eventGroupResource)
      .isEqualTo("dataProviders/data-provider-3/eventGroups/resource-id-4")
  }

  @Test
  fun `sync skips delete when DELETED event group not found in CMMS`() {
    val deletedEventGroup = eventGroup {
      eventGroupReferenceId = "reference-id-nonexistent"
      measurementConsumer = "measurementConsumers/measurement-consumer-1"
      state = State.DELETED
    }
    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(deletedEventGroup).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )
    val result = runBlocking { eventGroupSync.sync().toList() }
    verifyBlocking(eventGroupsServiceMock, times(0)) { deleteEventGroup(any()) }
    assertThat(result).isEmpty()
  }

  @Test
  fun `records sync_deleted metric when event group is deleted`() {
    runBlocking {
      val deletedEventGroup = eventGroup {
        eventGroupReferenceId = "reference-id-1"
        measurementConsumer = "measurementConsumers/measurement-consumer-1"
        state = State.DELETED
      }
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-delete-metric",
          eventGroupsStub,
          clientAccountsStub,
          listOf(deletedEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncDeletedMetric = metrics.find { it.name == "edpa.event_group.sync_deleted" }
      assertThat(syncDeletedMetric).isNotNull()
      assertThat(syncDeletedMetric!!.description)
        .isEqualTo("Number of Event Groups successfully deleted")
      assertThat(syncDeletedMetric.longSumData.points.sumOf { it.value }).isEqualTo(1)

      val firstPoint = syncDeletedMetric.longSumData.points.first()
      assertThat(firstPoint.attributes.get(AttributeKey.stringKey("data_provider_name")))
        .isEqualTo("dataProviders/test-edp-delete-metric")
    }
  }

  @Test
  fun `does not record sync_deleted metric when DELETED event group not found in CMMS`() {
    runBlocking {
      val deletedEventGroup = eventGroup {
        eventGroupReferenceId = "reference-id-nonexistent"
        measurementConsumer = "measurementConsumers/measurement-consumer-1"
        state = State.DELETED
      }
      val eventGroupSync =
        EventGroupSync(
          "dataProviders/test-edp-no-delete-metric",
          eventGroupsStub,
          clientAccountsStub,
          listOf(deletedEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncDeletedMetric = metrics.find { it.name == "edpa.event_group.sync_deleted" }
      if (syncDeletedMetric != null) {
        assertThat(syncDeletedMetric.longSumData.points).isEmpty()
      }
    }
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
      measurementConsumer = "measurementConsumers/measurement-consumer-1"
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
        clientAccountsStub,
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
        clientAccountsStub,
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
          clientAccountsStub,
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
          clientAccountsStub,
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
          clientAccountsStub,
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
          clientAccountsStub,
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
  fun `records only failure metrics when validation fails early`() {
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
          clientAccountsStub,
          listOf(invalidEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val syncAttemptsMetric = metrics.find { it.name == "edpa.event_group.sync_attempts" }
      val syncSuccessMetric = metrics.find { it.name == "edpa.event_group.sync_success" }
      val invalidEventGroupFailureMetric =
        metrics.find { it.name == "edpa.event_group.invalid_event_group_failure" }

      // No attempt should be recorded (validation happens before syncEventGroupItem)
      if (syncAttemptsMetric != null) {
        assertThat(syncAttemptsMetric.longSumData.points).isEmpty()
      }

      // Invalid event group failure should be recorded
      assertThat(invalidEventGroupFailureMetric).isNotNull()
      assertThat(invalidEventGroupFailureMetric!!.description)
        .isEqualTo("Number of Event Groups that failed validation")
      assertThat(invalidEventGroupFailureMetric.longSumData.points.sumOf { it.value }).isEqualTo(1)

      // No success metrics should be recorded (validation failed)
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
          clientAccountsStub,
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
          clientAccountsStub,
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
  fun `does not create Item span when validation fails early`() {
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
          clientAccountsStub,
          listOf(invalidEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )
      eventGroupSync.sync().collect()

      val spans = getSpans()
      val itemSpans = spans.filter { it.name == "EventGroupSync.Item" }

      // No Item span should be created (validation happens before syncEventGroupItem)
      assertThat(itemSpans).isEmpty()

      // But the main EventGroupSync span should exist
      val syncSpans = spans.filter { it.name == "EventGroupSync" }
      assertThat(syncSpans).hasSize(1)
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
          clientAccountsStub,
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
          clientAccountsStub,
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

  // ClientAccount resolution tests

  @Test
  fun `sync with client_account_reference_id resolves to measurement consumer`() {
    val eventGroupWithClientRef = eventGroup {
      eventGroupReferenceId = "reference-id-resolved"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-resolved"
            campaign = "campaign-resolved"
          }
        }
      }
      clientAccountReferenceId = "client-ref-1"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(eventGroupWithClientRef).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )

    runBlocking { eventGroupSync.sync().collect() }

    // Verify that ClientAccounts service was called
    verifyBlocking(clientAccountsServiceMock, times(1)) { listClientAccounts(any()) }

    // Verify that EventGroup was created with the resolved measurement consumer
    val createCaptor = argumentCaptor<CreateEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(1)) { createEventGroup(createCaptor.capture()) }
    assertThat(createCaptor.firstValue.eventGroup.measurementConsumer)
      .isEqualTo("measurementConsumers/measurement-consumer-1")
  }

  @Test
  fun `sync with both measurement_consumer and client_account_reference_id uses both`() {
    val eventGroupWithBoth = eventGroup {
      eventGroupReferenceId = "reference-id-both"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-both"
            campaign = "campaign-both"
          }
        }
      }
      measurementConsumer = "measurementConsumers/direct-consumer"
      clientAccountReferenceId = "client-ref-1"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(eventGroupWithBoth).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )

    val result = runBlocking { eventGroupSync.sync().toList() }

    // Verify that ClientAccounts service WAS called (both mappings are used)
    verifyBlocking(clientAccountsServiceMock, times(1)) { listClientAccounts(any()) }

    // Verify that EventGroups were created for BOTH the direct MC and the looked-up MC
    val createCaptor = argumentCaptor<CreateEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(2)) { createEventGroup(createCaptor.capture()) }

    val measurementConsumers = createCaptor.allValues.map { it.eventGroup.measurementConsumer }
    assertThat(measurementConsumers)
      .containsExactly(
        "measurementConsumers/direct-consumer",
        "measurementConsumers/measurement-consumer-1",
      )

    // Verify two results were returned
    assertThat(result).hasSize(2)
  }

  @Test
  fun `sync skips event group when client_account_reference_id has no match`() {
    val eventGroupWithNoMatch = eventGroup {
      eventGroupReferenceId = "reference-id-no-match"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-no-match"
            campaign = "campaign-no-match"
          }
        }
      }
      clientAccountReferenceId = "client-ref-nonexistent"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(eventGroupWithNoMatch).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )

    val result = runBlocking { eventGroupSync.sync().toList() }

    // Verify that ClientAccounts service was called
    verifyBlocking(clientAccountsServiceMock, times(1)) { listClientAccounts(any()) }

    // Verify that EventGroup was NOT created
    verifyBlocking(eventGroupsServiceMock, times(0)) { createEventGroup(any()) }

    // Verify no results were returned
    assertThat(result).isEmpty()
  }

  @Test
  fun `sync creates event groups for all measurement consumers when client_account_reference_id has multiple matches`() {
    val eventGroupWithMultiple = eventGroup {
      eventGroupReferenceId = "reference-id-multiple"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand-multiple"
            campaign = "campaign-multiple"
          }
        }
      }
      clientAccountReferenceId = "client-ref-multiple"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    val eventGroupSync =
      EventGroupSync(
        "edp-name",
        eventGroupsStub,
        clientAccountsStub,
        listOf(eventGroupWithMultiple).asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        100,
      )

    val result = runBlocking { eventGroupSync.sync().toList() }

    // Verify that ClientAccounts service was called
    verifyBlocking(clientAccountsServiceMock, times(1)) { listClientAccounts(any()) }

    // Verify that EventGroups were created for BOTH measurement consumers
    val createCaptor = argumentCaptor<CreateEventGroupRequest>()
    verifyBlocking(eventGroupsServiceMock, times(2)) { createEventGroup(createCaptor.capture()) }

    val measurementConsumers = createCaptor.allValues.map { it.eventGroup.measurementConsumer }
    assertThat(measurementConsumers)
      .containsExactly(
        "measurementConsumers/measurement-consumer-1",
        "measurementConsumers/measurement-consumer-2",
      )

    // Verify two results were returned (one for each measurement consumer)
    assertThat(result).hasSize(2)
  }

  @Test
  fun `records unmapped metric when event group cannot be resolved`() {
    runBlocking {
      val unmappableEventGroup = eventGroup {
        eventGroupReferenceId = "reference-id-unmapped"
        this.eventGroupMetadata = eventGroupMetadata {
          this.adMetadata = adMetadata {
            this.campaignMetadata = campaignMetadata {
              brand = "brand-unmapped"
              campaign = "campaign-unmapped"
            }
          }
        }
        clientAccountReferenceId = "client-ref-nonexistent"
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        }
        mediaTypes += listOf(MediaType.valueOf("OTHER"))
      }

      val eventGroupSync =
        EventGroupSync(
          "edp-name",
          eventGroupsStub,
          clientAccountsStub,
          listOf(unmappableEventGroup).asFlow(),
          MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          100,
        )

      eventGroupSync.sync().collect()

      val metrics = getMetrics()
      val unmappedMetric = metrics.find { it.name == "edpa.event_group.unmapped" }

      assertThat(unmappedMetric).isNotNull()
      assertThat(unmappedMetric!!.description)
        .isEqualTo("Number of Event Groups that could not be mapped to any MeasurementConsumer")
      assertThat(unmappedMetric.longSumData.points.sumOf { it.value }).isEqualTo(1)

      // Verify no EventGroup was created
      verifyBlocking(eventGroupsServiceMock, times(0)) { createEventGroup(any()) }
    }
  }

  @Test
  fun `does not delete event groups when account linkage is removed and fewer MCs are mapped`() {
    val clientAccountsMock: ClientAccountsCoroutineImplBase = mockService {
      onBlocking { listClientAccounts(any<ListClientAccountsRequest>()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<ListClientAccountsRequest>(0)
          when (request.filter.clientAccountReferenceId) {
            "client-ref-reduced" ->
              listClientAccountsResponse {
                clientAccounts += clientAccount {
                  name =
                    MeasurementConsumerClientAccountKey(
                        "measurement-consumer-1",
                        "client-account-1",
                      )
                      .toName()
                  clientAccountReferenceId = "client-ref-reduced"
                }
              }
            else -> listClientAccountsResponse {}
          }
        }
    }

    val eventGroupsMock: EventGroupsCoroutineImplBase = mockService {
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
                  name = "dataProviders/data-provider-1/eventGroups/resource-id-100"
                  measurementConsumer = "measurementConsumers/measurement-consumer-1"
                  eventGroupReferenceId = "reference-id-reduced"
                  mediaTypes += listOf(CmmsMediaType.valueOf("OTHER"))
                  eventGroupMetadata = cmmsEventGroupMetadata {
                    this.adMetadata = cmmsAdMetadata {
                      this.campaignMetadata = cmmsCampaignMetadata {
                        brandName = "brand-reduced"
                        campaignName = "campaign-reduced"
                      }
                    }
                  }
                  dataAvailabilityInterval = interval {
                    startTime = timestamp { seconds = 200 }
                    endTime = timestamp { seconds = 300 }
                  }
                },
                cmmsEventGroup {
                  name = "dataProviders/data-provider-1/eventGroups/resource-id-101"
                  measurementConsumer = "measurementConsumers/measurement-consumer-2"
                  eventGroupReferenceId = "reference-id-reduced"
                  mediaTypes += listOf(CmmsMediaType.valueOf("OTHER"))
                  eventGroupMetadata = cmmsEventGroupMetadata {
                    this.adMetadata = cmmsAdMetadata {
                      this.campaignMetadata = cmmsCampaignMetadata {
                        brandName = "brand-reduced"
                        campaignName = "campaign-reduced"
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

    val testRule = GrpcTestServerRule {
      addService(eventGroupsMock)
      addService(clientAccountsMock)
    }

    val statement =
      object : org.junit.runners.model.Statement() {
        override fun evaluate() {
          val eventGroupWithReducedMapping = eventGroup {
            eventGroupReferenceId = "reference-id-reduced"
            this.eventGroupMetadata = eventGroupMetadata {
              this.adMetadata = adMetadata {
                this.campaignMetadata = campaignMetadata {
                  brand = "brand-reduced"
                  campaign = "campaign-reduced"
                }
              }
            }
            clientAccountReferenceId = "client-ref-reduced"
            dataAvailabilityInterval = interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
            mediaTypes += listOf(MediaType.valueOf("OTHER"))
          }

          val eventGroupSync =
            EventGroupSync(
              "edp-name",
              EventGroupsCoroutineStub(testRule.channel),
              ClientAccountsCoroutineStub(testRule.channel),
              listOf(eventGroupWithReducedMapping).asFlow(),
              MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
              100,
            )

          runBlocking { eventGroupSync.sync().collect() }

          verifyBlocking(eventGroupsMock, times(0)) { updateEventGroup(any()) }
          verifyBlocking(eventGroupsMock, times(0)) { deleteEventGroup(any()) }
        }
      }

    testRule.apply(statement, org.junit.runner.Description.EMPTY).evaluate()
  }

  @Test
  fun `validateDeletedEventGroup accepts event group with reference id and measurement consumer`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id"
      measurementConsumer = "measurementConsumers/mc-1"
      state = State.DELETED
    }
    EventGroupSync.validateDeletedEventGroup(eventGroup)
  }

  @Test
  fun `validateDeletedEventGroup rejects event group with blank reference id`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = ""
      measurementConsumer = "measurementConsumers/mc-1"
      state = State.DELETED
    }
    assertFailsWith<IllegalStateException> { EventGroupSync.validateDeletedEventGroup(eventGroup) }
  }

  @Test
  fun `validateDeletedEventGroup rejects event group with no measurement consumer`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id"
      state = State.DELETED
    }
    val exception =
      assertFailsWith<IllegalStateException> {
        EventGroupSync.validateDeletedEventGroup(eventGroup)
      }
    assertThat(exception.message)
      .contains("Measurement Consumer must be set for deleted Event Groups")
  }

  @Test
  fun `validateDeletedEventGroup rejects event group with only client account reference id`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id"
      clientAccountReferenceId = "client-ref-1"
      state = State.DELETED
    }
    val exception =
      assertFailsWith<IllegalStateException> {
        EventGroupSync.validateDeletedEventGroup(eventGroup)
      }
    assertThat(exception.message)
      .contains("Measurement Consumer must be set for deleted Event Groups")
  }

  @Test
  fun `validateEventGroup accepts client_account_reference_id only`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand"
            campaign = "campaign"
          }
        }
      }
      clientAccountReferenceId = "client-ref"
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    // Should not throw an exception
    EventGroupSync.validateEventGroup(eventGroup)
  }

  @Test
  fun `validateEventGroup rejects event group with neither measurement_consumer nor client_account_reference_id`() {
    val eventGroup = eventGroup {
      eventGroupReferenceId = "reference-id"
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = "brand"
            campaign = "campaign"
          }
        }
      }
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      mediaTypes += listOf(MediaType.valueOf("OTHER"))
    }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupSync.validateEventGroup(eventGroup) }
    assertThat(exception.message)
      .contains("Either Measurement Consumer or Client Account Reference ID must be set")
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
