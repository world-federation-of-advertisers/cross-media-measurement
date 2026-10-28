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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteGrpc.BigQueryWriteImplBase
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest
import com.google.cloud.bigquery.storage.v1.WriteStream
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcCleanupRule
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.HighOverlapImpressionRow
import org.measurement.integration.k8s.testing.highOverlapImpressionRow
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.display
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.video
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

@RunWith(JUnit4::class)
class HighOverlapImpressionExporterTest {
  @get:Rule val grpcCleanup = GrpcCleanupRule()

  @Test
  fun `add exports video impression fields`() {
    val row = exportRow(VIDEO_EVENT)

    assertThat(row)
      .isEqualTo(
        highOverlapImpressionRow {
          modelLine = MODEL_LINE
          specFingerprint = SPEC_FINGERPRINT
          edpName = EDP_NAME
          logicalGroup = LOGICAL_GROUP
          entityType = "campaign"
          entityId = "test-campaign"
          cmmsEventGroupReferenceId = "campaign-test-campaign"
          vid = VID
          eventTime = 1_234_567_890L
          gender = "FEMALE"
          ageGroup = "YEARS_35_TO_54"
          usState = "CALIFORNIA"
          signedIn = true
          videoCompletedFraction = 0.75F
          videoViewableFraction = 0.5F
          displayViewableFraction = 0.0F
          mediaType = "VIDEO"
        }
      )
  }

  @Test
  fun `add exports display impression fields`() {
    val event = testEvent {
      common = VIDEO_EVENT.common
      display = display { viewableFraction = 0.25F }
    }

    val row = exportRow(event)

    assertThat(row.mediaType).isEqualTo("DISPLAY")
    assertThat(row.displayViewableFraction).isEqualTo(0.25F)
    assertThat(row.videoCompletedFraction).isEqualTo(0.0F)
    assertThat(row.videoViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `add identifies video with zero engagement fractions`() {
    val row = exportRow(VIDEO_EVENT.copy { video = video {} })

    assertThat(row.mediaType).isEqualTo("VIDEO")
    assertThat(row.videoCompletedFraction).isEqualTo(0.0F)
    assertThat(row.videoViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `add identifies display with zero viewable fraction`() {
    val row = exportRow(testEvent { display = display {} })

    assertThat(row.mediaType).isEqualTo("DISPLAY")
    assertThat(row.displayViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `add truncates timestamp precision below microseconds`() {
    val row = exportRow(VIDEO_EVENT, Instant.ofEpochSecond(0, 999))

    assertThat(row.eventTime).isEqualTo(0L)
  }

  @Test
  fun `add exports timestamps before the epoch in microseconds`() {
    val row = exportRow(VIDEO_EVENT, Instant.ofEpochSecond(-1, 123_456_789))

    assertThat(row.eventTime).isEqualTo(-876_544L)
  }

  @Test
  fun `add exports unspecified demographic enum names`() {
    val row = exportRow(testEvent { video = video {} })

    assertThat(row.gender).isEqualTo("GENDER_UNSPECIFIED")
    assertThat(row.ageGroup).isEqualTo("AGE_GROUP_UNSPECIFIED")
    assertThat(row.usState).isEqualTo("US_STATE_UNSPECIFIED")
    assertThat(row.signedIn).isFalse()
  }

  @Test
  fun `add rejects impressions with both video and display`() {
    val rows = withExporter { exporter ->
      val exception =
        assertFailsWith<IllegalArgumentException> {
          exporter.add(
            EDP_NAME,
            LOGICAL_GROUP,
            ENTITY_KEY,
            LabeledEvent(
              EVENT_TIME,
              VID,
              VIDEO_EVENT.copy { display = display { viewableFraction = 0.25F } },
            ),
          )
        }
      assertThat(exception.message).contains("exactly one")
      assertThat(exporter.rowCount).isEqualTo(0L)
    }

    assertThat(rows).isEmpty()
  }

  @Test
  fun `add rejects impressions without video or display`() {
    val rows = withExporter { exporter ->
      val exception =
        assertFailsWith<IllegalArgumentException> {
          exporter.add(
            EDP_NAME,
            LOGICAL_GROUP,
            ENTITY_KEY,
            LabeledEvent(EVENT_TIME, VID, testEvent {}),
          )
        }
      assertThat(exception.message).contains("exactly one")
      assertThat(exporter.rowCount).isEqualTo(0L)
    }

    assertThat(rows).isEmpty()
  }

  @Test
  fun `close exports all buffered impressions in order`() {
    val rows = withExporter { exporter ->
      exporter.add(
        EDP_NAME,
        LOGICAL_GROUP,
        ENTITY_KEY,
        LabeledEvent(EVENT_TIME, 101L, VIDEO_EVENT),
      )
      exporter.add(
        EDP_NAME,
        LOGICAL_GROUP,
        ENTITY_KEY,
        LabeledEvent(EVENT_TIME, 102L, testEvent { display = display {} }),
      )
      assertThat(exporter.rowCount).isEqualTo(2L)
    }

    assertThat(rows.map { it.vid }).containsExactly(101L, 102L).inOrder()
    assertThat(rows.map { it.mediaType }).containsExactly("VIDEO", "DISPLAY").inOrder()
  }

  private fun exportRow(
    message: TestEvent,
    timestamp: Instant = EVENT_TIME,
  ): HighOverlapImpressionRow {
    val rows = withExporter {
      it.add(EDP_NAME, LOGICAL_GROUP, ENTITY_KEY, LabeledEvent(timestamp, VID, message))
    }
    return rows.single()
  }

  private fun withExporter(
    action: (HighOverlapImpressionExporter) -> Unit
  ): List<HighOverlapImpressionRow> {
    val serverName: String = InProcessServerBuilder.generateName()
    // StreamWriter caches locations and connection pools across instances.
    val projectId = "test-$serverName"
    val service = InMemoryBigQueryWriteService(location = projectId)
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(service)
        .build()
        .start()
    )
    val channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
    val settings =
      BigQueryWriteSettings.newBuilder()
        .setTransportChannelProvider(
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        )
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build()
    BigQueryWriteClient.create(settings).use { writeClient ->
      HighOverlapImpressionExporter(
          bigQuery = mock<BigQuery>(),
          writeClient = writeClient,
          projectId = projectId,
          datasetId = "test_dataset",
          tableId = "test_table",
          modelLine = MODEL_LINE,
          specFingerprint = SPEC_FINGERPRINT,
        )
        .use(action)
    }
    return service.rows.toList()
  }

  private class InMemoryBigQueryWriteService(private val location: String) :
    BigQueryWriteImplBase() {
    val rows = ConcurrentLinkedQueue<HighOverlapImpressionRow>()

    override fun getWriteStream(
      request: GetWriteStreamRequest,
      responseObserver: StreamObserver<WriteStream>,
    ) {
      responseObserver.onNext(
        WriteStream.newBuilder().setName(request.name).setLocation(location).build()
      )
      responseObserver.onCompleted()
    }

    override fun appendRows(
      responseObserver: StreamObserver<AppendRowsResponse>
    ): StreamObserver<AppendRowsRequest> {
      return object : StreamObserver<AppendRowsRequest> {
        override fun onNext(request: AppendRowsRequest) {
          for (serializedRow in request.protoRows.rows.serializedRowsList) {
            rows.add(HighOverlapImpressionRow.parseFrom(serializedRow))
          }
          responseObserver.onNext(
            AppendRowsResponse.newBuilder()
              .setAppendResult(AppendRowsResponse.AppendResult.getDefaultInstance())
              .build()
          )
        }

        override fun onError(t: Throwable) {
          responseObserver.onError(t)
        }

        override fun onCompleted() {
          responseObserver.onCompleted()
        }
      }
    }
  }

  companion object {
    private const val MODEL_LINE =
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs"
    private const val SPEC_FINGERPRINT = "test-fingerprint"
    private const val EDP_NAME = "test-edp"
    private const val LOGICAL_GROUP = "test-group"
    private const val VID = 987L
    private val ENTITY_KEY = EntityKey(entityType = "campaign", entityId = "test-campaign")
    private val EVENT_TIME = Instant.ofEpochSecond(1234, 567_890_123)
    private val VIDEO_EVENT = testEvent {
      common = common {
        gender = Common.Gender.FEMALE
        ageGroup = Common.AgeGroup.YEARS_35_TO_54
        usState = Common.UsState.CALIFORNIA
        signedIn = true
      }
      video = video {
        completedFraction = 0.75F
        viewableFraction = 0.5F
      }
    }
  }
}
