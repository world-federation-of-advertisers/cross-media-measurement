/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.job

import com.google.api.core.ApiFutures
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.TableResult
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.cloud.bigquery.storage.v1.TableSchema
import com.google.cloud.bigquery.storage.v1.WriteStream
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import com.google.rpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.bigquerytables.ComputationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.measurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.requisitionsTableRow
import org.wfanet.measurement.internal.kingdom.computationParticipant
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntryStageAttempt
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementDetails
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntryDetails
import org.wfanet.measurement.internal.kingdom.measurementLogEntryError
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

@RunWith(JUnit4::class)
class OperationalMetricsExportTest {
  private val measurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {
    onBlocking { streamMeasurements(any()) }
      .thenReturn(flowOf(DIRECT_MEASUREMENT, COMPUTATION_MEASUREMENT))
  }

  private val requisitionsMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase = mockService {
    onBlocking { streamRequisitions(any()) }.thenReturn(flowOf(REQUISITION, REQUISITION_2))
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementsMock)
    addService(requisitionsMock)
  }

  private lateinit var measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub
  private lateinit var requisitionsClient: RequisitionsGrpcKt.RequisitionsCoroutineStub

  private val bigQueryWriteClientMock: BigQueryWriteClient = mock { bigQueryWriteClient ->
    val writeStreamMock: WriteStream = mock { writeStream ->
      whenever(writeStream.tableSchema).thenReturn(TableSchema.getDefaultInstance())
      whenever(writeStream.location).thenReturn("LOCATION")
    }
    whenever(bigQueryWriteClient.settings).thenReturn(BigQueryWriteSettings.newBuilder().build())
    whenever(
        bigQueryWriteClient.getWriteStream(ArgumentMatchers.isA(GetWriteStreamRequest::class.java))
      )
      .thenReturn(writeStreamMock)
  }

  private lateinit var measurementsStreamWriterMock: StreamWriter
  private lateinit var requisitionsStreamWriterMock: StreamWriter
  private lateinit var computationParticipantStagesStreamWriterMock: StreamWriter
  private lateinit var latestMeasurementReadStreamWriterMock: StreamWriter
  private lateinit var latestRequisitionReadStreamWriterMock: StreamWriter
  private lateinit var latestComputationReadStreamWriterMock: StreamWriter

  private lateinit var streamWriterFactoryTestImpl: StreamWriterFactory

  @Before
  fun init() {
    measurementsClient = MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel)
    requisitionsClient = RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)

    measurementsStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    requisitionsStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    computationParticipantStagesStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    latestMeasurementReadStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    latestRequisitionReadStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    latestComputationReadStreamWriterMock = mock {
      whenever(it.append(any()))
        .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))
      whenever(it.isClosed).thenReturn(false)
    }

    streamWriterFactoryTestImpl =
      StreamWriterFactory {
        _: String,
        _: String,
        tableId: String,
        _: BigQueryWriteClient,
        _: ProtoSchema ->
        when (tableId) {
          MEASUREMENTS_TABLE_ID -> measurementsStreamWriterMock
          REQUISITIONS_TABLE_ID -> requisitionsStreamWriterMock
          COMPUTATION_PARTICIPANT_STAGES_TABLE_ID -> computationParticipantStagesStreamWriterMock
          else -> mock {}
        }
      }
  }

  @Test
  fun `job successfully creates protos for appending to streams`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<ProtoRows>()) {
      verify(measurementsStreamWriterMock, times(2)).append(capture())

      val firstProtoRows: ProtoRows = firstValue
      assertThat(firstProtoRows.serializedRowsList).hasSize(1)

      val directMeasurementTableRow =
        MeasurementsTableRow.parseFrom(firstProtoRows.serializedRowsList[0])
      assertThat(directMeasurementTableRow)
        .isEqualTo(
          measurementsTableRow {
            measurementConsumerId =
              externalIdToApiId(DIRECT_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(DIRECT_MEASUREMENT.externalMeasurementId)
            isDirect = true
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = MeasurementsTableRow.State.SUCCEEDED
            createTime = DIRECT_MEASUREMENT.createTime
            updateTime = DIRECT_MEASUREMENT.updateTime
            completionDurationSeconds =
              Duration.between(
                  DIRECT_MEASUREMENT.createTime.toInstant(),
                  DIRECT_MEASUREMENT.updateTime.toInstant(),
                )
                .seconds
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(DIRECT_MEASUREMENT.updateTime)
          }
        )

      val secondProtoRows: ProtoRows = lastValue
      assertThat(secondProtoRows.serializedRowsList).hasSize(1)

      val computationMeasurementTableRow =
        MeasurementsTableRow.parseFrom(secondProtoRows.serializedRowsList[0])
      assertThat(computationMeasurementTableRow)
        .isEqualTo(
          measurementsTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            isDirect = false
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = MeasurementsTableRow.State.SUCCEEDED
            createTime = COMPUTATION_MEASUREMENT.createTime
            updateTime = COMPUTATION_MEASUREMENT.updateTime
            completionDurationSeconds =
              Duration.between(
                  COMPUTATION_MEASUREMENT.createTime.toInstant(),
                  COMPUTATION_MEASUREMENT.updateTime.toInstant(),
                )
                .seconds
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(requisitionsStreamWriterMock, times(2)).append(capture())

      val firstProtoRows: ProtoRows = firstValue
      assertThat(firstProtoRows.serializedRowsList).hasSize(1)

      val requisitionTableRow = RequisitionsTableRow.parseFrom(firstProtoRows.serializedRowsList[0])
      assertThat(requisitionTableRow)
        .isEqualTo(
          requisitionsTableRow {
            measurementConsumerId = externalIdToApiId(REQUISITION.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(REQUISITION.externalMeasurementId)
            requisitionId = externalIdToApiId(REQUISITION.externalRequisitionId)
            dataProviderId = externalIdToApiId(REQUISITION.externalDataProviderId)
            isDirect = false
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = RequisitionsTableRow.State.FULFILLED
            createTime = REQUISITION.parentMeasurement.createTime
            updateTime = REQUISITION.updateTime
            completionDurationSeconds =
              Duration.between(
                  REQUISITION.parentMeasurement.createTime.toInstant(),
                  REQUISITION.updateTime.toInstant(),
                )
                .seconds
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(REQUISITION.updateTime)
          }
        )

      val secondProtoRows: ProtoRows = lastValue
      assertThat(secondProtoRows.serializedRowsList).hasSize(1)

      val requisition2TableRow =
        RequisitionsTableRow.parseFrom(secondProtoRows.serializedRowsList[0])
      assertThat(requisition2TableRow)
        .isEqualTo(
          requisitionsTableRow {
            measurementConsumerId = externalIdToApiId(REQUISITION_2.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(REQUISITION_2.externalMeasurementId)
            requisitionId = externalIdToApiId(REQUISITION_2.externalRequisitionId)
            dataProviderId = externalIdToApiId(REQUISITION_2.externalDataProviderId)
            isDirect = true
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            state = RequisitionsTableRow.State.FULFILLED
            createTime = REQUISITION_2.parentMeasurement.createTime
            updateTime = REQUISITION_2.updateTime
            completionDurationSeconds =
              Duration.between(
                  REQUISITION_2.parentMeasurement.createTime.toInstant(),
                  REQUISITION_2.updateTime.toInstant(),
                )
                .seconds
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(REQUISITION_2.updateTime)
          }
        )
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(computationParticipantStagesStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(4)

      val stageOneTableRow =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[0])

      assertThat(stageOneTableRow)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "0"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_ONE
            stageStartTime = timestamp { seconds = 100 }
            completionDurationSeconds = 200
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageTwoTableRow =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[1])

      assertThat(stageTwoTableRow)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "0"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_TWO
            stageStartTime = timestamp { seconds = 300 }
            completionDurationSeconds = 300
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageOneTableRow2 =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[2])

      assertThat(stageOneTableRow2)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "1"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_ONE
            stageStartTime = timestamp { seconds = 100 }
            completionDurationSeconds = 200
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageTwoTableRow2 =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[3])

      assertThat(stageTwoTableRow2)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "1"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_TWO
            stageStartTime = timestamp { seconds = 300 }
            completionDurationSeconds = 300
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )
    }
  }

  @Test
  fun `job successfully creates proto for stages when measurement failed`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsMock.streamMeasurements(any()))
      .thenReturn(flowOf(COMPUTATION_MEASUREMENT.copy { state = Measurement.State.FAILED }))

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<ProtoRows>()) {
      verify(computationParticipantStagesStreamWriterMock).append(capture())

      val protoRows: ProtoRows = allValues.first()
      assertThat(protoRows.serializedRowsList).hasSize(4)

      val stageOneTableRow =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[0])

      assertThat(stageOneTableRow)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "0"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_ONE
            stageStartTime = timestamp { seconds = 100 }
            completionDurationSeconds = 200
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageTwoTableRow =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[1])

      assertThat(stageTwoTableRow)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "0"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.FAILED
            stageName = STAGE_TWO
            stageStartTime = timestamp { seconds = 300 }
            completionDurationSeconds = 300
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageOneTableRow2 =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[2])

      assertThat(stageOneTableRow2)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "1"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
            stageName = STAGE_ONE
            stageStartTime = timestamp { seconds = 100 }
            completionDurationSeconds = 200
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )

      val stageTwoTableRow2 =
        ComputationParticipantStagesTableRow.parseFrom(protoRows.serializedRowsList[3])

      assertThat(stageTwoTableRow2)
        .isEqualTo(
          computationParticipantStagesTableRow {
            measurementConsumerId =
              externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementConsumerId)
            measurementId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalMeasurementId)
            computationId = externalIdToApiId(COMPUTATION_MEASUREMENT.externalComputationId)
            duchyId = "1"
            measurementType = MeasurementType.REACH_AND_FREQUENCY
            result = ComputationParticipantStagesTableRow.Result.FAILED
            stageName = STAGE_TWO
            stageStartTime = timestamp { seconds = 300 }
            completionDurationSeconds = 300
            completionDurationSecondsSquared = completionDurationSeconds * completionDurationSeconds
            computationUpdateTimeNanoseconds = Timestamps.toNanos(COMPUTATION_MEASUREMENT.updateTime)
          }
        )
    }
  }

  @Test
  fun `job can process the next batch of measurements without starting at the beginning`() =
    runBlocking {
      val directMeasurement = DIRECT_MEASUREMENT

      val updateTimeNanosecondsFieldValue: FieldValue =
        FieldValue.of(
          FieldValue.Attribute.PRIMITIVE,
          "${Timestamps.toNanos(directMeasurement.updateTime)}",
        )

      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll())
          .thenReturn(
            listOf(
              FieldValueList.of(mutableListOf(updateTimeNanosecondsFieldValue), MEASUREMENT_FIELD_LIST)
            )
          )
          .thenReturn(emptyList())
      }

      whenever(measurementsMock.streamMeasurements(any()))
        .thenReturn(flowOf(COMPUTATION_MEASUREMENT))

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      operationalMetricsExport.execute()

      with(argumentCaptor<StreamMeasurementsRequest>()) {
        verify(measurementsMock, times(2)).streamMeasurements(capture())

        val streamMeasurementsRequest = firstValue
        assertThat(streamMeasurementsRequest)
          .ignoringRepeatedFieldOrder()
          .isEqualTo(
            streamMeasurementsRequest {
              measurementView = Measurement.View.DEFAULT
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += Measurement.State.SUCCEEDED
                  states += Measurement.State.FAILED
                  updatedAfter = directMeasurement.updateTime
                }
              limit = 1000
            }
          )
      }
    }

  @Test
  fun `job uses specified batch size for measurements`() = runBlocking {
    val directMeasurement = DIRECT_MEASUREMENT

    val updateTimeNanosecondsFieldValue: FieldValue =
      FieldValue.of(
        FieldValue.Attribute.PRIMITIVE,
        "${Timestamps.toNanos(directMeasurement.updateTime)}",
      )

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll())
        .thenReturn(
          listOf(
            FieldValueList.of(mutableListOf(updateTimeNanosecondsFieldValue), MEASUREMENT_FIELD_LIST)
          )
        )
        .thenReturn(emptyList())
    }

    whenever(measurementsMock.streamMeasurements(any())).thenReturn(flowOf(COMPUTATION_MEASUREMENT))

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
        batchSize = 100,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<StreamMeasurementsRequest>()) {
      verify(measurementsMock, times(2)).streamMeasurements(capture())
      val streamMeasurementsRequest = allValues.first()

      assertThat(streamMeasurementsRequest)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          streamMeasurementsRequest {
            measurementView = Measurement.View.DEFAULT
            filter =
              StreamMeasurementsRequestKt.filter {
                states += Measurement.State.SUCCEEDED
                states += Measurement.State.FAILED
                updatedAfter = directMeasurement.updateTime
              }
            limit = 100
          }
        )
    }
  }

  @Test
  fun `job can process the next batch of requisitions without starting at the beginning`() =
    runBlocking {
      val requisition = REQUISITION

      val updateTimeNanosecondsFieldValue: FieldValue =
        FieldValue.of(
          FieldValue.Attribute.PRIMITIVE,
          "${Timestamps.toNanos(requisition.updateTime)}",
        )

      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll())
          .thenReturn(emptyList())
          .thenReturn(
            listOf(
              FieldValueList.of(mutableListOf(updateTimeNanosecondsFieldValue), REQUISITION_FIELD_LIST)
            )
          )
          .thenReturn(emptyList())
      }

      whenever(requisitionsMock.streamRequisitions(any())).thenReturn(flowOf(REQUISITION_2))

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      operationalMetricsExport.execute()

      with(argumentCaptor<StreamRequisitionsRequest>()) {
        verify(requisitionsMock).streamRequisitions(capture())
        val streamRequisitionsRequest = firstValue

        assertThat(streamRequisitionsRequest)
          .ignoringRepeatedFieldOrder()
          .isEqualTo(
            streamRequisitionsRequest {
              filter =
                StreamRequisitionsRequestKt.filter {
                  states += Requisition.State.FULFILLED
                  states += Requisition.State.REFUSED
                  updatedAfter = requisition.updateTime
                }
              limit = 1000
            }
          )
      }
    }

  @Test
  fun `job uses specified batch size for requisitions`() = runBlocking {
    val requisition = REQUISITION

    val updateTimeNanosecondsFieldValue: FieldValue =
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Timestamps.toNanos(requisition.updateTime)}")

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll())
        .thenReturn(emptyList())
        .thenReturn(
          listOf(
            FieldValueList.of(mutableListOf(updateTimeNanosecondsFieldValue), REQUISITION_FIELD_LIST)
          )
        )
        .thenReturn(emptyList())
    }

    whenever(requisitionsMock.streamRequisitions(any())).thenReturn(flowOf(REQUISITION_2))

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
        batchSize = 100,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<StreamRequisitionsRequest>()) {
      verify(requisitionsMock).streamRequisitions(capture())
      val streamRequisitionsRequest = allValues.first()

      assertThat(streamRequisitionsRequest)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          streamRequisitionsRequest {
            filter =
              StreamRequisitionsRequestKt.filter {
                states += Requisition.State.FULFILLED
                states += Requisition.State.REFUSED
                updatedAfter = requisition.updateTime
              }
            limit = 100
          }
        )
    }
  }

  @Test
  fun `job can process the next batch of computations without starting at the beginning`() =
    runBlocking {
      val computationMeasurement = COMPUTATION_MEASUREMENT

      val computationUpdateTimeNanosecondsFieldValue: FieldValue =
        FieldValue.of(
          FieldValue.Attribute.PRIMITIVE,
          "${Timestamps.toNanos(computationMeasurement.updateTime)}",
        )

      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll())
          .thenReturn(emptyList())
          .thenReturn(emptyList())
          .thenReturn(
            listOf(
              FieldValueList.of(mutableListOf(computationUpdateTimeNanosecondsFieldValue), COMPUTATION_PARTICIPANT_STAGE_FIELD_LIST)
            )
          )
          .thenReturn(emptyList())
      }

      whenever(measurementsMock.streamMeasurements(any()))
        .thenReturn(flowOf(COMPUTATION_MEASUREMENT))

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      operationalMetricsExport.execute()

      with(argumentCaptor<StreamMeasurementsRequest>()) {
        verify(measurementsMock, times(2)).streamMeasurements(capture())
        val streamMeasurementsRequest = lastValue

        assertThat(streamMeasurementsRequest)
          .ignoringRepeatedFieldOrder()
          .isEqualTo(
            streamMeasurementsRequest {
              measurementView = Measurement.View.COMPUTATION_STATS
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += Measurement.State.SUCCEEDED
                  states += Measurement.State.FAILED
                  updatedAfter = computationMeasurement.updateTime
                }
              limit = 1000
            }
          )
      }
    }

  @Test
  fun `job uses specified batch size for computations`() = runBlocking {
    val computationMeasurement = COMPUTATION_MEASUREMENT

    val computationUpdateTimeNanosecondsFieldValue: FieldValue =
      FieldValue.of(
        FieldValue.Attribute.PRIMITIVE,
        "${Timestamps.toNanos(computationMeasurement.updateTime)}",
      )

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll())
        .thenReturn(emptyList())
        .thenReturn(emptyList())
        .thenReturn(
          listOf(
            FieldValueList.of(mutableListOf(computationUpdateTimeNanosecondsFieldValue), COMPUTATION_PARTICIPANT_STAGE_FIELD_LIST)
          )
        )
        .thenReturn(emptyList())
    }

    whenever(measurementsMock.streamMeasurements(any())).thenReturn(flowOf(COMPUTATION_MEASUREMENT))

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
        batchSize = 100,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<StreamMeasurementsRequest>()) {
      verify(measurementsMock, times(2)).streamMeasurements(capture())
      val streamMeasurementsRequest = allValues.last()

      assertThat(streamMeasurementsRequest)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          streamMeasurementsRequest {
            measurementView = Measurement.View.COMPUTATION_STATS
            filter =
              StreamMeasurementsRequestKt.filter {
                states += Measurement.State.SUCCEEDED
                states += Measurement.State.FAILED
                updatedAfter = computationMeasurement.updateTime
              }
            limit = 100
          }
        )
    }
  }

  @Test
  fun `job skips direct measurements when attempting to export stages`() = runBlocking {
    val batchSize = 10
    whenever(measurementsMock.streamMeasurements(any()))
      .thenReturn(flowOf(DIRECT_MEASUREMENT, COMPUTATION_MEASUREMENT))
      .thenReturn(
        buildList {
            for (i in 1..batchSize) {
              add(DIRECT_MEASUREMENT)
            }
          }
          .asFlow()
      )
      .thenReturn(flowOf(DIRECT_MEASUREMENT, COMPUTATION_MEASUREMENT))

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
        batchSize = batchSize,
      )

    operationalMetricsExport.execute()

    with(argumentCaptor<StreamMeasurementsRequest>()) {
      verify(measurementsMock, times(3)).streamMeasurements(capture())
    }

    with(argumentCaptor<ProtoRows>()) {
      verify(computationParticipantStagesStreamWriterMock).append(capture())

      val protoRows: ProtoRows = firstValue
      assertThat(protoRows.serializedRowsList).hasSize(4)
    }
  }

  @Test
  fun `job succeeds when column in latest read query is null`() = runBlocking {
    val updateTimeNanosecondsFieldValue: FieldValue =
      FieldValue.of(
        FieldValue.Attribute.PRIMITIVE,
        null,
      )

    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll())
        .thenReturn(
          listOf(
            FieldValueList.of(
              mutableListOf(
                updateTimeNanosecondsFieldValue,
              ),
              MEASUREMENT_FIELD_LIST,
            )
          )
        )
        .thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsStreamWriterMock.append(any(), any()))
      .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job recreates streamwriter if it is closed`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsStreamWriterMock.isClosed).thenReturn(true)
    whenever(measurementsStreamWriterMock.isUserClosed).thenReturn(false)

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job succeeds when bigquery append fails with internal only once`() = runBlocking {
    val tableResultMock: TableResult = mock { tableResult ->
      whenever(tableResult.iterateAll()).thenReturn(emptyList())
    }

    val bigQueryMock: BigQuery = mock { bigQuery ->
      whenever(bigQuery.query(any())).thenReturn(tableResultMock)
    }

    whenever(measurementsStreamWriterMock.append(any()))
      .thenReturn(
        ApiFutures.immediateFuture(
          AppendRowsResponse.newBuilder()
            .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
            .build()
        )
      )
      .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()))

    val operationalMetricsExport =
      OperationalMetricsExport(
        measurementsClient = measurementsClient,
        requisitionsClient = requisitionsClient,
        bigQuery = bigQueryMock,
        bigQueryWriteClient = bigQueryWriteClientMock,
        projectId = PROJECT_ID,
        datasetId = DATASET_ID,
        measurementsTableId = MEASUREMENTS_TABLE_ID,
        requisitionsTableId = REQUISITIONS_TABLE_ID,
        computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
        streamWriterFactory = streamWriterFactoryTestImpl,
      )

    operationalMetricsExport.execute()
  }

  @Test
  fun `job fails when streamMeasurements fails`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsMock.streamMeasurements(any()))
        .thenThrow(StatusRuntimeException(io.grpc.Status.DEADLINE_EXCEEDED))

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<StatusException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when streamRequisitions fails`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(requisitionsMock.streamRequisitions(any()))
        .thenThrow(StatusRuntimeException(io.grpc.Status.DEADLINE_EXCEEDED))

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<StatusException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when bigquery append fails with internal too many times`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
              .build()
          )
        )

      whenever(requisitionsStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
              .build()
          )
        )

      whenever(computationParticipantStagesStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INTERNAL_VALUE).build())
              .build()
          )
        )

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<IllegalStateException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when bigquery append fails with invalid argument`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenReturn(
          ApiFutures.immediateFuture(
            AppendRowsResponse.newBuilder()
              .setError(Status.newBuilder().setCode(Code.INVALID_ARGUMENT_VALUE).build())
              .build()
          )
        )

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<IllegalStateException> { operationalMetricsExport.execute() }
    }
  }

  @Test
  fun `job fails when bigquery append fails with append serialization error`() {
    runBlocking {
      val tableResultMock: TableResult = mock { tableResult ->
        whenever(tableResult.iterateAll()).thenReturn(emptyList())
      }

      val bigQueryMock: BigQuery = mock { bigQuery ->
        whenever(bigQuery.query(any())).thenReturn(tableResultMock)
      }

      whenever(measurementsStreamWriterMock.append(any()))
        .thenThrow(AppendSerializationError(0, "", "", mapOf()))

      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQueryMock,
          bigQueryWriteClient = bigQueryWriteClientMock,
          projectId = PROJECT_ID,
          datasetId = DATASET_ID,
          measurementsTableId = MEASUREMENTS_TABLE_ID,
          requisitionsTableId = REQUISITIONS_TABLE_ID,
          computationParticipantStagesTableId = COMPUTATION_PARTICIPANT_STAGES_TABLE_ID,
          streamWriterFactory = streamWriterFactoryTestImpl,
        )

      assertFailsWith<AppendSerializationError> { operationalMetricsExport.execute() }
    }
  }

  companion object {
    private const val PROJECT_ID = "project"
    private const val DATASET_ID = "dataset"
    private const val MEASUREMENTS_TABLE_ID = "measurements"
    private const val REQUISITIONS_TABLE_ID = "requisitions"
    private const val COMPUTATION_PARTICIPANT_STAGES_TABLE_ID = "computation_participant_stages"
    private const val LATEST_MEASUREMENT_READ_TABLE_ID = "latest_measurement_read"
    private const val LATEST_REQUISITION_READ_TABLE_ID = "latest_requisition_read"
    private const val LATEST_COMPUTATION_READ_TABLE_ID = "latest_computation_read"

    private val API_VERSION = Version.V2_ALPHA.toString()

    private val PUBLIC_API_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = ByteString.copyFromUtf8("key")
    }

    private val PUBLIC_API_MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.pack()
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 1.1
            delta = 1.2
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 2.1
            delta = 2.2
          }
          maximumFrequency = 10
        }
    }

    private val MEASUREMENT = measurement {
      externalMeasurementConsumerId = 1234
      externalMeasurementConsumerCertificateId = 1234
      details = measurementDetails {
        apiVersion = API_VERSION
        measurementSpec = PUBLIC_API_MEASUREMENT_SPEC.toByteString()
        measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
        measurementSpecSignatureAlgorithmOid = "2.9999"
      }
    }

    private val STAGE_ONE = "stage_one"
    private val STAGE_TWO = "stage_two"

    private val COMPUTATION_MEASUREMENT =
      MEASUREMENT.copy {
        externalMeasurementId = 123
        externalComputationId = 124
        providedMeasurementId = "computation-participant"
        state = Measurement.State.SUCCEEDED
        createTime = timestamp { seconds = 200 }
        updateTime = timestamp { seconds = 600 }
        details =
          details.copy {
            protocolConfig = protocolConfig {
              liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
            }
          }

        requisitions += requisition {
          externalDataProviderId = 432
          externalRequisitionId = 433
          state = Requisition.State.FULFILLED
          updateTime = timestamp {
            seconds = 500
            nanos = 100
          }
        }

        computationParticipants += computationParticipant {
          externalDuchyId = "0"
          state = ComputationParticipant.State.READY
          updateTime = timestamp { seconds = 300 }
          failureLogEntry = duchyMeasurementLogEntry {
            logEntry = measurementLogEntry {
              createTime = timestamp { seconds = 350 }
              details = measurementLogEntryDetails { error = measurementLogEntryError {} }
            }
            details = duchyMeasurementLogEntryDetails {
              stageAttempt = duchyMeasurementLogEntryStageAttempt { stageName = STAGE_TWO }
            }
          }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "1"
          state = ComputationParticipant.State.READY
          updateTime = timestamp { seconds = 400 }
        }
        computationParticipants += computationParticipant {
          externalDuchyId = "2"
          state = ComputationParticipant.State.READY
          updateTime = timestamp { seconds = 500 }
        }
        logEntries += duchyMeasurementLogEntry {
          externalDuchyId = "0"
          logEntry = measurementLogEntry { createTime = timestamp { seconds = 300 } }
          details = duchyMeasurementLogEntryDetails {
            stageAttempt = duchyMeasurementLogEntryStageAttempt {
              stage = 1
              stageStartTime = timestamp { seconds = 100 }
              stageName = STAGE_ONE
            }
          }
        }
        logEntries += duchyMeasurementLogEntry {
          externalDuchyId = "0"
          logEntry = measurementLogEntry { createTime = timestamp { seconds = 300 } }
          details = duchyMeasurementLogEntryDetails {
            stageAttempt = duchyMeasurementLogEntryStageAttempt {
              stage = 2
              stageStartTime = timestamp { seconds = 300 }
              stageName = STAGE_TWO
            }
          }
        }
        logEntries += duchyMeasurementLogEntry {
          externalDuchyId = "1"
          logEntry = measurementLogEntry { createTime = timestamp { seconds = 300 } }
          details = duchyMeasurementLogEntryDetails {
            stageAttempt = duchyMeasurementLogEntryStageAttempt {
              stage = 1
              stageStartTime = timestamp { seconds = 100 }
              stageName = STAGE_ONE
            }
          }
        }
        logEntries += duchyMeasurementLogEntry {
          externalDuchyId = "1"
          logEntry = measurementLogEntry { createTime = timestamp { seconds = 300 } }
          details = duchyMeasurementLogEntryDetails {
            stageAttempt = duchyMeasurementLogEntryStageAttempt {
              stage = 2
              stageStartTime = timestamp { seconds = 300 }
              stageName = STAGE_TWO
            }
          }
        }
      }

    private val DIRECT_MEASUREMENT =
      MEASUREMENT.copy {
        externalMeasurementId = 321
        externalComputationId = 0
        providedMeasurementId = "direct"
        state = Measurement.State.SUCCEEDED
        createTime = timestamp { seconds = 200 }
        updateTime = timestamp {
          seconds = 220
          nanos = 200
        }
        details =
          details.copy {
            protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
          }
      }

    private val REQUISITION = requisition {
      externalMeasurementConsumerId = 1234
      externalMeasurementId = 123
      externalDataProviderId = 432
      externalRequisitionId = 433
      state = Requisition.State.FULFILLED
      updateTime = timestamp {
        seconds = 500
        nanos = 100
      }
      parentMeasurement =
        RequisitionKt.parentMeasurement {
          apiVersion = API_VERSION
          measurementSpec = PUBLIC_API_MEASUREMENT_SPEC.toByteString()
          measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
          measurementSpecSignatureAlgorithmOid = "2.9999"
          protocolConfig = protocolConfig {
            liquidLegionsV2 = ProtocolConfig.LiquidLegionsV2.getDefaultInstance()
          }
          createTime = timestamp { seconds = 200 }
        }
    }

    private val REQUISITION_2 = requisition {
      externalMeasurementConsumerId = 1234
      externalMeasurementId = 123
      externalDataProviderId = 432
      externalRequisitionId = 437
      state = Requisition.State.FULFILLED
      updateTime = timestamp {
        seconds = 600
        nanos = 100
      }
      parentMeasurement =
        RequisitionKt.parentMeasurement {
          apiVersion = API_VERSION
          measurementSpec = PUBLIC_API_MEASUREMENT_SPEC.toByteString()
          measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
          measurementSpecSignatureAlgorithmOid = "2.9999"
          protocolConfig = protocolConfig { direct = ProtocolConfig.Direct.getDefaultInstance() }
          createTime = timestamp { seconds = 200 }
        }
    }

    private val MEASUREMENT_FIELD_LIST: FieldList =
      FieldList.of(listOf(Field.of("update_time_nanoseconds", LegacySQLTypeName.INTEGER)))

    private val REQUISITION_FIELD_LIST: FieldList =
      FieldList.of(listOf(Field.of("update_time_nanoseconds", LegacySQLTypeName.INTEGER)))

    private val COMPUTATION_PARTICIPANT_STAGE_FIELD_LIST: FieldList =
      FieldList.of(listOf(Field.of("computation_update_time_nanoseconds", LegacySQLTypeName.INTEGER)))
  }
}
