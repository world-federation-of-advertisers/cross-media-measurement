// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.CreateDuchyMeasurementLogEntryRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryError
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationLogEntryKey
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest

private const val DUCHY_ID: String = "some-duchy-id"
private const val MILL_ID: String = "some-mill-id"
private const val STAGE_ATTEMPT_STAGE = 9
private const val STAGE_ATTEMPT_STAGE_NAME = "a stage"
private const val STAGE_ATTEMPT_ATTEMPT_NUMBER = 1L
private const val DUCHY_ERROR_MESSAGE = "something is wrong."

private const val EXTERNAL_COMPUTATION_ID = 1L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 5L
private const val EXTERNAL_MEASUREMENT_ID = 6L
private const val EXTERNAL_COMPUTATION_LOG_ENTRY_ID = 7L

private val EXTERNAL_COMPUTATION_ID_STRING = externalIdToApiId(EXTERNAL_COMPUTATION_ID)
private val EXTERNAL_COMPUTATION_LOG_ENTRY_ID_STRING =
  externalIdToApiId(EXTERNAL_COMPUTATION_LOG_ENTRY_ID)
private val PUBLIC_MEASUREMENT_NAME =
  "measurementConsumers/${externalIdToApiId(EXTERNAL_MEASUREMENT_CONSUMER_ID)}/" +
    "measurements/${externalIdToApiId(EXTERNAL_MEASUREMENT_ID)}"
private val SYSTEM_COMPUTATION_PARTICIPATE_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/participants/$DUCHY_ID"
private val COMPUTATION_LOG_ENTRY_NAME =
  ComputationLogEntryKey(
      EXTERNAL_COMPUTATION_ID_STRING,
      DUCHY_ID,
      EXTERNAL_COMPUTATION_LOG_ENTRY_ID_STRING,
    )
    .toName()

private val MEASUREMENT_LOG_ENTRY =
  MeasurementLogEntry.newBuilder()
    .apply {
      externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID
      externalMeasurementId = EXTERNAL_MEASUREMENT_ID
      createTimeBuilder.apply {
        seconds = 1
        nanos = 2
      }
      detailsBuilder.apply {
        logMessage = DUCHY_ERROR_MESSAGE
        errorBuilder.apply {
          type = MeasurementLogEntryError.Type.TRANSIENT
          errorTimeBuilder.apply {
            seconds = 3
            nanos = 4
          }
        }
      }
    }
    .build()

private val DUCHY_MEASUREMENT_LOG_ENTRY =
  DuchyMeasurementLogEntry.newBuilder()
    .apply {
      logEntry = MEASUREMENT_LOG_ENTRY
      externalDuchyId = DUCHY_ID
      externalComputationLogEntryId = EXTERNAL_COMPUTATION_LOG_ENTRY_ID
      detailsBuilder.apply {
        duchyChildReferenceId = MILL_ID
        stageAttemptBuilder.apply {
          stage = STAGE_ATTEMPT_STAGE
          stageName = STAGE_ATTEMPT_STAGE_NAME
          attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
          stageStartTimeBuilder.apply {
            seconds = 100
            nanos = 200
          }
        }
      }
    }
    .build()

private val COMPUTATION_LOG_ENTRY =
  ComputationLogEntry.newBuilder()
    .apply {
      name = COMPUTATION_LOG_ENTRY_NAME
      participantChildReferenceId = MILL_ID
      logMessage = DUCHY_ERROR_MESSAGE
      stageAttemptBuilder.apply {
        stage = STAGE_ATTEMPT_STAGE
        stageName = STAGE_ATTEMPT_STAGE_NAME
        attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
        stageStartTimeBuilder.apply {
          seconds = 100
          nanos = 200
        }
      }
      errorDetailsBuilder.apply {
        type = ComputationLogEntry.ErrorDetails.Type.TRANSIENT
        errorTimeBuilder.apply {
          seconds = 3
          nanos = 4
        }
      }
    }
    .build()

@RunWith(JUnit4::class)
class ComputationLogEntriesServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val measurementLogEntriesServiceMock: MeasurementLogEntriesCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(measurementLogEntriesServiceMock) }

  private val service =
    ComputationLogEntriesService(
      MeasurementLogEntriesCoroutineStub(grpcTestServerRule.channel),
      duchyIdentityProvider = duchyIdProvider,
    )
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
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

  @Test
  fun `CreateComputationLogEntry successfully`() = runBlocking {
    whenever(measurementLogEntriesServiceMock.createDuchyMeasurementLogEntry(any()))
      .thenReturn(DUCHY_MEASUREMENT_LOG_ENTRY)

    val request =
      CreateComputationLogEntryRequest.newBuilder()
        .apply {
          parent = SYSTEM_COMPUTATION_PARTICIPATE_NAME
          computationLogEntry = COMPUTATION_LOG_ENTRY.toBuilder().clearName().build()
        }
        .build()

    val response = service.createComputationLogEntry(request)
    assertThat(response).isEqualTo(COMPUTATION_LOG_ENTRY)
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.name).isEqualTo("kingdom.computation_log_entry.create")
    assertThat(span.attributes.get(ReportTraceAttributes.COMPUTATION_NAME))
      .isEqualTo("computations/$EXTERNAL_COMPUTATION_ID_STRING")
    assertThat(span.attributes.get(ReportTraceAttributes.DUCHY_ID)).isEqualTo(DUCHY_ID)
    assertThat(span.attributes.get(ReportTraceAttributes.MEASUREMENT_NAME))
      .isEqualTo(PUBLIC_MEASUREMENT_NAME)
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("kingdom_computation_log_entry_acceptance")
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("accepted")

    verifyProtoArgument(
        measurementLogEntriesServiceMock,
        MeasurementLogEntriesCoroutineImplBase::createDuchyMeasurementLogEntry,
      )
      .isEqualTo(
        CreateDuchyMeasurementLogEntryRequest.newBuilder()
          .apply {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalDuchyId = DUCHY_ID
            measurementLogEntryDetailsBuilder.apply {
              logMessage = DUCHY_ERROR_MESSAGE
              errorBuilder.apply {
                type = MeasurementLogEntryError.Type.TRANSIENT
                errorTimeBuilder.apply {
                  seconds = 3
                  nanos = 4
                }
              }
            }
            detailsBuilder.apply {
              duchyChildReferenceId = MILL_ID
              stageAttemptBuilder.apply {
                stage = STAGE_ATTEMPT_STAGE
                stageName = STAGE_ATTEMPT_STAGE_NAME
                attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
                stageStartTimeBuilder.apply {
                  seconds = 100
                  nanos = 200
                }
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `createComputationLogEntry emits failed lifecycle span when internal RPC fails`() {
    measurementLogEntriesServiceMock.stub {
      onBlocking { createDuchyMeasurementLogEntry(any()) }
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())
    }
    val request =
      CreateComputationLogEntryRequest.newBuilder()
        .apply {
          parent = SYSTEM_COMPUTATION_PARTICIPATE_NAME
          computationLogEntry = COMPUTATION_LOG_ENTRY.toBuilder().clearName().build()
        }
        .build()

    val exception =
      assertFailsWith<StatusException> {
        runBlocking { service.createComputationLogEntry(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.COMPUTATION_NAME))
      .isEqualTo("computations/$EXTERNAL_COMPUTATION_ID_STRING")
    assertThat(span.attributes.get(ReportTraceAttributes.DUCHY_ID)).isEqualTo(DUCHY_ID)
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("kingdom_computation_log_entry_acceptance")
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE)).isEqualTo("grpc.UNAVAILABLE")
  }

  @Test
  fun `missing resource name should throw`() {
    val e =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.createComputationLogEntry(CreateComputationLogEntryRequest.getDefaultInstance())
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.localizedMessage).contains("Resource name unspecified or invalid.")
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("kingdom_computation_log_entry_acceptance")
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE))
      .isEqualTo("grpc.INVALID_ARGUMENT")
  }

  @Test
  fun `Permanent error should throw`() {
    val request =
      CreateComputationLogEntryRequest.newBuilder()
        .apply {
          parent = SYSTEM_COMPUTATION_PARTICIPATE_NAME
          computationLogEntry =
            COMPUTATION_LOG_ENTRY.toBuilder()
              .apply {
                clearName()
                errorDetailsBuilder.apply { type = ComputationLogEntry.ErrorDetails.Type.PERMANENT }
              }
              .build()
        }
        .build()
    val e =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createComputationLogEntry(request) }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.localizedMessage)
      .contains("Only transient error is support in the computationLogEntriesService.")
  }
}
