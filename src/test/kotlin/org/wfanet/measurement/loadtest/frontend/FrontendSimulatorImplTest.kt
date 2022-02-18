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

package org.wfanet.measurement.loadtest.frontend

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchConfig.ValueSpec.Aggregator
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PRIVATE_KEYSET
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_CERT_DER_FILE
import org.wfanet.measurement.common.crypto.testing.FIXED_SERVER_KEY_DER_FILE
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private const val API_AUTHENTICATION_KEY = "authentication key"
private const val RUN_ID = "run id"
private const val BUFFER_SIZE_BYTES = 1024 * 32 // 32 KiB
private const val REQUISITION_ONE = "requisition_one"
private const val REQUISITION_TWO = "requisition_two"

private const val LLV2_DECAY_RATE = 12.0
private const val LLV2_MAX_SIZE = 100_000L
private const val MAX_FREQUENCY = 10

private val SKETCH_CONFIG =
  SketchConfig.newBuilder()
    .apply {
      addIndexesBuilder().apply {
        name = "Index"
        distributionBuilder.exponentialBuilder.apply {
          rate = LLV2_DECAY_RATE
          numValues = LLV2_MAX_SIZE
        }
      }
      addValuesBuilder().apply {
        name = "SamplingIndicator"
        aggregator = Aggregator.UNIQUE
        distributionBuilder.uniformBuilder.apply {
          numValues = 10_000_000 // 10M
        }
      }
      addValuesBuilder().apply {
        name = "Frequency"
        aggregator = Aggregator.SUM
        distributionBuilder.oracleBuilder.apply { key = "frequency" }
      }
    }
    .build()
private val OUTPUT_DP_PARAMS = DifferentialPrivacyParams.getDefaultInstance()
private val LIQUID_LEGIONS_V2_PROTOCOL_CONFIG = liquidLegionsV2 {
  sketchParams =
    liquidLegionsSketchParams {
      decayRate = LLV2_DECAY_RATE
      maxSize = LLV2_MAX_SIZE
    }
  maximumFrequency = MAX_FREQUENCY
}

@RunWith(JUnit4::class)
class FrontendSimulatorImplTest {
  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService()
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()
  private val measurementsServiceMock: MeasurementsCoroutineImplBase = mockService()
  private val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase = mockService()
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase =
    mockService() {
      onBlocking { listRequisitions(any()) }
        .thenReturn(
          ListRequisitionsResponse.newBuilder()
            .apply {
              addRequisitionsBuilder().name = REQUISITION_ONE
              addRequisitionsBuilder().name = REQUISITION_TWO
            }
            .build()
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(dataProvidersServiceMock)
    addService(eventGroupsServiceMock)
    addService(measurementsServiceMock)
    addService(measurementConsumersServiceMock)
    addService(requisitionsServiceMock)
  }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }
  private val measurementsStub: MeasurementsCoroutineStub by lazy {
    MeasurementsCoroutineStub(grpcTestServerRule.channel)
  }
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }
  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var frontendSimulator: FrontendSimulator

  @Test
  fun `get expected result from sketches successfully`() = runBlocking {
    frontendSimulator =
      FrontendSimulator(
        MEASUREMENT_CONSUMER_DATA,
        OUTPUT_DP_PARAMS,
        dataProvidersStub,
        eventGroupsStub,
        measurementsStub,
        requisitionsStub,
        measurementConsumersStub,
        sketchStore,
        RUN_ID
      )

    assertThat(frontendSimulator.getExpectedResult("foo", LIQUID_LEGIONS_V2_PROTOCOL_CONFIG))
      .isEqualTo(
        Measurement.Result.newBuilder()
          .apply {
            reachBuilder.value = 9
            frequencyBuilder.apply {
              putRelativeFrequencyDistribution(1, 2.0 / 3) // 1,2,6,7
              putRelativeFrequencyDistribution(2, 1.0 / 3) // 4,5
            }
          }
          .build()
      )
  }

  companion object {
    private val MEASUREMENT_CONSUMER_DATA =
      MeasurementConsumerData(
        "name",
        loadSigningKey(FIXED_SERVER_CERT_DER_FILE, FIXED_SERVER_KEY_DER_FILE),
        loadPrivateKey(FIXED_ENCRYPTION_PRIVATE_KEYSET),
        API_AUTHENTICATION_KEY
      )

    @JvmField @ClassRule val temporaryFolder: TemporaryFolder = TemporaryFolder()

    lateinit var sketchStore: SketchStore
      private set

    @JvmStatic
    @BeforeClass
    fun writeSketchesToStore() =
      runBlocking<Unit> {
        val sketch1 =
          Sketch.newBuilder()
            .apply {
              config = SKETCH_CONFIG
              addRegisters(newRegister(index = 1, key = 1, count = 1))
              addRegisters(newRegister(index = 2, key = 2, count = 1))
              addRegisters(newRegister(index = 3, key = 3, count = 1))
              addRegisters(newRegister(index = 4, key = 4, count = 1))
              addRegisters(newRegister(index = 5, key = 5, count = 1))
              addRegisters(newRegister(index = 10, key = 0, count = 1)) // destroyed
            }
            .build()
        val sketch2 =
          Sketch.newBuilder()
            .apply {
              config = SKETCH_CONFIG
              addRegisters(newRegister(index = 3, key = 13, count = 1))
              addRegisters(newRegister(index = 4, key = 4, count = 1))
              addRegisters(newRegister(index = 5, key = 5, count = 1))
              addRegisters(newRegister(index = 6, key = 6, count = 1))
              addRegisters(newRegister(index = 7, key = 7, count = 1))
            }
            .build()

        sketchStore = SketchStore(FileSystemStorageClient(temporaryFolder.root))
        sketchStore.write(REQUISITION_ONE, sketch1.toByteString().asBufferedFlow(BUFFER_SIZE_BYTES))
        sketchStore.write(REQUISITION_TWO, sketch2.toByteString().asBufferedFlow(BUFFER_SIZE_BYTES))
      }

    private fun newRegister(index: Long, key: Long, count: Long): Sketch.Register {
      return Sketch.Register.newBuilder()
        .also {
          it.index = index
          it.addValues(key)
          it.addValues(count)
        }
        .build()
    }
  }
}
