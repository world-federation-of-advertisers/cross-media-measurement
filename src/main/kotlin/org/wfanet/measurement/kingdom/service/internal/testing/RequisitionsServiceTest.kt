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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as ComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase as DataProvidersCoroutineService
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumerKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase as MeasurementConsumersCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementKt
import org.wfanet.measurement.internal.kingdom.MeasurementKt.dataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as MeasurementsCoroutineService
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt.parentMeasurement
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as RequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.getRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.getRequisitionRequest
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementConsumer
import org.wfanet.measurement.internal.kingdom.requisition
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter

private const val RANDOM_SEED = 1L

@RunWith(JUnit4::class)
abstract class RequisitionsServiceTest<T : RequisitionsCoroutineService> {
  data class TestDataServices(
    val measurementConsumersService: MeasurementConsumersCoroutineService,
    val dataProvidersService: DataProvidersCoroutineService,
    val measurementsService: MeasurementsCoroutineService,
    val computationParticipantsService: ComputationParticipantsCoroutineService
  )

  protected val clock: Clock = Clock.systemUTC()
  protected val idGenerator = RandomIdGenerator(clock, Random(RANDOM_SEED))
  @get:Rule val duchyIdSetter = DuchyIdSetter("alpha", "bravo", "charlie")

  protected lateinit var dataServices: TestDataServices
    private set

  /** Subject under test (SUT). */
  protected lateinit var service: T
    private set

  /** Constructs services used to populate test data. */
  protected abstract fun newTestDataServices(
    clock: Clock,
    idGenerator: IdGenerator
  ): TestDataServices

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initDataServices() {
    dataServices = newTestDataServices(clock, idGenerator)
  }

  @Before
  fun initService() {
    service = newService()
  }

  protected suspend fun createMeasurementConsumer(): MeasurementConsumer {
    return dataServices.measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        certificate =
          buildRequestCertificate("MC cert", "MC SKID " + idGenerator.generateExternalId().value)
        details =
          MeasurementConsumerKt.details {
            apiVersion = API_VERSION.string
            publicKey = ByteString.copyFromUtf8("MC public key")
            publicKeySignature = ByteString.copyFromUtf8("MC public key signature")
          }
      }
    )
  }

  protected suspend fun createDataProvider(): DataProvider {
    return dataServices.dataProvidersService.createDataProvider(
      dataProvider {
        certificate =
          buildRequestCertificate("EDP cert", "EDP SKID " + idGenerator.generateExternalId().value)
        details =
          DataProviderKt.details {
            apiVersion = API_VERSION.string
            publicKey = ByteString.copyFromUtf8("EDP public key")
            publicKeySignature = ByteString.copyFromUtf8("EDP public key signature")
          }
      }
    )
  }

  protected suspend fun createMeasurement(
    measurementConsumer: MeasurementConsumer,
    providedMeasurementId: String,
    vararg dataProviders: DataProvider
  ): Measurement {
    return dataServices.measurementsService.createMeasurement(
      measurement {
        externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
        this.providedMeasurementId = providedMeasurementId
        externalMeasurementConsumerCertificateId =
          measurementConsumer.certificate.externalCertificateId
        externalProtocolConfigId = "llv2"
        details =
          MeasurementKt.details {
            apiVersion = API_VERSION.string
            measurementSpec = ByteString.copyFromUtf8("MeasurementSpec")
            measurementSpecSignature = ByteString.copyFromUtf8("MeasurementSpec signature")
            dataProviderList = ByteString.copyFromUtf8("EDP list")
            dataProviderListSalt = ByteString.copyFromUtf8("EDP list salt")
            duchyProtocolConfig =
              duchyProtocolConfig {
                liquidLegionsV2 = DuchyProtocolConfig.LiquidLegionsV2.getDefaultInstance()
              }
          }
        for (dataProvider in dataProviders) {
          this.dataProviders[dataProvider.externalDataProviderId] =
            dataProviderValue {
              externalDataProviderCertificateId = dataProvider.certificate.externalCertificateId
              dataProviderPublicKey = dataProvider.details.publicKey
              dataProviderPublicKeySignature = dataProvider.details.publicKeySignature
              encryptedRequisitionSpec = ByteString.copyFromUtf8("Encrypted RequisitionSpec")
            }
        }
      }
    )
  }

  private fun buildRequestCertificate(derUtf8: String, skidUtf8: String): Certificate {
    val now = clock.instant()
    return certificate {
      notValidBefore = now.toProtoTime()
      notValidAfter = now.plus(365L, ChronoUnit.DAYS).toProtoTime()
      subjectKeyIdentifier = ByteString.copyFromUtf8(skidUtf8)
      details = CertificateKt.details { x509Der = ByteString.copyFromUtf8(derUtf8) }
    }
  }

  @Test
  fun `streamRequisitions returns all requisitions for EDP in order`(): Unit = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider = createDataProvider()
    val measurement1 = createMeasurement(measurementConsumer, "measurement 1", dataProvider)
    val measurement2 = createMeasurement(measurementConsumer, "measurement 2", dataProvider)
    createMeasurement(measurementConsumer, "measurement 3", createDataProvider())

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement1.externalMeasurementId
        },
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )
      .inOrder()
  }

  @Test
  fun `streamRequisitions returns all requisitions for MC`(): Unit = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider1 = createDataProvider()
    val dataProvider2 = createDataProvider()
    val measurement1 =
      createMeasurement(measurementConsumer, "measurement 1", dataProvider1, dataProvider2)
    val measurement2 = createMeasurement(measurementConsumer, "measurement 2", dataProvider1)
    createMeasurement(createMeasurementConsumer(), "other MC measurement", dataProvider1)

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement1.externalMeasurementId
          externalDataProviderId = dataProvider2.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurementConsumer.externalMeasurementConsumerId
          externalMeasurementId = measurement2.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        }
      )
  }

  @Test
  fun `streamRequisitions returns all requisitions for measurement`(): Unit = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider1 = createDataProvider()
    val dataProvider2 = createDataProvider()
    val measurement =
      createMeasurement(measurementConsumer, "measurement 1", dataProvider1, dataProvider2)
    createMeasurement(measurementConsumer, "measurement 2", dataProvider1)

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          externalDataProviderId = dataProvider1.externalDataProviderId
        },
        requisition {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          externalDataProviderId = dataProvider2.externalDataProviderId
        }
      )
  }

  @Test
  fun `streamRequisitions respects updated_after`(): Unit = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider = createDataProvider()
    val measurement1 = createMeasurement(measurementConsumer, "measurement 1", dataProvider)
    val measurement2 = createMeasurement(measurementConsumer, "measurement 2", dataProvider)
    createMeasurement(measurementConsumer, "measurement 3", createDataProvider())

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalDataProviderId = dataProvider.externalDataProviderId
                updatedAfter = measurement1.updateTime
              }
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement2.externalMeasurementId
        }
      )
  }

  @Test
  fun `streamRequisitions respects limit`(): Unit = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider = createDataProvider()
    val measurement1 = createMeasurement(measurementConsumer, "measurement 1", dataProvider)
    createMeasurement(measurementConsumer, "measurement 2", dataProvider)

    val requisitions: List<Requisition> =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter = filter { externalDataProviderId = dataProvider.externalDataProviderId }
            limit = 1
          }
        )
        .toList()

    assertThat(requisitions)
      .comparingExpectedFieldsOnly()
      .containsExactly(
        requisition {
          externalDataProviderId = dataProvider.externalDataProviderId
          externalMeasurementId = measurement1.externalMeasurementId
        }
      )
  }

  @Test
  fun `getRequisition returns expected requisition`() = runBlocking {
    val measurementConsumer = createMeasurementConsumer()
    val dataProvider = createDataProvider()
    val providedMeasurementId = "measurement"
    val measurement = createMeasurement(measurementConsumer, providedMeasurementId, dataProvider)
    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .first()
    val externalRequisitionId = listedRequisition.externalRequisitionId

    val requisition =
      service.getRequisition(
        getRequisitionRequest {
          externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
          externalMeasurementId = measurement.externalMeasurementId
          this.externalRequisitionId = externalRequisitionId
        }
      )

    val expectedRequisition = requisition {
      externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
      externalMeasurementId = measurement.externalMeasurementId
      externalDataProviderId = dataProvider.externalDataProviderId
      this.externalRequisitionId = externalRequisitionId
      state = Requisition.State.UNFULFILLED
      parentMeasurement =
        parentMeasurement {
          apiVersion = measurement.details.apiVersion
          externalMeasurementConsumerCertificateId =
            measurement.externalMeasurementConsumerCertificateId
          measurementSpec = measurement.details.measurementSpec
          measurementSpecSignature = measurement.details.measurementSpecSignature
        }
    }
    assertThat(requisition).comparingExpectedFieldsOnly().isEqualTo(expectedRequisition)
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  @Test
  fun `getRequisitionByDataProviderId returns requisition`() = runBlocking {
    val dataProvider = createDataProvider()
    val measurement = createMeasurement(createMeasurementConsumer(), "measurement", dataProvider)
    val listedRequisition =
      service
        .streamRequisitions(
          streamRequisitionsRequest {
            filter =
              filter {
                externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                externalMeasurementId = measurement.externalMeasurementId
              }
          }
        )
        .first()
    val externalRequisitionId = listedRequisition.externalRequisitionId

    val requisition =
      service.getRequisitionByDataProviderId(
        getRequisitionByDataProviderIdRequest {
          externalDataProviderId = dataProvider.externalDataProviderId
          this.externalRequisitionId = externalRequisitionId
        }
      )
    assertThat(requisition).isEqualTo(listedRequisition)
  }

  companion object {
    protected val API_VERSION = Version.V2_ALPHA
  }
}
