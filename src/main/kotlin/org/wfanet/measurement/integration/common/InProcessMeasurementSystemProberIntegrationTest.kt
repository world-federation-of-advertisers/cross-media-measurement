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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import io.grpc.StatusException
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.kingdom.batch.MeasurementSystemProber
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessMeasurementSystemProberIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule, useEdpSimulators = true)

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.getMeasurementConsumerData().name)
  }

  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicRequisitionsClient by lazy {
    RequisitionsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private lateinit var clock: TestClockWithNamedInstants

  private lateinit var prober: MeasurementSystemProber

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()
    initMeasurementSystemProber()
  }

  @Before
  fun initMeasurementSystemProber() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val secretsDir: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    val privateKeyDerFile = secretsDir.resolve("${MC_DISPLAY_NAME}_cs_private.der")
    val durationBetweenMeasurement: Duration = Duration.ofSeconds(5)
    val measurementLookBackDuration = Duration.ofDays(1)
    val measurementUpdateLookBackDuration = Duration.ofHours(2)
    clock = TestClockWithNamedInstants(Instant.now())
    prober =
      MeasurementSystemProber(
        measurementConsumerData.name,
        inProcessCmmsComponents.getDataProviderResourceNames(),
        measurementConsumerData.apiAuthenticationKey,
        privateKeyDerFile,
        measurementLookBackDuration,
        durationBetweenMeasurement,
        measurementUpdateLookBackDuration,
        publicMeasurementConsumersClient,
        publicMeasurementsClient,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicRequisitionsClient,
        clock,
      )
  }

  @After
  fun stopEdpSimulators() {
    inProcessCmmsComponents.stopEdpSimulators()
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  @Test
  fun `prober creates the first measurement`(): Unit = runBlocking {
    prober.run()
    val measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(1)
  }

  @Test
  fun `prober does not create the second measurement because it is too soon`(): Unit = runBlocking {
    prober.run()
    var measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(1)

    prober.run()
    measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(1)
  }

  @Test
  fun `prober creates the first two measurements `(): Unit = runBlocking {
    prober.run()
    var measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(1)

    try {
      publicMeasurementsClient
        .withAuthenticationKey(
          inProcessCmmsComponents.getMeasurementConsumerData().apiAuthenticationKey
        )
        .cancelMeasurement(cancelMeasurementRequest { name = measurements.single().name })
    } catch (e: StatusException) {
      throw Exception("Unable to cancel measurement ${measurements.single().name}")
    }

    clock.tickSeconds(
      "Time buffer to allow the first prober measurement to finish",
      Duration.ofSeconds(6).toSeconds(),
    )

    prober.run()
    measurements = listMeasurements()
    assertThat(measurements.size).isEqualTo(2)
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    val measurements: Flow<Measurement> =
      publicMeasurementsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .listResources<Measurement, String, MeasurementsCoroutineStub> { pageToken ->
          val response =
            try {
              listMeasurements(
                listMeasurementsRequest {
                  parent = measurementConsumerData.name
                  this.pageToken = pageToken
                }
              )
            } catch (e: StatusException) {
              throw Exception(
                "Unable to list measurements for measurement consumer ${measurementConsumerData.name}",
                e,
              )
            }
          ResourceList(response.measurementsList, response.nextPageToken)
        }
        .flattenConcat()

    return measurements.toList()
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
