// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.server

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import org.junit.Before
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val MEASUREMENT_CONSUMER_ID = 1L
private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 2L
private const val PUBLIC_KEY_CERTIFICATE_ID = 3L

/**
 * Integration test for Kingdom internal services + Spanner.
 *
 * This minimally tests each RPC method. Edge cases are tested in individual unit tests for the
 * services. This focuses on ensuring that the databases integrate with the gRPC services.
 */
class GcpKingdomDataServerTest : KingdomDatabaseTestBase() {
  @get:Rule
  val grpcTestServer =
    GrpcTestServerRule(logAllRequests = true) {
      val clock = Clock.systemUTC()
<<<<<<< HEAD
      val databases = makeSpannerKingdomDatabases(clock, RandomIdGenerator(clock), databaseClient)
<<<<<<< HEAD
<<<<<<< HEAD
      val services =
        buildLegacyDataServices(databases.reportDatabase, databases.requisitionDatabase)
=======
      val services = buildLegacyDataServices(databases.reportDatabase, databases.requisitionDatabase)
>>>>>>> bbcf20ac (first commit)
=======
      val services =
        buildLegacyDataServices(databases.reportDatabase, databases.requisitionDatabase)
>>>>>>> 382125ff (fixed build and lint more)
=======
      val services =
        SpannerDataServices(clock, RandomIdGenerator(clock), databaseClient).buildDataServices()
>>>>>>> f73cb724 (preparing)
      services.forEach(this::addService)
    }

  private val channel by lazy { grpcTestServer.channel }
  private val measurementConsumersStub by lazy { MeasurementConsumersCoroutineStub(channel) }

  @Before
  fun populateDatabase() = runBlocking {
    insertCertificate(PUBLIC_KEY_CERTIFICATE_ID)
    insertMeasurementConsumer(
      MEASUREMENT_CONSUMER_ID,
      EXTERNAL_MEASUREMENT_CONSUMER_ID,
      PUBLIC_KEY_CERTIFICATE_ID
    )
  }

  @Test
  fun coverage() {
    val serviceDescriptors = listOf(MeasurementConsumersGrpcKt.serviceDescriptor,
                                    CertificatesGrpcKt.serviceDescriptor)

    val expectedTests =
      serviceDescriptors.flatMap { descriptor ->
        descriptor.methods.map { it.fullMethodName.substringAfterLast('.').replace('/', ' ') }
      }

    val actualTests =
      javaClass.methods.filter { it.isAnnotationPresent(Test::class.java) }.map { it.name }

    assertThat(actualTests).containsAtLeastElementsIn(expectedTests)
  }

  @Test
  fun `MeasurementConsumers GetMeasurementConsumer`() = runBlocking {
//     val request =
//       GetMeasurementConsumerRequest.newBuilder()
//         .setExternalMeasurementConsumerId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
//         .build()
//     val expected =
//       MeasurementConsumer.newBuilder()
//         .apply { externalMeasurementConsumerId = EXTERNAL_MEASUREMENT_CONSUMER_ID }
//         .build()

//     val result = measurementConsumersStub.getMeasurementConsumer(request)
//     assertThat(result).comparingExpectedFieldsOnly().isEqualTo(expected)
  }

  @Test
  fun `MeasurementConsumers CreateMeasurementConsumer`() = runBlocking {
    // assertThat(5).isEqualTo(5)
  }

  @Test
  fun `Certificates CreateCertificate`() = runBlocking {
    // assertThat(5).isEqualTo(5)
  }

  @Test
  fun `Certificates GetCertificate`() = runBlocking {
    // TODO(uakyol) : implement this test
  }

  @Test
  fun `Certificates RevokeCertificate`() = runBlocking {
    // TODO(uakyol) : implement this test
  }

  @Test
  fun `Certificates ReleaseCertificateHold`() = runBlocking {
    // TODO(uakyol) : implement this test
  }
}
