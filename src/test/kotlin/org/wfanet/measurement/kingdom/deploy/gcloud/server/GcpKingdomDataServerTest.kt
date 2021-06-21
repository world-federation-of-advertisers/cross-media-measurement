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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.common.collect.Range
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Clock
import java.time.Instant
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.FinishReportRequest
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.RepetitionSpec
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigDetails
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.SpannerDataServices


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
  private val measurementConsumersStub by lazy { MeasurementConsumersStub(channel) }

  @Before
  fun populateDatabase() = runBlocking {
    insertMeasurementConsumer(MEASUREMENT_CONSUMER_ID, EXTERNAL_MEASUREMENT_CONSUMER_ID)
  }

  @Test
  fun coverage() {
    val serviceDescriptors =
      listOf(
        ReportsGrpcKt.serviceDescriptor,
        ReportLogEntriesGrpcKt.serviceDescriptor,
        RequisitionsGrpcKt.serviceDescriptor
      )

    val expectedTests =
      serviceDescriptors.flatMap { descriptor ->
        descriptor.methods.map { it.fullMethodName.substringAfterLast('.').replace('/', ' ') }
      }

    val actualTests =
      javaClass.methods.filter { it.isAnnotationPresent(Test::class.java) }.map { it.name }

    assertThat(actualTests).containsAtLeastElementsIn(expectedTests)
  }
}
