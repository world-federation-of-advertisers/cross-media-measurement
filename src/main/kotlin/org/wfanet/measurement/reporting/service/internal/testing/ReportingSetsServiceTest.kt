// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.reporting.ReportingSetKt
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.batchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.getReportingSetRequest
import org.wfanet.measurement.internal.reporting.reportingSet
import org.wfanet.measurement.internal.reporting.streamReportingSetsRequest

private const val FIXED_EXTERNAL_ID = 123456789L
private const val FIXED_INTERNAL_ID = 987654321L

@RunWith(JUnit4::class)
abstract class ReportingSetsServiceTest<T : ReportingSetsCoroutineImplBase> {
  protected val idGenerator =
    FixedIdGenerator(InternalId(FIXED_INTERNAL_ID), ExternalId(FIXED_EXTERNAL_ID))

  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initService() {
    service = newService(idGenerator)
  }

  @Test
  fun `createReportingSet succeeds`() {
    val reportingSet = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "4321"
        }
      filter = "filter"
      displayName = "displayName"
    }

    runBlocking {
      val createdReportingSet = service.createReportingSet(reportingSet)
      assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
      val retrievedReportingSet =
        service.getReportingSet(
          getReportingSetRequest {
            measurementConsumerReferenceId = createdReportingSet.measurementConsumerReferenceId
            externalReportingSetId = createdReportingSet.externalReportingSetId
          }
        )
      assertThat(createdReportingSet).ignoringRepeatedFieldOrder().isEqualTo(retrievedReportingSet)
    }
  }

  @Test
  fun `createReportingSet fails when generated IDs are the same`() {
    val reportingSet = reportingSet {
      measurementConsumerReferenceId = "1234"
      filter = "filter"
      displayName = "displayName"
    }

    runBlocking {
      service.createReportingSet(reportingSet)
      val exception =
        assertFailsWith<StatusRuntimeException> { service.createReportingSet(reportingSet) }
      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }
  }

  @Test
  fun `getReportingSet throws NOT FOUND when reporting set doesn't exist`() {
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getReportingSet(
            getReportingSetRequest {
              measurementConsumerReferenceId = "1234"
              externalReportingSetId = 1234
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }
  }

  @Test
  fun `streamReportingSets limit truncates results`() {
    val reportingSet1 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    val createdReportingSet1 = runBlocking { service.createReportingSet(reportingSet1) }

    val reportingSet2 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    idGenerator.internalId = InternalId(FIXED_INTERNAL_ID + 1)
    idGenerator.externalId = ExternalId(FIXED_EXTERNAL_ID + 1)
    runBlocking { service.createReportingSet(reportingSet2) }

    val reportingSets = runBlocking {
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter = StreamReportingSetsRequestKt.filter { measurementConsumerReferenceId = "1234" }
            limit = 1
          }
        )
        .toList()
    }

    assertThat(reportingSets).containsExactly(createdReportingSet1)
  }

  @Test
  fun `streamReportingSets measurementConsumerReferenceId filter limits results`() {
    val reportingSet1 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    val createdReportingSet1 = runBlocking { service.createReportingSet(reportingSet1) }

    val reportingSet2 = reportingSet {
      measurementConsumerReferenceId = "4321"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "4321"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    idGenerator.internalId = InternalId(FIXED_INTERNAL_ID + 1)
    idGenerator.externalId = ExternalId(FIXED_EXTERNAL_ID + 1)
    runBlocking { service.createReportingSet(reportingSet2) }

    val reportingSets = runBlocking {
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter = StreamReportingSetsRequestKt.filter { measurementConsumerReferenceId = "1234" }
          }
        )
        .toList()
    }

    assertThat(reportingSets).containsExactly(createdReportingSet1)
  }

  @Test
  fun `streamReportingSets can get the next page of results`() {
    val reportingSet1 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    runBlocking { service.createReportingSet(reportingSet1) }

    val reportingSet2 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    idGenerator.internalId = InternalId(FIXED_INTERNAL_ID + 1)
    idGenerator.externalId = ExternalId(FIXED_EXTERNAL_ID + 1)
    val createdReportingSet2 = runBlocking { service.createReportingSet(reportingSet2) }

    val reportingSets = runBlocking {
      service
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                measurementConsumerReferenceId = "1234"
                externalReportingSetIdAfter = FIXED_EXTERNAL_ID
              }
            limit = 1
          }
        )
        .toList()
    }

    assertThat(reportingSets).containsExactly(createdReportingSet2)
  }

  @Test
  fun `batchGetReportingSet can get multiple reporting sets`() {
    val reportingSet1 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    val createdReportingSet = runBlocking { service.createReportingSet(reportingSet1) }

    val reportingSet2 = reportingSet {
      measurementConsumerReferenceId = "1234"
      eventGroupKeys +=
        ReportingSetKt.eventGroupKey {
          measurementConsumerReferenceId = "1234"
          dataProviderReferenceId = "1234"
          eventGroupReferenceId = "1234"
        }
      filter = "filter"
      displayName = "displayName"
    }
    idGenerator.internalId = InternalId(FIXED_INTERNAL_ID + 1)
    idGenerator.externalId = ExternalId(FIXED_EXTERNAL_ID + 1)
    val createdReportingSet2 = runBlocking { service.createReportingSet(reportingSet2) }

    val reportingSets = runBlocking {
      service
        .batchGetReportingSet(
          batchGetReportingSetRequest {
            measurementConsumerReferenceId = "1234"
            externalReportingSetIds += FIXED_EXTERNAL_ID
            externalReportingSetIds += FIXED_EXTERNAL_ID + 1
          }
        )
        .toList()
    }

    assertThat(reportingSets).containsExactly(createdReportingSet2, createdReportingSet)
  }

  @Test
  fun `batchGetReportingSet returns nothing when no ids are given`() {
    val reportingSets = runBlocking {
      service
        .batchGetReportingSet(
          batchGetReportingSetRequest { measurementConsumerReferenceId = "1234" }
        )
        .toList()
    }

    assertThat(reportingSets).hasSize(0)
  }
}
