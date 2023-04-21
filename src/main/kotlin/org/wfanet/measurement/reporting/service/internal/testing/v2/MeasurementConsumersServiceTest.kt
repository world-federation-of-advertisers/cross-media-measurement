// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest<T : MeasurementConsumersCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(idGenerator: IdGenerator): T

  @Before
  fun initService() {
    service = newService(idGenerator)
  }

  @Test
  fun `createMeasurementConsumer succeeds`() {
    runBlocking {
      service.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )
    }
  }

  @Test
  fun `createMeasurementConsumer throws ALREADY_EXISTS when MeasurementConsumer already exists`() {
    runBlocking {
      val request = measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      service.createMeasurementConsumer(request)

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createMeasurementConsumer(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }
  }
}
