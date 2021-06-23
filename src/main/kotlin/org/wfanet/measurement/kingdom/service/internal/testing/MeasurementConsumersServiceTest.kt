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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.GetMeasurementConsumerRequest
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private val CREATE_TIME: Timestamp = Timestamp.newBuilder().setSeconds(456).build()

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest {
  abstract val service: MeasurementConsumersCoroutineImplBase
  @Test
  fun `getMeasurementConsumer fails for missing MeasurementConsumer`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getMeasurementConsumer(
          GetMeasurementConsumerRequest.newBuilder()
            .setExternalMeasurementConsumerId(EXTERNAL_MEASUREMENT_CONSUMER_ID)
            .build()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `createMeasurementConsumer succeeds`() = runBlocking {
    val measurementConsumer =
      service.createMeasurementConsumer(
        MeasurementConsumer.newBuilder().apply { detailsBuilder.apply { apiVersion = "" } }.build()
      )

    assertThat(measurementConsumer).isEqualTo(measurementConsumer)
  }
}
