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

import com.google.protobuf.Timestamp
import org.junit.Test
import kotlinx.coroutines.runBlocking
import org.junit.runner.RunWith
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.runners.JUnit4
import kotlin.test.assertEquals

private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = 123L
private val CREATE_TIME: Timestamp = Timestamp.newBuilder().setSeconds(456).build()

@RunWith(JUnit4::class)
abstract class MeasurementConsumersServiceTest {
  abstract val measurementConsumerService: BindableService
  @Test fun `sth`() = runBlocking { assertEquals(5, 5) }
}
