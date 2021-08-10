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

import com.google.protobuf.ByteString
import java.time.Instant
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase

private const val RANDOM_SEED = 1
private val TEST_INSTANT = Instant.ofEpochMilli(123456789L)

@RunWith(JUnit4::class)
abstract class ComputationParticipantsServiceTest<T : ComputationParticipantsCoroutineImplBase> {

  protected data class Services<T>(
    val computationParticipantsService: T,
    val measurementsService: MeasurementsCoroutineImplBase
  )

  protected val idGenerator =
    RandomIdGenerator(TestClockWithNamedInstants(TEST_INSTANT), Random(RANDOM_SEED))

  protected lateinit var computationParticipantsService: T
    private set

  protected lateinit var measurementsService: MeasurementsCoroutineImplBase
    private set

  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    computationParticipantsService = services.computationParticipantsService
    measurementsService = services.measurementsService
  }

  @Test fun `setParticipantRequisitionParams succeeds for non-last duchy`() = runBlocking {}
  @Test fun `setParticipantRequisitionParams succeeds for last duchy`() = runBlocking {}

  @Test fun `confirmComputationParticipant succeeds for non-last duchy`() = runBlocking {}
  @Test fun `confirmComputationParticipant succeeds for last duchy`() = runBlocking {}

  @Test fun `failComputationParticipant succeeds`() = runBlocking {}
}
