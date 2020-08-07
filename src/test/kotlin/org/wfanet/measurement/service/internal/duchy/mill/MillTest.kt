// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.internal.duchy.mill

import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.computationcontrol.ComputationControlServiceImpl
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class MillTest {
  @get:Rule
  val grpcCleanup = GrpcCleanupRule()

  private val duchyNames = listOf("Alsace", "Bavaria", "Carinthia")
  private lateinit var mills: List<Mill>

  // TODO Use the ComputationStorageService to determine what work the mill needs to do.

  @Before
  fun setup() {
    val workerServiceMap = duchyNames.associateWith { setupComputationControlService() }
    mills = duchyNames.map {
      Mill(workerServiceMap, 1000)
    }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ComputationStorageServiceImpl(FakeComputationStorage(duchyNames)))
  }

  @Test
  fun `mill polls for work 3 times`() = runBlocking {
    // TODO: Should be a real test of something.
  }

  private fun setupComputationControlService(): ComputationControlServiceCoroutineStub {
    val serverName = InProcessServerBuilder.generateName()
    val client = ComputationControlServiceCoroutineStub(
      grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build()
      )
    )
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(
          ComputationControlServiceImpl(
            LiquidLegionsSketchAggregationComputationStorageClients(
              ComputationStorageServiceCoroutineStub(grpcTestServerRule.channel),
              FakeComputationsBlobDb(mutableMapOf()),
              duchyNames
            )
          )
        )
        .build()
        .start()
    )
    return client
  }
}
