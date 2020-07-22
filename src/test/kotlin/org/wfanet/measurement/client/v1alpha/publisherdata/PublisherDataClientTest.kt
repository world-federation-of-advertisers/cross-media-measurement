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

package org.wfanet.measurement.client.v1alpha.publisherdata

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt

@RunWith(JUnit4::class)
class PublisherDataClientTest {
  @Rule
  @JvmField
  val grpcCleanup = GrpcCleanupRule()

  lateinit var channel: ManagedChannel
  lateinit var coroutineStub: PublisherDataGrpcKt.PublisherDataCoroutineStub

  @Before
  fun setup() {
    val serverName = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(PublisherDataFake())
        .build()
        .start()
    )
    channel = InProcessChannelBuilder.forName(
      serverName
    ).directExecutor().build()
    coroutineStub =
      PublisherDataGrpcKt.PublisherDataCoroutineStub(
        grpcCleanup.register(
          channel
        )
      )
  }

  @Test
  fun testStuff() {
    val client = PublisherDataClient(channel, coroutineStub)
    val response = runBlocking { client.getCombinedPublicKey() }
    assertThat(response).isEqualTo(CombinedPublicKey.getDefaultInstance())
  }
}

class PublisherDataFake : PublisherDataGrpcKt.PublisherDataCoroutineImplBase() {
  override suspend fun getCombinedPublicKey(
    request: GetCombinedPublicKeyRequest
  ): CombinedPublicKey {
    return CombinedPublicKey.getDefaultInstance()
  }
}
