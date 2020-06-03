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
