package org.wfanet.measurement.client.v1alpha.publisherdata

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt
import java.io.Closeable
import java.util.concurrent.TimeUnit

class PublisherDataClient : Closeable {
  private val channel: ManagedChannel
  private val stub: PublisherDataGrpcKt.PublisherDataCoroutineStub
  private val host: String = "localhost"
  private val port: Int = 31125

  init {
    channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    stub = PublisherDataGrpcKt.PublisherDataCoroutineStub(channel)
  }

  override fun close() {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }

  suspend fun getCombinedPublicKey() = coroutineScope {
    println("Sending request to GetCombinedPublicKey...")
    val response = async {
      stub.getCombinedPublicKey(GetCombinedPublicKeyRequest.newBuilder().setKey(
        CombinedPublicKey.Key.newBuilder()
          .setCombinedPublicKeyId("\"Ceci n'est pas une clé\""))
                                  .build())
    }
    println("Response: ${response.await()}")
  }
}
