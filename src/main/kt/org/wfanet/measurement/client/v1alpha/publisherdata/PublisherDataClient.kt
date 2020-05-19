package org.wfanet.measurement.client.v1alpha.publisherdata

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.io.Closeable
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt

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
    val request = GetCombinedPublicKeyRequest.newBuilder().apply {
      keyBuilder.combinedPublicKeyId = "\"Ceci n'est pas une clé\""
    }.build()
    val response = stub.getCombinedPublicKey(request)
    println("Response: $response")
  }
}
