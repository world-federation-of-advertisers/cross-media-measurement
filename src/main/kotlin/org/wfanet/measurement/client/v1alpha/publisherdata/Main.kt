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

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt

fun main() = runBlocking {
  val channel =
    ManagedChannelBuilder.forAddress("localhost", 31125).usePlaintext().build()
  val stub = PublisherDataGrpcKt.PublisherDataCoroutineStub(channel)

  val response: CombinedPublicKey =
    PublisherDataClient(channel, stub).use {
      it.getCombinedPublicKey()
    }
  println("Response: $response")
}
