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

package org.wfanet.measurement.client.internal.duchy.worker

import io.grpc.ManagedChannel
import java.io.Closeable
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt

class WorkerClient(
  private val channel: ManagedChannel,
  private val stub: ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
) :
  Closeable {

  override fun close() {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
}
