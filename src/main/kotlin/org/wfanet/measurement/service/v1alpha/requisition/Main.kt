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

package org.wfanet.measurement.service.v1alpha.requisition

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt

object StubFlags {
  val INTERNAL_API_HOST = stringFlag("internal_api_host", "")
  val INTERNAL_API_PORT = intFlag("internal_api_port", 0)
}

fun main(args: Array<String>) {
  Flags.parse(args.toList())

  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forAddress(StubFlags.INTERNAL_API_HOST.value, StubFlags.INTERNAL_API_PORT.value)
      .usePlaintext()
      .build()

  val stub = RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub(channel)

  CommonServer("RequisitionServer", 8080, RequisitionService(stub))
    .start()
    .blockUntilShutdown()
}
