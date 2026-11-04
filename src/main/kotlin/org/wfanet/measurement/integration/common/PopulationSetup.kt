/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common

import com.google.common.hash.Hashing
import com.google.protobuf.BoolValue
import com.google.protobuf.util.Durations
import io.grpc.Channel
import io.grpc.StatusRuntimeException
import io.grpc.serviceconfig.MethodConfigKt
import io.grpc.serviceconfig.copy
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig

/** Idempotent population setup for cloud tests. */
object PopulationSetup {
  /** Allows connection recovery for CreatePopulation while retaining the shared retry policy. */
  val SERVICE_CONFIG =
    ProtobufServiceConfig.DEFAULT.copy {
      methodConfig +=
        ProtobufServiceConfig.DEFAULT.message.getMethodConfig(0).copy {
          name.clear()
          name +=
            MethodConfigKt.name {
              service = PopulationsGrpc.SERVICE_NAME
              method = "CreatePopulation"
            }
          // gRPC uses the shorter service-config or stub deadline, so override this method.
          timeout = Durations.fromSeconds(120)
          waitForReady = BoolValue.of(true)
        }
    }

  @Blocking
  fun ensurePopulation(
    channel: Channel,
    parent: String,
    populationSpec: PopulationSpec,
    description: String,
  ): Population {
    // Serialization may vary across builds; stable bytes within a run keep retries idempotent.
    @OptIn(ExperimentalStdlibApi::class)
    val requestId =
      Hashing.murmur3_128()
        .hashBytes(populationSpec.toByteArray())
        .asBytes()
        .toHexString(HexFormat.Default)
    return try {
      PopulationsGrpc.newBlockingStub(channel)
        .createPopulation(
          createPopulationRequest {
            this.parent = parent
            population = population {
              this.populationSpec = populationSpec
              this.description = description
            }
            this.requestId = requestId
          }
        )
    } catch (e: StatusRuntimeException) {
      throw IllegalStateException("Error creating population for $parent", e)
    }
  }
}
