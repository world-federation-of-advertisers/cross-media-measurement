/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common

import io.grpc.Channel
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.service.internal.Services as InternalServices
import org.wfanet.measurement.access.service.v1alpha.Services
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially

class InProcessAccess(
  verboseGrpcLogging: Boolean,
  private val getInternalServices: () -> InternalServices,
) : TestRule {
  private val internalAccessServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      val services = getInternalServices().toList()
      for (service in services) {
        addService(service)
      }
    }

  private val accessServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      val internalChannel = internalAccessServer.channel
      val services = Services.build(internalChannel).toList()
      for (service in services) {
        addService(service)
      }
    }

  val channel: Channel
    get() = accessServer.channel

  override fun apply(base: Statement, description: Description): Statement {
    return chainRulesSequentially(internalAccessServer, accessServer).apply(base, description)
  }
}
