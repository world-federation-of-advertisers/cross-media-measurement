// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common.server

import kotlinx.coroutines.runInterruptible
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.service.internal.buildDataServices
import picocli.CommandLine

abstract class KingdomDataServer : Runnable {
  @CommandLine.Mixin
  private lateinit var serverFlags: CommonServer.Flags

  @CommandLine.Mixin
  private lateinit var duchyIdFlags: DuchyIdFlags

  protected suspend fun run(database: KingdomRelationalDatabase) {
    DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)

    val services = buildDataServices(database)
    val server = CommonServer.fromFlags(serverFlags, this::class.simpleName!!, services)

    runInterruptible { server.start().blockUntilShutdown() }
  }
}
