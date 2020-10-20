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

package org.wfanet.measurement.duchy.deploy.common.server

import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.duchy.service.internal.metricvalues.MetricValuesService
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

abstract class MetricValuesServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var serverFlags: CommonServer.Flags
    private set

  protected fun run(metricValueDb: MetricValueDatabase, storageClient: StorageClient) {
    CommonServer.fromFlags(
      serverFlags,
      javaClass.name,
      MetricValuesService(metricValueDb, storageClient)
    ).start().blockUntilShutdown()
  }

  companion object {
    const val SERVICE_NAME = "MetricValues"
  }
}
