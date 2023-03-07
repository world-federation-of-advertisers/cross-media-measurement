// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import java.time.Clock
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.db.computation.*
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.testing.ComputationsServiceTest
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

private const val ALSACE = "Alsace"

@RunWith(JUnit4::class)
class SpannerComputationsServiceTest : ComputationsServiceTest<ComputationsService>() {

  private val mockComputationLogEntriesService: ComputationLogEntriesCoroutineImplBase =
    mockService()
  private val tempDirectory = TemporaryFolder()
  private lateinit var storageClient: FileSystemStorageClient
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore

  val grpcTestServerRule = GrpcTestServerRule {
    storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    addService(mockComputationLogEntriesService)
  }


  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  override fun newService(): ComputationsService {
    val fakeDatabase = FakeComputationsDatabase()
    val systemComputationLogEntriesClient =
      ComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)

    return ComputationsService(
      fakeDatabase,
      systemComputationLogEntriesClient,
      ComputationStore(storageClient),
      RequisitionStore(storageClient),
      ALSACE,
      Clock.systemUTC()
    )

  }
}
