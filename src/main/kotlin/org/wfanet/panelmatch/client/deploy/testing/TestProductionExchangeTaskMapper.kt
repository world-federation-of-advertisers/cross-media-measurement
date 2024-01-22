// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.deploy.testing

import java.time.Clock
import java.time.Duration
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.deploy.ProductionExchangeTaskMapper
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager

private val preprocessingParameters =
  PreprocessingParameters(maxByteSize = 1024 * 1024, fileCount = 1)
private val taskContext = TaskParameters(setOf(preprocessingParameters))
private val clock = Clock.systemUTC()

/** Version of ProductionTaskMapper for Testing. */
class TestProductionExchangeTaskMapper(
  privateStorageSelector: PrivateStorageSelector,
  sharedStorageSelector: SharedStorageSelector,
) :
  ProductionExchangeTaskMapper(
    privateStorageSelector = privateStorageSelector,
    sharedStorageSelector = sharedStorageSelector,
    certificateManager = TestCertificateManager,
    inputTaskThrottler = MinimumIntervalThrottler(clock, Duration.ofMillis(250)),
    makePipelineOptions = PipelineOptionsFactory::create,
    taskContext = taskContext,
  )
