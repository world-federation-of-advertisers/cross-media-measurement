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

package org.wfanet.panelmatch.client.privatemembership

import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesWorkflow.Parameters
import org.wfanet.panelmatch.client.privatemembership.testing.AbstractEvaluateQueriesWorkflowEndToEndTest
import org.wfanet.panelmatch.client.privatemembership.testing.JniQueryEvaluatorContext
import org.wfanet.panelmatch.client.privatemembership.testing.JniQueryEvaluatorTestHelper
import org.wfanet.panelmatch.client.privatemembership.testing.QueryEvaluatorTestHelper

class JniEvaluateQueriesWorkflowEndToEndTest : AbstractEvaluateQueriesWorkflowEndToEndTest() {
  private val contexts = mutableMapOf<Parameters, JniQueryEvaluatorContext>()

  private fun getContext(parameters: Parameters): JniQueryEvaluatorContext {
    return contexts.getOrPut(parameters) {
      JniQueryEvaluatorContext(parameters.numShards, parameters.numBucketsPerShard)
    }
  }

  override fun makeQueryEvaluator(parameters: Parameters): QueryEvaluator {
    return JniQueryEvaluator(getContext(parameters).privateMembershipParameters.toByteString())
  }

  override fun makeHelper(parameters: Parameters): QueryEvaluatorTestHelper {
    return JniQueryEvaluatorTestHelper(getContext(parameters))
  }
}
