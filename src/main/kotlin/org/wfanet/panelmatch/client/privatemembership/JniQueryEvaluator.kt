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

import org.wfanet.panelmatch.common.loadLibraryFromResource

/** [QueryEvaluator] that calls into C++ via JNI. */
class JniQueryEvaluator(private val parameters: QueryEvaluatorParameters) : QueryEvaluator {

  override fun executeQueries(
    shards: List<DatabaseShard>,
    queryBundles: List<QueryBundle>
  ): List<Result> {
    TODO()
  }

  override fun combineResults(results: Sequence<Result>): Result {
    TODO()
  }

  override fun finalizeResults(results: Sequence<Result>): Sequence<Result> {
    TODO()
  }

  companion object {
    init {
      loadLibraryFromResource("private_membership", "$SWIG_PREFIX/privatemembership")
    }
  }
}
