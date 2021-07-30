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

package org.wfanet.panelmatch.client.batchlookup

import java.io.Serializable

/** Provides core batch lookup operations. */
interface QueryEvaluator : Serializable {
  /** Executes [queryBundles] on [shards]. */
  fun executeQueries(shards: List<DatabaseShard>, queryBundles: List<QueryBundle>): List<Result>

  /**
   * Combines [results] into a single [Result].
   *
   * @throws IllegalArgumentException if [results] do not all have the same [QueryId].
   */
  fun combineResults(results: Sequence<Result>): Result
}
