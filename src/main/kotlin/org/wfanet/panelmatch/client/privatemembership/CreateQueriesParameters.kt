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

data class CreateQueriesParameters(
  val numShards: Int,
  val numBucketsPerShard: Int,
  val maxQueriesPerShard: Int,
  val padQueries: Boolean,
) {
  init {
    require(numShards > 0)
    require(numBucketsPerShard > 0)
    require(maxQueriesPerShard > 0)
  }
}
