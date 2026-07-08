// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * A deterministic draw from a sequence of input [parts].
 *
 * Identical [parts] always yield the same value, so a draw is a pure function of its inputs with no
 * hidden state or randomness.
 */
fun interface DeterministicSampler {
  /** Returns the deterministic draw derived from [parts]. */
  fun sample(vararg parts: ByteArray): Double
}
