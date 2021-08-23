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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.protobuf.ByteString

/**
 * Takes in a cryptokey as ByteString and outputs the same ByteString
 *
 * Security concerns this introduces:
 *
 * The crypto key could get logged. If logs are visible to engineers, this could be vulnerable to
 * insider risk.
 *
 * The crypto key will reside in memory for longer. Other processes on the machines executing the
 * Apache Beam could potentially compromise it.
 *
 * The crypto key will be serialized and sent between Apache Beam workers. This means that
 * vulnerable temporary files or network connections could leak key material.
 */
class HardCodedDeterministicCommutativeCipherKeyProvider(private val cryptoKey: ByteString) :
  DeterministicCommutativeCipherKeyProvider {
  override fun get(): ByteString = cryptoKey
}
