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

package org.wfanet.panelmatch.client.eventpostprocessing

import org.wfanet.panelmatch.client.common.BrotliCompressorFactory
import org.wfanet.panelmatch.client.common.BrotliDictionaryBuilder
import org.wfanet.panelmatch.client.common.DictionaryBuilder
import org.wfanet.panelmatch.client.eventpostprocessing.testing.AbstractUncompressEventsFnTest

class BrotliCompressorUncompressEventsFnTest : AbstractUncompressEventsFnTest() {
  override val dictionaryBuilder: DictionaryBuilder = BrotliDictionaryBuilder()
  override val compressorFactory = BrotliCompressorFactory()
}
