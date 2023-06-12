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

package org.wfanet.panelmatch.client.common.testing

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.beam.byteStringKvCoder
import org.wfanet.panelmatch.common.beam.kvOf
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase

fun BeamTestBase.eventsOf(
  vararg pairs: Pair<String, String>
): PCollection<KV<ByteString, ByteString>> {
  @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
  return pcollectionOf(
    "Create Events",
    pairs.map { kvOf(it.first.toByteStringUtf8(), it.second.toByteStringUtf8()) },
    coder = byteStringKvCoder
  )
}
