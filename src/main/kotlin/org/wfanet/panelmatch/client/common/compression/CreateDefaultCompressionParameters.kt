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

package org.wfanet.panelmatch.client.common.compression

import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.compression.CompressionParameters

/** Builds a singleton PCollection that contains recommended [CompressionParameters]. */
class CreateDefaultCompressionParameters :
  PTransform<PBegin, PCollection<CompressionParameters>>() {
  override fun expand(input: PBegin): PCollection<CompressionParameters> {
    return input.apply(
      "Create",
      Create.ofProvider(
        DefaultCompressionProvider(),
        ProtoCoder.of(CompressionParameters::class.java),
      ),
    )
  }
}

private class DefaultCompressionProvider : ValueProvider<CompressionParameters> {
  override fun get(): CompressionParameters = DEFAULT_COMPRESSION_PARAMETERS

  override fun isAccessible(): Boolean = true
}
