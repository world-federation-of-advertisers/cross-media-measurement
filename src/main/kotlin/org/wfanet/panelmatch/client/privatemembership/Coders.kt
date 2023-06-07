// Copyright 2022 The Cross-Media Measurement Authors
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

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.MapCoder
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder

/**
 * Apache Beam coder for Map<QueryId, PaddingNonce>.
 *
 * TODO: allow Apache Beam to infer the correct coder.
 */
val paddingNonceMapCoder: Coder<Map<QueryId, PaddingNonce>> =
  MapCoder.of(ProtoCoder.of(QueryId::class.java), ProtoCoder.of(PaddingNonce::class.java))
