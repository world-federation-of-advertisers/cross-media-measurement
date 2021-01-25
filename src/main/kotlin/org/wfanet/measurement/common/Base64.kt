// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import java.util.Base64

/** Encodes [ByteArray] with RFC 7515's Base64url encoding into a base-64 string. */
fun ByteArray.base64UrlEncode(): String =
  Base64.getUrlEncoder().withoutPadding().encodeToString(this)

/** Decodes a [String] encoded with RFC 7515's Base64url into a [ByteArray]. */
fun String.base64UrlDecode(): ByteArray =
  Base64.getUrlDecoder().decode(this)
