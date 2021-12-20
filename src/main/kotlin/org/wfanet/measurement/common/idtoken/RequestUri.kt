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

package org.wfanet.measurement.common.idtoken

import java.net.URLEncoder
import org.wfanet.measurement.common.identity.externalIdToApiId

/** Creates a request uri for an open id connect authentication request. */
fun createRequestUri(state: Long, nonce: Long, redirectUri: String): String {
  val uriParts = mutableListOf<String>()
  uriParts.add("openid://?response_type=id_token")
  uriParts.add("scope=openid")
  uriParts.add("state=" + externalIdToApiId(state))
  uriParts.add("nonce=" + externalIdToApiId(nonce))
  uriParts.add("client_id=${URLEncoder.encode(redirectUri, "UTF-8")}")

  return uriParts.joinToString("&")
}
