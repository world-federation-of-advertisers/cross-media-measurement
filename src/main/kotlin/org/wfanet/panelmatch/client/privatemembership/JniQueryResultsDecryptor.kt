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

import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException

/**
 * A [QueryResultsDecryptor] implementation using the JNI [QueryResultDecryptorWrapper]. Keys should
 * have been generated prior to this step using an implementation of [PrivateMembershipCryptor].
 */
class JniQueryResultsDecryptor : QueryResultsDecryptor {

  override fun decryptQueryResults(
    request: DecryptQueryResultsRequest
  ): DecryptQueryResultsResponse {
    return wrapJniException {
      DecryptQueryResultsResponse.parseFrom(
        DecryptQueryResultsWrapper.decryptQueryResultsWrapper(request.toByteArray())
      )
    }
  }

  companion object {
    init {
      loadLibraryFromResource("decrypt_query_results", "$SWIG_PREFIX/decryptqueryresults")
    }
  }
}
