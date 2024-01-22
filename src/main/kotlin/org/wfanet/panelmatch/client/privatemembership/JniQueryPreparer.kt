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

import com.google.protobuf.ByteString
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.querypreparer.QueryPreparerSwig

/** [QueryPreparer] that calls into C++ via JNI. */
class JniQueryPreparer : QueryPreparer {

  override fun prepareLookupKeys(
    identifierHashPepper: ByteString,
    decryptedJoinKeyAndIds: List<JoinKeyAndId>,
  ): List<LookupKeyAndId> {

    val request = prepareQueryRequest {
      this.decryptedJoinKeyAndIds += decryptedJoinKeyAndIds
      this.identifierHashPepper = identifierHashPepper
    }
    val response = wrapJniException {
      PrepareQueryResponse.parseFrom(QueryPreparerSwig.prepareQueryWrapper(request.toByteArray()))
    }
    return response.lookupKeyAndIdsList
  }

  companion object {
    init {
      loadLibraryFromResource("query_preparer", "$SWIG_PREFIX/querypreparer")
    }
  }
}
