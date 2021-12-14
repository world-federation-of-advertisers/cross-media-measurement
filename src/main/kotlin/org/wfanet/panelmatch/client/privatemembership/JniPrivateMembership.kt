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

import com.google.privatemembership.batch.client.Client.DecryptQueriesRequest
import com.google.privatemembership.batch.client.Client.DecryptQueriesResponse
import com.google.privatemembership.batch.client.Client.EncryptQueriesRequest
import com.google.privatemembership.batch.client.Client.EncryptQueriesResponse
import com.google.privatemembership.batch.client.Client.GenerateKeysRequest
import com.google.privatemembership.batch.client.Client.GenerateKeysResponse
import com.google.privatemembership.batch.server.Server.ApplyQueriesRequest
import com.google.privatemembership.batch.server.Server.ApplyQueriesResponse
import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.privatemembership.PrivateMembershipSwig

/** Type-safe wrapper over [PrivateMembershipSwig]. */
object JniPrivateMembership {
  init {
    loadLibraryFromResource("private_membership", "$SWIG_PREFIX/privatemembership")
  }

  fun applyQueries(request: ApplyQueriesRequest): ApplyQueriesResponse {
    return wrapJniException {
      ApplyQueriesResponse.parseFrom(
        PrivateMembershipSwig.applyQueriesWrapper(request.toByteArray())
      )
    }
  }

  fun decryptQueries(request: DecryptQueriesRequest): DecryptQueriesResponse {
    return wrapJniException {
      DecryptQueriesResponse.parseFrom(
        PrivateMembershipSwig.decryptQueriesWrapper(request.toByteArray())
      )
    }
  }

  fun encryptQueries(request: EncryptQueriesRequest): EncryptQueriesResponse {
    return wrapJniException {
      EncryptQueriesResponse.parseFrom(
        PrivateMembershipSwig.encryptQueriesWrapper(request.toByteArray())
      )
    }
  }

  fun generateKeys(request: GenerateKeysRequest): GenerateKeysResponse {
    return wrapJniException {
      GenerateKeysResponse.parseFrom(
        PrivateMembershipSwig.generateKeysWrapper(request.toByteArray())
      )
    }
  }
}
