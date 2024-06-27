/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.cloud.Timestamp
import org.wfanet.measurement.gcloud.common.toInstant

object ETags {
  /** Returns an RFC 7232 entity tag from [updateTime]. */
  fun computeETag(updateTime: Timestamp): String {
    return org.wfanet.measurement.common.api.ETags.computeETag(updateTime.toInstant())
  }
}
