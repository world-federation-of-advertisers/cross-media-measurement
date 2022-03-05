/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * Returns a list of privacy bucket groups that might be affected by a query.
 *
 * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
 * range and demo groups are obtained from this.
 * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
 * sampling interval is obtained from from this.
 * @return A list of potentially affected PrivacyBucketGroups. It is guaranteed that the items in
 * this list are disjoint. In the current implementation, each privacy bucket group represents a
 * single privacy bucket.
 */
internal fun getPrivacyBucketGroups(
  measurementSpec: MeasurementSpec,
  requisitionSpec: RequisitionSpec
): List<PrivacyBucketGroup> = TODO("Not implemented $measurementSpec $requisitionSpec")
