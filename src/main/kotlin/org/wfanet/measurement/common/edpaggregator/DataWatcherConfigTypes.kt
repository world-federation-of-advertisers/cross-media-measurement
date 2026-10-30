/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.edpaggregator

import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ExternalDataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

/** Protobuf message types supported in `DataWatcherConfig` route parameters. */
object DataWatcherConfigTypes {
  val registry: TypeRegistry =
    TypeRegistry.newBuilder()
      .add(ResultsFulfillerParams.getDescriptor())
      .add(EventGroupSyncParams.getDescriptor())
      .add(ExternalDataAvailabilitySyncParams.getDescriptor())
      .build()
}
