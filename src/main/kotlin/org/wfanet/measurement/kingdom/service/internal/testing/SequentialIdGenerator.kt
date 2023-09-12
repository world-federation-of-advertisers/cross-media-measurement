/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal.testing

import java.util.concurrent.atomic.AtomicLong
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId

/**
 * Test [IdGenerator] that generates sequential IDs starting at [start].
 *
 * TODO(@SanjayVas): Move this to common-jvm.
 */
class SequentialIdGenerator(start: Long = 1) : IdGenerator {
  private val nextValue = AtomicLong(start)

  override fun generateExternalId(): ExternalId {
    return ExternalId(nextValue.getAndIncrement())
  }

  override fun generateInternalId(): InternalId {
    return InternalId(nextValue.getAndIncrement())
  }
}
