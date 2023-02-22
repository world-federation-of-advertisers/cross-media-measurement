/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.protobuf.Message
import org.projectnessie.cel.Program

/** Maps Privacy bucket related objects to event filter related objects and vice versa. */
interface PrivacyBucketMapper {

  /** Maps [filterExpression] to a [Program] by using privacy related fields and [Message] */
  fun toPrivacyFilterProgram(filterExpression: String): Program

  /** Maps [privacyBucketGroup] to an event [Message] */
  fun toEventMessage(privacyBucketGroup: PrivacyBucketGroup): Message
}
