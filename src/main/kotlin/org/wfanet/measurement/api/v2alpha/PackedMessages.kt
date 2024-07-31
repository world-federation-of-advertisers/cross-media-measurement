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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import com.google.protobuf.kotlin.unpack
import org.wfanet.measurement.common.ProtoReflection

/** The packed value contained in this [SignedMessage]. */
val SignedMessage.packedValue: ByteString
  get() {
    return if (hasMessage()) {
      message.value
    } else {
      @Suppress("DEPRECATION") // Handle legacy resources.
      data
    }
  }

/**
 * Unpacks the protobuf message in this [SignedMessage].
 *
 * @throws com.google.protobuf.InvalidProtocolBufferException if the message type is not [T]
 */
inline fun <reified T : Message> SignedMessage.unpack(): T {
  return if (hasMessage()) {
    message.unpack()
  } else {
    @Suppress("DEPRECATION") // Handle legacy resources.
    ProtoReflection.getDefaultInstance<T>().parserForType.parseFrom(data) as T
  }
}

/** Returns whether the [Message] type contained in this [EncryptedMessage] is [T]. */
inline fun <reified T : Message> EncryptedMessage.isA(): Boolean {
  return isA(ProtoReflection.getDefaultInstance<T>().descriptorForType)
}

/**
 * Returns whether the [Message] type contained in this [EncryptedMessage] is described by
 * [descriptor].
 */
fun EncryptedMessage.isA(descriptor: Descriptors.Descriptor): Boolean {
  return typeUrl == ProtoReflection.getTypeUrl(descriptor)
}

/**
 * Sets [SignedMessageKt.Dsl.message] and [SignedMessageKt.Dsl.data].
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#1301): Inline this once we no longer
 *   need to set `data`.
 */
fun SignedMessageKt.Dsl.setMessage(value: ProtoAny) {
  message = value

  // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this field.
  data = this.message.value
}
