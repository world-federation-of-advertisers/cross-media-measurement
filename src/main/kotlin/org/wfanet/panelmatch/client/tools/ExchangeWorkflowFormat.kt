// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.tools

import com.google.privatemembership.batch.Shared.Parameters
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.panelmatch.client.common.toInternal
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow

/** Formats for serializing and deserializing an exchange workflow. */
enum class ExchangeWorkflowFormat {
  /**
   * A serialized [V2AlphaExchangeWorkflow] for exchanges that are Kingdom-based and use the
   * `v2alpha` API.
   */
  V2ALPHA {
    override fun parseBytes(serializedExchangeWorkflow: ByteString): ExchangeWorkflow =
      V2AlphaExchangeWorkflow.parseFrom(serializedExchangeWorkflow).toInternal()

    override fun parseTextProto(file: File): Message =
      parseTextProto(file, V2AlphaExchangeWorkflow.getDefaultInstance(), TYPE_REGISTRY)
  },

  /** A serialized [ExchangeWorkflow] for exchanges that are Kingdom-less. */
  KINGDOMLESS {
    override fun parseBytes(serializedExchangeWorkflow: ByteString): ExchangeWorkflow =
      ExchangeWorkflow.parseFrom(serializedExchangeWorkflow)

    override fun parseTextProto(file: File): Message =
      parseTextProto(file, ExchangeWorkflow.getDefaultInstance(), TYPE_REGISTRY)
  };

  /**
   * Parses [sereializedExchangeWorkflow] and returns it as a panel match-internal
   * [ExchangeWorkflow]. Depending on the format, this may parse the input as a different message
   * type and then convert it.
   */
  abstract fun parseBytes(serializedExchangeWorkflow: ByteString): ExchangeWorkflow

  /**
   * Parses the contents of [file] as a textproto and returns a [Message] instance based on [this]
   * format.
   */
  abstract fun parseTextProto(file: File): Message

  companion object {
    private val TYPE_REGISTRY =
      TypeRegistry.newBuilder().apply { add(Parameters.getDescriptor()) }.build()
  }
}
