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

package org.wfanet.measurement.common.telemetry

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.context.propagation.TextMapSetter
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.withContext

/** Utilities for propagating W3C trace context through persistent asynchronous work items. */
object W3CTraceContext {
  private val propagator: TextMapPropagator = W3CTraceContextPropagator.getInstance()

  /** Serializes [context] into W3C trace-context fields suitable for a protobuf map. */
  fun inject(context: Context = Context.current()): Map<String, String> {
    val carrier = mutableMapOf<String, String>()
    propagator.inject(context, carrier, MapSetter)
    return carrier
  }

  /** Extracts a parent OpenTelemetry context from W3C trace-context [fields]. */
  fun extract(fields: Map<String, String>): Context {
    return propagator.extract(Context.current(), fields, MapGetter)
  }

  /** Runs [block] with the W3C trace context from [fields] bound to the coroutine. */
  suspend fun <T> withExtractedContext(fields: Map<String, String>, block: suspend () -> T): T {
    return withContext(extract(fields).asContextElement()) { block() }
  }

  private object MapSetter : TextMapSetter<MutableMap<String, String>> {
    override fun set(carrier: MutableMap<String, String>?, key: String, value: String) {
      carrier?.set(key, value)
    }
  }

  private object MapGetter : TextMapGetter<Map<String, String>> {
    override fun keys(carrier: Map<String, String>): Iterable<String> = carrier.keys

    override fun get(carrier: Map<String, String>?, key: String): String? {
      return carrier?.entries?.firstOrNull { it.key.equals(key, ignoreCase = true) }?.value
    }
  }
}
