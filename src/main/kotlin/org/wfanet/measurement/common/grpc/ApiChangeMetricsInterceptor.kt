/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import io.grpc.Context
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.TimeSource
import kotlin.time.measureTime
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.type.FutureDisposition
import org.wfanet.measurement.type.FutureDispositionProto

private typealias FieldPath = List<Descriptors.FieldDescriptor>

class ApiChangeMetricsInterceptor(private val getPrincipalIdentifier: (Context) -> String?) :
  ServerInterceptor {
  /** Cache of interesting fields for a given [Message] type. */
  private val interestingFieldCache =
    ConcurrentHashMap<Descriptors.Descriptor, List<InterestingField>>()

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    if (Instrumentation.openTelemetry == OpenTelemetry.noop()) {
      return next.startCall(call, headers)
    }

    val service: String = checkNotNull(call.methodDescriptor.serviceName)
    val method: String = checkNotNull(call.methodDescriptor.bareMethodName)
    val principal: String? = getPrincipalIdentifier(Context.current())

    return object : SimpleForwardingServerCallListener<ReqT>(next.startCall(call, headers)) {
      override fun onMessage(message: ReqT) {
        val elapsed: Duration =
          TimeSource.Monotonic.measureTime {
            recordMetrics(message as Message, service, method, principal)
          }
        Interceptors.recordServerDuration(elapsed, INTERCEPTOR_NAME, service, method)
        super.onMessage(message)
      }
    }
  }

  private fun recordMetrics(request: Message, service: String, method: String, principal: String?) {
    val interestingFields: List<InterestingField> = getInterestingFields(request.descriptorForType)
    if (interestingFields.isEmpty()) {
      return
    }

    for (interestingField in interestingFields) {
      if (interestingField.deprecated) {
        if (request.haveValue(interestingField.path).any { it }) {
          deprecatedFieldSet.increment(service, method, interestingField.path.pathString, principal)
        }
      }
      if (interestingField.futureDeprecated) {
        if (request.haveValue(interestingField.path).any { it }) {
          futureDeprecatedFieldSet.increment(
            service,
            method,
            interestingField.path.pathString,
            principal,
          )
        }
      }
      if (interestingField.futureRequired) {
        if (!request.haveValue(interestingField.path).all { it }) {
          futureRequiredFieldNotSet.increment(
            service,
            method,
            interestingField.path.pathString,
            principal,
          )
        }
      }
    }
  }

  private data class InterestingField(
    val path: FieldPath,
    val deprecated: Boolean,
    val futureDeprecated: Boolean,
    val futureRequired: Boolean,
  )

  private fun getInterestingFields(
    messageDescriptor: Descriptors.Descriptor
  ): List<InterestingField> {
    return interestingFieldCache.getOrPut(messageDescriptor) {
      messageDescriptor.getInterestingFields(emptyList()).toList()
    }
  }

  companion object {
    private const val INSTRUMENTATION_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.grpc"

    private val FIELD_PATH_ATTRIBUTE =
      AttributeKey.stringKey("$INSTRUMENTATION_NAMESPACE.request_field_path")
    private val PRINCIPAL_ATTRIBUTE = AttributeKey.stringKey("$INSTRUMENTATION_NAMESPACE.principal")
    private val INTERCEPTOR_NAME = this::class.java.declaringClass.name

    private val deprecatedFieldSet =
      Instrumentation.meter
        .counterBuilder("${INSTRUMENTATION_NAMESPACE}.deprecated_field_set")
        .setDescription("Count of a deprecated field being set in a request")
        .build()

    private val futureDeprecatedFieldSet =
      Instrumentation.meter
        .counterBuilder("${INSTRUMENTATION_NAMESPACE}.future_deprecated_field_set")
        .setDescription(
          "Count of a field that is intended to be deprecated in the future being set in a " +
            "request"
        )
        .build()

    private val futureRequiredFieldNotSet =
      Instrumentation.meter
        .counterBuilder("${INSTRUMENTATION_NAMESPACE}.future_required_field_not_set")
        .setDescription(
          "Count of a field that is intended to be required in the future not being set in a " +
            "request"
        )
        .build()

    private fun LongCounter.increment(
      service: String,
      method: String,
      fieldPath: String,
      principal: String?,
    ) =
      add(
        1L,
        Attributes.of(
          Interceptors.GRPC_SERVICE_ATTRIBUTE,
          service,
          Interceptors.GRPC_METHOD_ATTRIBUTE,
          method,
          FIELD_PATH_ATTRIBUTE,
          fieldPath,
          PRINCIPAL_ATTRIBUTE,
          principal.orEmpty(),
        ),
      )

    private fun Descriptors.Descriptor.getInterestingFields(
      pathPrefix: FieldPath
    ): Sequence<InterestingField> = sequence {
      for (field: Descriptors.FieldDescriptor in fields) {
        if (field.javaType == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
          yieldAll(field.messageType.getInterestingFields(pathPrefix + field))
        } else {
          val deprecated = field.options.deprecated
          val futureDeprecated = field.options.futureDeprecated
          val futureRequired = field.options.futureRequired
          if (deprecated || futureDeprecated || futureRequired) {
            yield(
              InterestingField(pathPrefix + field, deprecated, futureDeprecated, futureRequired)
            )
          }
        }
      }
    }
  }
}

private val DescriptorProtos.FieldOptions.futureDeprecated: Boolean
  get() = getExtension(FutureDispositionProto.futureDisposition) == FutureDisposition.DEPRECATED

private val DescriptorProtos.FieldOptions.futureRequired: Boolean
  get() = getExtension(FutureDispositionProto.futureDisposition) == FutureDisposition.REQUIRED

private val FieldPath.pathString: String
  get() = joinToString(".") { it.name }

/**
 * Returns one element for each instance of the specified field within this message, where the
 * element is whether than instance has a value.
 *
 * An instance of a field has a value if it would satisfy [com.google.api.FieldBehavior.REQUIRED].
 */
private fun Message.haveValue(fieldPath: FieldPath): Sequence<Boolean> {
  if (fieldPath.isEmpty()) {
    return emptySequence()
  }

  return sequence {
    if (fieldPath.size == 1) {
      val leafField: Descriptors.FieldDescriptor = fieldPath.single()
      val hasValue =
        if (leafField.isRepeated) {
          (getField(leafField) as List<*>).isNotEmpty()
        } else {
          hasField(leafField)
        }
      yield(hasValue)
    } else {
      val current: Descriptors.FieldDescriptor = fieldPath.first()
      val remaining: List<Descriptors.FieldDescriptor> = fieldPath.takeLast(fieldPath.size - 1)

      if (current.isRepeated) {
        val values = getField(current) as List<*>
        for (value in values) {
          require(value is Message) {
            "${fieldPath.pathString} not found in message $this@haveValue"
          }
          yieldAll(value.haveValue(remaining))
        }
      } else {
        val value = getField(current)
        require(value is Message) { "${fieldPath.pathString} not found in message $this@haveValue" }
        yieldAll(value.haveValue(remaining))
      }
    }
  }
}
