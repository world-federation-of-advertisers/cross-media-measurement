package org.wfanet.measurement.common.testing

import com.google.common.truth.extensions.proto.ProtoSubject
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.Message
import com.nhaarman.mockitokotlin2.KArgumentCaptor
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.verifyBlocking

/**
 * Captures the sole parameter to [method] on a Mockito [mock].
 */
inline fun <reified T : Any, M> verifyAndCapture(
  mock: M,
  crossinline method: suspend M.(T) -> Any
): T =
  captureFirst {
    verifyBlocking(mock) {
      this.method(capture())
    }
  }

/**
 * Creates a captor, runs [block] in its scope, and returns the first captured value.
 */
inline fun <reified T : Any> captureFirst(block: KArgumentCaptor<T>.() -> Unit): T =
  argumentCaptor(block).firstValue

/**
 * Captures the first argument to [method], a proto message, and runs [ProtoTruth.assertThat] on it
 * for convenient chaining.
 *
 * For example:
 *   verifyProtoArgument(someMock, SomeClass::someMethod)
 *     .comparedExpectedFieldsOnly()
 *     .isEqualTo(someExpectedProto)
 */
inline fun <reified T : Message, M> verifyProtoArgument(
  mock: M,
  noinline method: suspend M.(T) -> Any
): ProtoSubject {
  return ProtoTruth.assertThat(verifyAndCapture(mock, method))
}
