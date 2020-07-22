// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common.testing

import com.google.protobuf.Message
import io.grpc.kotlin.AbstractCoroutineServerImpl
import kotlin.reflect.KFunction2
import kotlin.reflect.KSuspendFunction2
import kotlinx.coroutines.flow.Flow

/**
 * A lightweight, opinionated version of mocking.
 *
 * This captures all arguments and crashes unless a return value was supplied.
 *
 * Usage:
 *   class FakeFooService : FooCoroutineImplBase(), ServiceMocker<FooCoroutineImplBase>() {
 *
 *     val mocker = ServiceMocker<RequisitionStorageCoroutineImplBase>()
 *
 *     override suspend fun someMethod(request: Foo): Bar = mocker.handleCall(request)
 *   }
 *
 *   val fakeFooService = FakeFooService()
 *   fakeFooService.mocker.mock(FooCoroutineImplBase::someMethod) { someReturnValue }
 *   ...
 *   assertThat(fakeFooService.mocker.callsForMethod("someMethod")).containsExactly(someRequest)
 */
@Suppress("UNCHECKED_CAST")
class ServiceMocker<T : AbstractCoroutineServerImpl> {
  private val handlers = mutableMapOf<String, (Message) -> Any>()

  private data class GrpcCall(val methodName: String, val arg: Message)
  private val calls = mutableListOf<GrpcCall>()

  /** Returns the list, in order, of all calls that happened for a method. */
  fun callsForMethod(name: String): List<Message> =
    calls.filter { it.methodName == name }.map { it.arg }

  /** Delegates to a mocked method. */
  fun <ReturnT> handleCall(request: Message): ReturnT {
    val method = callerName()
    val handler = handlers[method] ?: error("Method not mocked: $method")
    calls.add(GrpcCall(method, request))
    return handler(request) as ReturnT
  }

  /** Returns the name of the function that calls [handleCall]. */
  private fun callerName() = Thread.currentThread().stackTrace[3].methodName

  /** Sets the implementation of a unary non-streaming gRPC method. */
  fun <M : T, RequestT : Message, ResponseT : Message> mock(
    method: KSuspendFunction2<M, RequestT, ResponseT>,
    block: (RequestT) -> ResponseT
  ) {
    mockInternal(method.name, block)
  }

  /** Sets the implementation of a server-side streaming gRPC method. */
  fun <M : T, RequestT : Message, ResponseT : Message> mockStreaming(
    method: KFunction2<M, RequestT, Flow<ResponseT>>,
    block: (RequestT) -> Flow<ResponseT>
  ) {
    mockInternal(method.name, block)
  }

  /** Resets all recorded calls and mocked behavior. */
  fun reset() {
    calls.clear()
    handlers.clear()
  }

  private fun <RequestT : Message> mockInternal(
    methodName: String,
    block: (RequestT) -> Any
  ) {
    check(!handlers.containsKey(methodName)) { "Mock already defined for $methodName" }
    handlers[methodName] = { block(it as RequestT) }
  }
}
