package org.wfanet.measurement.service.v1alpha.common

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils

/**
 * Details about an authenticated Duchy.
 *
 * @property[authenticatedDuchyId] Stable identifier for a duchy. Null if unauthenticated.
 */
data class DuchyAuth(val authenticatedDuchyId: String)

val duchyAuthFromContext: DuchyAuth
  get() = requireNotNull(DUCHY_AUTH_CONTEXT_KEY.get())

private val DUCHY_AUTH_CONTEXT_KEY: Context.Key<DuchyAuth> = Context.key("duchy_auth")
private val DUCHY_ID_METADATA_KEY = Metadata.Key.of("duchy_id", Metadata.ASCII_STRING_MARSHALLER)

/**
 * Add an interceptor that sets DuchyAuth in the context.
 *
 * Note that this doesn't provide any guarantees that the Duchy is who it claims to be -- that is
 * still required.
 *
 * To install in a server, wrap your Service with:
 *    ServerInterceptors.interceptForward(yourService, DuchyServerIdentityInterceptor())
 *
 * On the client side, use [attachDuchyIdentityHeaders].
 */
class DuchyServerIdentityInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val duchyId: String? = headers.get(DUCHY_ID_METADATA_KEY)

    if (duchyId == null) {
      call.close(
        Status.UNAUTHENTICATED.withDescription("gRPC metadata missing 'duchy_id' key"),
        Metadata()
      )
      return object : ServerCall.Listener<ReqT>() {}
    }

    val context = Context.current().withValue(DUCHY_AUTH_CONTEXT_KEY, DuchyAuth(duchyId))
    return Contexts.interceptCall(context, call, headers, next)
  }
}

/**
 * Sets metadata key "duchy_id" on all outgoing requests.
 *
 * Usage:
 *   val someStub = attachDuchyIdentityHeaders(SomeServiceCoroutineStub(channel), "MyDuchyId")
 */
fun <T : AbstractStub<T>> attachDuchyIdentityHeaders(stub: T, duchyId: String): T {
  val metadata = Metadata()
  metadata.put(DUCHY_ID_METADATA_KEY, duchyId)
  return MetadataUtils.attachHeaders(stub, metadata)
}
