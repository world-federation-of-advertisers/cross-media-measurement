package org.wfanet.measurement.service.v1alpha.common

import io.grpc.Context

/**
 * Details about an authenticated Duchy.
 *
 * @property[authenticatedDuchyId] Stable identifier for a duchy. Null if unauthenticated.
 */
data class DuchyAuth(val authenticatedDuchyId: String)

val duchyAuthFromContext: DuchyAuth
  get() = DUCHY_AUTH_KEY.get()!!

private val DUCHY_AUTH_KEY: Context.Key<DuchyAuth> = Context.key("duchy_auth")
