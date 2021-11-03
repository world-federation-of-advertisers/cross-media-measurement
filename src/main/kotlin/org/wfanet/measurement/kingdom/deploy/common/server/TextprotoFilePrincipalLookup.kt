package org.wfanet.measurement.kingdom.deploy.common.server

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.api.v2alpha.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.api.v2alpha.authorityKeyToPrincipalMap
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.kingdom.service.api.v2alpha.Principal
import org.wfanet.measurement.kingdom.service.api.v2alpha.PrincipalServerInterceptor.PrincipalLookup

/**
 * [PrincipalLookup] that reads a mapping from a file.
 *
 * @param textprotoFile contains an [AuthorityKeyToPrincipalMap] in textproto format.
 */
class TextprotoFilePrincipalLookup(textprotoFile: File) : PrincipalLookup {
  private val map: Map<ByteString, Principal<*>> by lazy {
    parseTextProto(textprotoFile, authorityKeyToPrincipalMap {})
      .entriesList
      .associate { it.authorityKeyIdentifier to it.principalResourceName }
      .mapValues {
        requireNotNull(Principal.fromName(it.value)) { "Invalid Principal name: ${it.value}" }
      }
  }

  override fun get(authorityKeyIdentifier: ByteString): Principal<*>? {
    return map[authorityKeyIdentifier]
  }
}
