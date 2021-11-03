package org.wfanet.measurement.kingdom.deploy.common.server

import java.io.File
import picocli.CommandLine

/** Flags specific to the V2alpha API version. */
class V2alphaFlags {
  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
    required = true,
  )
  lateinit var authorityKeyIdentifierToPrincipalMapFile: File
    private set
}
