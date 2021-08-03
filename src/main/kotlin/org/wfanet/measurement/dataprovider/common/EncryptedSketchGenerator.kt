package org.wfanet.measurement.dataprovider.common

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.anysketch.Sketch
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey

/** Interface for handling Requisitions with an [EncryptedSketch]-typed MeasurementSpec. */
interface EncryptedSketchGenerator {
  /**
   * Generates an encrypted sketch as a flow of ByteStrings.
   *
   * Each ByteString's size should be optimized for gRPC network transmission (several KiB).
   */
  suspend fun generate(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): Flow<ByteString>
}
