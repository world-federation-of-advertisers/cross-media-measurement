package org.wfanet.measurement.reporting.service.api.v1alpha.crypto

import com.google.protobuf.ByteString
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.crypto.PrivateKeyHandle

/**
 * Decrypts the encrypted signed [MeasurementResult].
 *
 * @param encryptedSignedDataResult an encrypted [SignedData] containing a [MeasurementResult].
 * @param measurementPrivateKey the encryption private key matching the Measurement public key.
 */
fun decryptResult(
  encryptedSignedDataResult: ByteString,
  measurementPrivateKey: PrivateKeyHandle
): SignedData {
  return SignedData.parseFrom(measurementPrivateKey.hybridDecrypt(encryptedSignedDataResult))
}