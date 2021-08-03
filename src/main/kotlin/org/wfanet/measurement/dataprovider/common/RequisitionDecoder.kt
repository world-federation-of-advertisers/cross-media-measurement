package org.wfanet.measurement.dataprovider.fake

import org.wfanet.measurement.api.v2alpha.*

/** Interface for [Requisition] signature validation and decryption. */
interface RequisitionDecoder {
  fun decodeMeasurementSpec(requisition: Requisition): MeasurementSpec
  fun decodeRequisitionSpec(requisition: Requisition): RequisitionSpec
}

/** Indicates an invalid cryptographic signature. */
class InvalidSignatureException(override val message: String) : Exception()

/** Indicates a failure to decrypt. */
class DecryptionException(override val message: String) : Exception()

/**
 * [RequisitionDecoder] that makes no attempt to perform any cryptography.
 *
 * It assumes that there is no encryption and does not attempt to validate signatures.
 *
 * DO NOT use this in production.
 */
class FakeRequisitionDecoder : RequisitionDecoder {
  override fun decodeMeasurementSpec(requisition: Requisition): MeasurementSpec {
    val serializedMeasurementSpec = requisition.measurementSpec
    return MeasurementSpec.parseFrom(serializedMeasurementSpec.data)
  }

  override fun decodeRequisitionSpec(requisition: Requisition): RequisitionSpec {
    val signedData = SignedData.parseFrom(requisition.encryptedRequisitionSpec)
    return RequisitionSpec.parseFrom(signedData.data)
  }
}
