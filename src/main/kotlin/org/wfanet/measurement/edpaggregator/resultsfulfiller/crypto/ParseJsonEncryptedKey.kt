package org.wfanet.measurement.edpaggregator.resultsfulfiller.crypto

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.util.JsonFormat
import com.google.crypto.tink.proto.AesGcmHkdfStreamingParams
import com.google.crypto.tink.proto.AesGcmHkdfStreamingKey
import com.google.crypto.tink.proto.HashType
import com.google.crypto.tink.proto.KeyData
import com.google.crypto.tink.proto.KeyStatusType
import com.google.crypto.tink.proto.Keyset
import com.google.crypto.tink.proto.OutputPrefixType
import java.security.SecureRandom
import kotlin.math.absoluteValue
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptionKey
import org.wfanet.measurement.edpaggregator.v1alpha.HashType as EdpAggregatorHashType

fun parseJsonEncryptedKey(
    encryptedDek: ByteArray,
    kekAead: Aead,
    associatedData: ByteArray?
): KeysetHandle {
    val decrypted = kekAead.decrypt(encryptedDek, associatedData ?: byteArrayOf())

    val builder = EncryptionKey.newBuilder()
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(decrypted.toString(Charsets.UTF_8), builder)

    val encryptionKey = builder.build()

    val aesKey = when (encryptionKey.keyCase) {
        EncryptionKey.KeyCase.AES_GCM_HKDF_STREAMING_KEY -> encryptionKey.aesGcmHkdfStreamingKey
        EncryptionKey.KeyCase.KEY_NOT_SET ->
            throw IllegalArgumentException("EncryptionKey has no key_type set")
    }

    val params = AesGcmHkdfStreamingParams.newBuilder()
        .setDerivedKeySize(aesKey.params.derivedKeySize)
        .setHkdfHashType(aesKey.params.hkdfHashType.mapHashTypeCloneToTink())
        .setCiphertextSegmentSize(aesKey.params.ciphertextSegmentSize)
        .build()

    val tinkKey = AesGcmHkdfStreamingKey.newBuilder()
        .setParams(params)
        .setKeyValue(aesKey.keyValue)
        .setVersion(aesKey.version)
        .build()

    val keyData = KeyData.newBuilder()
        .setTypeUrl("type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey")
        .setKeyMaterialType(KeyData.KeyMaterialType.SYMMETRIC)
        .setValue(tinkKey.toByteString())
        .build()

    val keyId = SecureRandom().nextInt().absoluteValue
    val ks = Keyset.newBuilder()
        .setPrimaryKeyId(keyId)
        .addKey(
            Keyset.Key.newBuilder()
                .setKeyId(keyId)
                .setStatus(KeyStatusType.ENABLED)
                .setOutputPrefixType(OutputPrefixType.RAW)
                .setKeyData(keyData)
        )
        .build()

    return CleartextKeysetHandle.read(BinaryKeysetReader.withBytes(ks.toByteArray()))
}

private fun EdpAggregatorHashType.mapHashTypeCloneToTink(): HashType =
    when (this) {
        EdpAggregatorHashType.SHA1 -> HashType.SHA1
        EdpAggregatorHashType.SHA256 -> HashType.SHA256
        EdpAggregatorHashType.SHA512 -> HashType.SHA512
        else -> throw IllegalArgumentException("Unsupported hkdf_hash_type: $this")
    }