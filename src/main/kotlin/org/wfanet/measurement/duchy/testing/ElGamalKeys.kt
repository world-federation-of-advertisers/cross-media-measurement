package org.wfanet.measurement.duchy.testing

import org.wfanet.measurement.duchy.mill.toElGamalKeys
import org.wfanet.measurement.duchy.mill.toElGamalPublicKeys
import org.wfanet.measurement.internal.duchy.ElGamalKeys
import org.wfanet.measurement.internal.duchy.ElGamalPublicKeys

private const val DUCHY_PK_G =
  "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"

private const val DUCHY_1_PK_Y =
  "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3"
private const val DUCHY_1_SK = "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
private const val DUCHY_2_PK_Y =
  "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3"
private const val DUCHY_2_SK = "31cc32e7cd53ff24f2b64ae8c531099af9867ebf5d9a659f742459947caa29b0"
private const val DUCHY_3_PK_Y =
  "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7"
private const val DUCHY_3_SK = "338cce0306416b70e901436cb9eca5ac758e8ff41d7b58dabadf8726608ca6cc"
private const val CLIENT_PK_G = "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
private const val CLIENT_PK_Y = "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04"

object TestKeys {
  val EL_GAMAL_KEYS: List<ElGamalKeys> = listOf(
    (DUCHY_PK_G + DUCHY_1_PK_Y + DUCHY_1_SK).toElGamalKeys(),
    (DUCHY_PK_G + DUCHY_2_PK_Y + DUCHY_2_SK).toElGamalKeys(),
    (DUCHY_PK_G + DUCHY_3_PK_Y + DUCHY_3_SK).toElGamalKeys()
  )

  val COMBINED_EL_GAMAL_PUBLIC_KEY: ElGamalPublicKeys =
    (CLIENT_PK_G + CLIENT_PK_Y).toElGamalPublicKeys()

  const val CURVE_ID: Int = 415 // NID_X9_62_prime256v1
}
