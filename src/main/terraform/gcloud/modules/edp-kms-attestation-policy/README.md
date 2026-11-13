# EDP KMS attestation policy

Generates the dev/head/qa Confidential Space conditions from approved projects,
workload service accounts, and image-signing fingerprints. Every condition
requires a stable Confidential Space image signed by an approved key.
Use SHA-256 hashes of DER-encoded signing public keys, prefixed with
`ECDSA_P256_SHA256:`; empty signers and base64 PEM values are rejected.
