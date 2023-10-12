# Certificate and Encryption Key Rotation

How to rotate leaf certificates and encryption keys. Rotation is the process of
changing the key/certificate, i.e. adding a new one to start using instead of an
old one.

Rotation limits the risk of a compromise, e.g. a private key being leaked.

## Background

The CMMS uses
[X.509 certificates](https://en.wikipedia.org/wiki/X.509#Certificates) for
[transport layer security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security)
and for [digital signatures](https://en.wikipedia.org/wiki/Digital_signature).
It also uses [hybrid encryption](https://developers.google.com/tink/hybrid) to
encrypt various pieces of data.

These are asymmetric schemes, where there is a public and a private component.
The public component (e.g. a certificate or encryption public key) is presented
to the recipient of the interaction. The private component is kept secret by the
sender.

### Certificate trust model

Each leaf certificate is issued by a certificate authority (CA). Each root CA
has its own self-signed certificate. This is commonly referred to as a *root
certificate*. Root certificates tend to have a very long validity period (e.g.
10 years). In the CMMS, each entity has their own private CA. Entities exchange
their root certificates out-of-band.

**Warning**: Root certificates are *very* difficult to rotate. As a result, the
private key associated with a root certificate must be kept very secure. Often
this is done by utilizing a CA service such as Google Cloud Certificate
Authority Service or AWS Private CA. These ensure that the private key is never
directly accessible. We use OpenSSL to act as a CA for testing, but this should
not be done in production.

Note: In addition to root CAs there can also be intermediate CAs. These are not
covered in the guide for simplicity.

The path from a leaf certificate to a trusted issuer is referred to as a
*certificate chain*. The trusted issuer in said chain is called its *trust
anchor*. Part of validating a certificate includes verifying this certificate
chain, i.e. ensuring that you have a trust anchor for it. Another part of
certificate validation is checking its validity period (e.g. whether the
certificate is expired) and determining its revocation status.

For server TLS, the server acts as the "sender" in the above definition. It
presents a certificate to the client, which it can prove it owns by virtue of
having its private key. The client verifies that it trusts that certificate by
ensuring that has the matching root certificate in its collection of trusted
certificates. It will also verify that the hostname of the address that it's
connecting to matches one of the names in the certificate's subject alternative
name (SAN) extension. TLS clients can override the hostname used in this
verification if needed, for example if connecting via Kubernetes cluster IP.

For mutual TLS (mTLS) AKA client TLS, the client additionally presents its own
certificate. The server similarly verifies this matches one of the root
certificates in its collection of trusted certificates. The server can use the
authority key identifier (AKID) of the client certificate to determine the
identity of the client by mapping it to the known owner of the root certificate
with the corresponding subject key identifier (SKID).

When certificates are used for digital signatures, the sender uses their private
key to generate a signature of some data. This process is known as *signing*.
The recipient generally first verifies that the certificate is valid/trusted as
above. It can then use the certificate to verify that the signature matches the
data.

### Asymmetric encryption

The sender encrypts some data using the recipient's encryption public key. The
recipient can then decrypt the data using their private key.

## Rotation

### TLS certificate rotation

This is changing the certificate that the sender (the server in server TLS or
the client in client TLS) presents. In Kubernetes, this generally involves
replacing the certificate and matching private key within a
[`Secret`](https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/secret-v1/).
Assuming you have a Kustomization directory as described in the
[update guide](updating-release.md), simply overwrite the files and run `kubectl
apply`.

### API certificate rotation

There are two parts to rotating a certificate within the CMMS public API.

1.  Register a new
    [`Certificate` resource](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/certificate.proto).

    Call the
    [`CreateCertificate` method](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/certificates_service.proto)
    for the appropriate parent resource. You can then reference this
    `Certificate` in other API calls.

2.  Update the
    [`PublicKey` sub-resource](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/public_key.proto).

    If the parent resource has a `PublicKey` sub-resource, you can update it to
    reference the new `Certificate`. You will need to sign the
    `EncryptionPublicKey` with the matching private key for the new certificate,
    and then pass that to the `UpdatePublicKey` method.

    Note: You can do this at the same time as rotating the encryption public
    key. See below.

You can use the `MeasurementSystem` and `EncryptionPublicKeys`
[CLI tools](../../src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools) to
assist with this.

### Encryption public key rotation

This can be done by calling the
[`UpdatePublicKey` method](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/public_keys_service.proto).
You will need to generate a new signature for the encryption public key using
the private key that matches the `Certificate` resource.

You can use the `MeasurementSystem` and `EncryptionPublicKeys`
[CLI tools](../../src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools) to
assist with this.

## Compromise

If your certificate or encryption key has been compromised, simple rotation is
not sufficient to solve the problem. There are additional impacts and steps that
may be taken to mitigate them.

### Certificate compromise

In this case, all digital signatures using the certificate must be considered
invalid. This is similar to what happens when a certificate expires. For
example, Measurement results utilizing the compromised certificate can no longer
be trusted. In order to obtain a trustable result, new Measurements may need to
be created.

Mitigations:

1.  Revoke the certificate.

    This prevents use of the certificate for anything new. In the CMMS,
    revocation status is communicated on the `Certificate` resource. This state
    can be updated using the `RevokeCertificate` method.

### Encryption key compromise

In this case, all data encrypted with the key is no longer private to the
recipient.

Mitigations:

1.  Re-encrypt data with a new key.

    For example, if a `MeasurementConsumer` key was compromised then the MC
    should request that `DataProvider`s re-encrypt any `EventGroup` metadata.

2.  Limit access to ciphertexts.

    In cases where data cannot be re-encrypted (for example, data that is
    treated as immutable in the API such as `RequisitionSpec` or
    `Measurement.Result`), further limiting access to the encrypted data can
    reduce the potential impact.

    Depending on the risk assessment and tolerance, deleting the data is an
    extreme form of limiting access. For example, contacting the Kingdom
    operator to delete Measurements using the internal API.
