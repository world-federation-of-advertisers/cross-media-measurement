# Certificates

The Cross-Media Measurement API uses X.509 certificates for digital signatures.
The certificates returned in API resources are never root certificates, and
should be verified using a certificate that has been exchanged outside of the
API.

## Certificate Preference

Some resources have a field that refers to a *preferred* certificate resource.
The following criteria, in order of most to least significant, determine
certificate preference within a collection:

1.  Not revoked.
1.  Within its validity period.
1.  Has a later end to its validity period.
1.  Has a later start to its validity period.

While it is expected that certificates with later end dates will also have later
start dates, this is not enforced by the API.
