# Crypto Test Data

## Certificates

The following commands assume that the default config exists at
`/etc/ssl/openssl.cnf`.

### Generate CA Certificate

```
openssl req -out ca.pem -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout ca.key -x509 -days 3650 -subj '/O=Server CA/CN=ca.server.example.com' -config openssl.cnf -extensions v3_ca
```

### Generate Server Certificate Signing Request

```
openssl req -out server.csr -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout server.key -subj '/O=Server/CN=server.example.com' -config openssl.cnf -extensions v3_req
```

### Sign Server Certificate Request

```
openssl x509 -in server.csr -out server.pem -days 3650 -req -CA ca.pem -CAkey ca.key -CAcreateserial -extfile openssl.cnf -extensions usr_cert
```

## Key Pairs

### Generate Private Key

```
openssl genpkey -out ec.key -algorithm ec -pkeyopt ec_paramgen_curve:prime256v1
```

### Extract Public Key as SubjectPublicKeyInfo

```
openssl pkey -pubout -outform der -in ec.key -out ec-public.der
```
