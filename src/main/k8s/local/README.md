# Local Kubernetes Deployment

How to deploy system components to a local Kubernetes cluster running in
[KiND](https://kind.sigs.k8s.io/).

## Create Secret

```shell
kubectl apply -k src/main/k8s/testing/secretfiles/
```

The secret name will be printed on creation, but it can also be obtained later
by running

```shell
kubectl get secrets
```

## Deploy Kingdom

Supposing the secret name is `certs-and-configs-2m48c7m6m6`, then the command to
deploy the Kingdom and set up some initial resources would be

```shell
bazel run //src/main/k8s/local:kingdom_kind --define=k8s_secret_name=certs-and-configs-2m48c7m6m6
```

After the resource setup job has completed, you can obtain the created resource
names from its logs.

```shell
kubectl logs -f jobs/resource-setup-job
```

## Create `config-files` ConfigMap

There are some configuration files that depend on API resource names, so they
must be created after resource-setup-job has completed. Substitute the
appropriate resource names below.

`authority_key_identifier_to_principal_map.textproto`

```prototext
# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\xD6\x65\x86\x86\xD8\x7E\xD2\xC4\xDA\xD8\xDF\x76\x39\x66\x21\x3A\xC2\x92\xCC\xE2"
  principal_resource_name: "dataProviders/UwZoypqu5D8"
}
entries {
  authority_key_identifier: "\x6F\x57\x36\x3D\x7C\x5A\x49\x7C\xD1\x68\x57\xCD\xA0\x44\xDF\x68\xBA\xD1\xBA\x86"
  principal_resource_name: "dataProviders/N2Z2hWVRGzI"
}
entries {
  authority_key_identifier: "\xEE\xB8\x30\x10\x0A\xDB\x8F\xEC\x33\x3B\x0A\x5B\x85\xDF\x4B\x2C\x06\x8F\x8E\x28"
  principal_resource_name: "dataProviders/BDnPTZqu5PM"
}
entries {
  authority_key_identifier: "\x74\x72\x6D\xF6\xC0\x44\x42\x61\x7D\x9F\xF7\x3F\xF7\xB2\xAC\x0F\x9D\xB0\xCA\xCC"
  principal_resource_name: "dataProviders/EiOed2VRGuY"
}
entries {
  authority_key_identifier: "\xA6\xED\xBA\xEA\x3F\x9A\xE0\x72\x95\xBF\x1E\xD2\xCB\xC8\x6B\x1E\x0B\x39\x47\xE9"
  principal_resource_name: "dataProviders/EZ4Vqpqu5UM"
}
entries {
  authority_key_identifier: "\xA7\x36\x39\x6B\xDC\xB4\x79\xC3\xFF\x08\xB6\x02\x60\x36\x59\x84\x3B\xDE\xDB\x93"
  principal_resource_name: "dataProviders/RGNQkWVRGqA"
}
```

Create the ConfigMap passing the `--from-file` option for each config file.

```shell
kubectl create configmap config-files --from-file=authority_key_identifier_to_principal_map.textproto
```

## Deploy Duchies

The testing environment uses three Duchies: an aggregator and two workers, named
`aggregator`, `worker1`, and `worker2` respectively. Substitute the appropriate
secret name and Certificate resource names in the command below.

```shell
//src/main/k8s/local:duchies_kind --define=k8s_secret_name=certs-and-configs-bmt62gbkh5 --define=aggregator_cert_name=duchies/aggregator/certificates/f3yI3aoXukM --define=worker1_cert_name=duchies/worker1/certificates/QtffTVXoRno --define=worker2_cert_name=duchies/worker2/certificates/eIYIf6oXuSM
```

## Deploy EDP Simulators

The testing environment simulates six DataProviders, named `edp1` through
`edp6`. Substitute the appropriate secret name and resource names in the command
below.

```shell
bazel run //src/main/k8s/local:edp_simulators_kind --define=k8s_secret_name=certs-and-configs-bmt62gbkh5 --define=mc_name=measurementConsumers/FS1n8aTrck0 --define=edp1_name=dataProviders/cwoo61sUbB8 --define=edp2_name=dataProviders/JL5yoqTrkYs --define=edp3_name=dataProviders/WEmC-VsUbzo --define=edp4_name=dataProviders/RkZu-6Trj5Y --define=edp5_name=dataProviders/Zi2U9FsUcTE --define=edp6_name=dataProviders/HyQJD6TrjgU
```
