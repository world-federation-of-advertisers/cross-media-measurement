# Create Resource by CLI Tool

This assumes that the Kingdom is deployed to a K8s cluster, either on the local by KiND or on the cloud.

Check the [README.md](../../../../../../../k8s/local/README.md) for the instruction of local deployment.

## Setup kubectl
If the cluster is running locally, switch to the correct context by
```shell
kubectl config use-context <context>
```
Check the current context by
```shell
kubectl config current-context
```
If the cluster is running on gcloud, follow the instruction to [configure cluster access for kubectl](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)

## Forward the request to the Kingdom
List pods and find one of `gcp-kingdom-data-server-deployment`
```shell
kubectl get pods
```
Set up port forward by `kubectl`
```shell
kubectl port-forward gcp-kingdom-data-server-deployment-<pod-name> 8443:8443
```

## Send requests
Run the CLI tool with `help` to check the instruction. Provide the credentials as required. One example here
```shell
create_resource account --tls-cert-file kingdom_tls.pem --tls-key-file kingdom_tls.key --cert-collection-file kingdom_root.pem \ 
    --internal-api-cert-host=localhost --internal-api-target=localhost:8443 --certificate-der-file mc_cs_cert.der \
    --encryption-public-key-file mc_enc_public.tink --encryption-public-key-signature-file mc_cs_cert.der
```