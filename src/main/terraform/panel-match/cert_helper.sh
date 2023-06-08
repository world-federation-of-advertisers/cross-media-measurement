#!/usr/bin/env bash
# Copyright 2022 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# These get auto-populated from Terraform
# If they get modified outside of the initial terraform apply, then you may have to manually update these
cert_arn=""
path_to_cue_file_wext=""
path_to_secrets=""

path_to_cue_file=${path_to_cue_file_wext%.*}

echo "Enter a unique identifier for your panel exchange certs:"
read identifier

echo "Enter the id assigned to you by Kingdom:"
read mp_id

echo "Enter a passphrase for generating certificates:"
read -s passphrase

# request certificate
echo "||| Generate certificate request"
cert_response=$(aws acm request-certificate \
--certificate-authority-arn $cert_arn \
--domain-name www.example.com \
--validation-method DNS \
--idempotency-token 1234)

cert_request_arn=$(jq -r ".CertificateArn" <<< $cert_response)

# wait for the cert to be ready
echo "||| wait for the cert request to be ready..."
sleep 20

# get certificate pieces
#cert_request_arn="arn:aws:acm:us-west-2:010295286036:certificate/d0dd8350-2fc5-42a1-9e62-bcec81cef44f"
echo "||| Export Certificate - certificate"
aws acm export-certificate \
     --certificate-arn $cert_request_arn \
     --passphrase $(echo -n $passphrase | openssl base64) \
     --output json \
     | jq -r '"\(.Certificate)"' \
     > $path_to_secrets/${identifier}_tls.pem

echo "||| Export Certificate - private key"
aws acm export-certificate \
     --certificate-arn $cert_request_arn \
     --passphrase $(echo -n $passphrase | openssl base64) \
     --output json \
     | jq -r '"\(.PrivateKey)"' \
     > $path_to_secrets/${identifier}_tls.enc.key

# Change private key format
echo "||| Reformat private key to pkcs8"
openssl pkcs8 -in $path_to_secrets/${identifier}_tls.enc.key -out $path_to_secrets/${identifier}_tls.key -passin pass:$passphrase

# Update BUILD file
echo "||| Update BUILD file"
if ! grep -q "${identifier}_tls.pem" $path_to_secrets/BUILD.bazel; then
  sed -i 's|SECRET_FILES = \[|SECRET_FILES = [\n    "'$identifier'_tls.pem",|' $path_to_secrets/BUILD.bazel
fi

if ! grep -q "${identifier}_tls.key" $path_to_secrets/BUILD.bazel; then
  sed -i 's|SECRET_FILES = \[|SECRET_FILES = [\n    "'$identifier'_tls.key",|' $path_to_secrets/BUILD.bazel
fi

# Update kustomization file
echo "||| Update kustomization file"
if ! grep -q "${identifier}_tls.pem" $path_to_secrets/kustomization.yaml; then
  sed -i 's|Files:|Files:\n  - '$identifier'_tls.pem|' $path_to_secrets/kustomization.yaml
fi

if ! grep -q "${identifier}_tls.key" $path_to_secrets/kustomization.yaml; then
  sed -i 's|Files:|Files:\n  - '$identifier'_tls.key|' $path_to_secrets/kustomization.yaml
fi

# Update cue file
echo "||| Update cue file"
sed -i -E 's|certFile: ".*"|certFile: "/var/run/secrets/files/'$identifier'_tls.pem"|' $path_to_cue_file.cue
sed -i -E 's|keyFile: ".*"|keyFile: "/var/run/secrets/files/'$identifier'_tls.key"|' $path_to_cue_file.cue

# Rebuild trusted certs
echo "||| Rebuilding trusted certs file"
cat $path_to_secrets/*_root.pem > $path_to_secrets/trusted_certs.pem

# Apply K8S secrets
echo "||| Apply K8S secrets"
kubectl apply -k $path_to_secrets

# Apply Kustomization
echo "||| Apply kustomization"
str=$(bazel run $path_to_secrets/apply_kustomization)
regex="(certs-and-configs-\S*)"
[[ $str =~ $regex ]]
secret_name=${BASH_REMATCH[0]}

# Rebuild the manifest
echo "||| Rebuild the manifest"
bazel build //src/main/k8s/dev:example_mp_daemon_aws --define=mp_name=modelProviders/$mp_id --define=mp_k8s_secret_name=$secret_name

# Apply the manifest to K8S
# Fix the path since it's likely relative:
#   https://stackoverflow.com/questions/16109353/strip-double-dots-from-path-in-bash
echo "||| Apply the manifest to K8s"
bazel_cue_path=$(sed -e 's|/\./|/|g' -e ':a' -e 's|\.\./\.\./|../..../|g' -e 's|^[^/]*/\.\.\/||' -e 't a' -e 's|/[^/]*/\.\.\/|/|' -e 't a' -e 's|\.\.\.\./|../|g' -e 't a' <<< \
  "bazel-bin/src/main/terraform/$path_to_cue_file.yaml")
kubectl apply -f ../../../$bazel_cue_path

# Redploy the cluster
echo "||| Redploying the cluster"
kubectl rollout restart deployment example-panel-exchange-daemon-deployment

