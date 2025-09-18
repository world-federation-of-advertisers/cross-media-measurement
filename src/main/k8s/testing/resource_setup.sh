#!/usr/bin/env bash
# Copyright 2023 The Cross-Media Measurement Authors
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

readonly BAZEL="${BAZEL:-bazel}"
readonly BAZEL_BIN="$($BAZEL info bazel-bin)"

exec $BAZEL_BIN/src/main/kotlin/org/wfanet/measurement/loadtest/resourcesetup/ResourceSetup \
--cert-collection-file=src/main/k8s/testing/secretfiles/kingdom_root.pem \
--tls-cert-file=src/main/k8s/testing/secretfiles/kingdom_tls.pem \
--tls-key-file=src/main/k8s/testing/secretfiles/kingdom_tls.key \
--mc-consent-signaling-cert-der-file=src/main/k8s/testing/secretfiles/mc_cs_cert.der \
--mc-consent-signaling-key-der-file=src/main/k8s/testing/secretfiles/mc_cs_private.der \
--mc-encryption-public-keyset=src/main/k8s/testing/secretfiles/mc_enc_public.tink \
--duchy-consent-signaling-cert-der-files=aggregator=src/main/k8s/testing/secretfiles/aggregator_cs_cert.der \
--duchy-consent-signaling-cert-der-files=worker1=src/main/k8s/testing/secretfiles/worker1_cs_cert.der \
--duchy-consent-signaling-cert-der-files=worker2=src/main/k8s/testing/secretfiles/worker2_cs_cert.der \
--data-provider-display-name=edp1 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp1_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp1_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp1_enc_public.tink \
--data-provider-display-name=edp2 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp2_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp2_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp2_enc_public.tink \
--data-provider-display-name=edp3 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp3_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp3_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp3_enc_public.tink \
--data-provider-display-name=edp4 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp4_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp4_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp4_enc_public.tink \
--data-provider-display-name=edp5 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp5_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp5_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp5_enc_public.tink \
--data-provider-display-name=edp6 \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/edp6_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/edp6_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/edp6_enc_public.tink \
--data-provider-display-name=pdp \
--data-provider-consent-signaling-cert=src/main/k8s/testing/secretfiles/pdp1_cs_cert.der \
--data-provider-consent-signaling-key=src/main/k8s/testing/secretfiles/pdp1_cs_private.der \
--data-provider-encryption-public-key=src/main/k8s/testing/secretfiles/pdp1_enc_public.tink \
"$@"
