# Copyright 2020 The Measurement System Authors
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

"""
Make variable strings used by build targets in project.
"""

# Settings for the repositories where container images are stored.
IMAGE_REPOSITORY_SETTINGS = struct(
    # The container registry for targets which push or pull container images.
    #
    # For example, `gcr.io` for Google Cloud Container Registry or `docker.io`
    # for DockerHub.
    container_registry = "$(container_registry)",

    # Common prefix of container image repositories.
    repository_prefix = "$(image_repo_prefix)",

    # Common tag for container images.
    image_tag = "$(image_tag)",
)

# Settings for test Kubernetes deployments.
TEST_K8S_SETTINGS = struct(
    # Resource name of the aggregator Duchy's Certificate.
    aggregator_cert_name = "$(aggregator_cert_name)",
    # Resource name of the 1st worker Duchy's Certificate.
    worker1_cert_name = "$(worker1_cert_name)",
    # Resource name of the 2nd worker Duchy's Certificate.
    worker2_cert_name = "$(worker2_cert_name)",
    mc_name = "$(mc_name)",
    mc_api_key = "$(mc_api_key)",
    edp1_name = "$(edp1_name)",
    edp2_name = "$(edp2_name)",
    edp3_name = "$(edp3_name)",
    edp4_name = "$(edp4_name)",
    edp5_name = "$(edp5_name)",
    edp6_name = "$(edp6_name)",
    db_secret_name = "$(k8s_db_secret_name)",
    mc_config_secret_name = "$(k8s_mc_config_secret_name)",
    grafana_secret_name = "$(k8s_grafana_secret_name)",
)

# Settings for Kingdom Kubernetes deployments.
KINGDOM_K8S_SETTINGS = struct(
    secret_name = "$(k8s_kingdom_secret_name)",
)

# Settings for Duchy Kubernetes deployments.
DUCHY_K8S_SETTINGS = struct(
    secret_name = "$(k8s_duchy_secret_name)",
    certificate_id = "$(duchy_cert_id)",
    storage_bucket = "$(duchy_storage_bucket)",
)

# Settings for simulator Kubernetes deployments.
SIMULATOR_K8S_SETTINGS = struct(
    secret_name = "$(k8s_simulator_secret_name)",
    storage_bucket = "$(simulator_storage_bucket)",
    mc_name = "$(mc_name)",
    mc_api_key = "$(mc_api_key)",
    edp1_name = "$(edp1_name)",
    edp2_name = "$(edp2_name)",
    edp3_name = "$(edp3_name)",
    edp4_name = "$(edp4_name)",
    edp5_name = "$(edp5_name)",
    edp6_name = "$(edp6_name)",
)

# Settings for Reporting server Kubernetes deployments.
REPORTING_K8S_SETTINGS = struct(
    secret_name = "$(k8s_reporting_secret_name)",
    mc_config_secret_name = "$(k8s_reporting_mc_config_secret_name)",
)

# Settings for Grafana Kubernetes deployments.
GRAFANA_K8S_SETTINGS = struct(
    secret_name = "$(k8s_grafana_secret_name)",
)
