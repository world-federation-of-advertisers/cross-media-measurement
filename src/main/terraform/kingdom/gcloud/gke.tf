# Copyright 2020 The Cross-Media Measurement Authors
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


# GKE cluster
resource "google_container_cluster" "halo_cmm_cluster" {
  name               = "halo-cmm-kingdom-demo-cluster"
  location           = "us-central1-a"
  remove_default_node_pool = true

  # Service account for the cluster
  service_account    = "gke-cluster@halo-kingdom-demo.iam.gserviceaccount.com"

  # Workload identity pool for the namespace
  workload_identity_config {
    workload_pool = "halo-kingdom-demo.svc.id.goog"
  }

  # Enable network policy and specify database encryption key
  network_policy {
    enabled = true
  }
  database_encryption {
    state = "ENCRYPTED"
    key_name = "projects/halo-cmm-dev/locations/us-central1/keyRings/test-key-ring/cryptoKeys/k8s-secret"
  }

  # Node pool
  node_pool {
    name = "halo-cmm-node-pool"
    initial_node_count = 3
    autoscaling {
      min_node_count = 3
      max_node_count = 6
    }
    node_config {
      machine_type = "e2-highcpu-2"
      disk_size_gb = 100
    }
  }

  # Kubernetes version
  master_version     = "1.24.5-gke.600"
  node_version       = "1.24.5-gke.600"

  # Release channel
  release_channel {
    channel = "REGULAR"
  }
}

# Kubernetes deployments
resource "kubernetes_deployment" "gcp_kingdom_data_server_deployment" {
  metadata {
    name = "gcp-kingdom-data-server-deployment"
    namespace = "halo-kingdom-demo"
    labels = {
      app = "gcp-kingdom-data-server"
    }
  }

  spec {
    replicas = 3

    selector {
      match_labels = {
        app = "gcp-kingdom-data-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "gcp-kingdom-data-server"
        }
      }

      spec {
        container {
          image = "gcr.io/halo-cmm-dev/kindom/gcp-kingdom-data-server:latest"
          name = "gcp-kingdom-data-server"
        }
      }
    }
  }
}

resource "kubernetes_deployment" "system_api_server_deployment" {
  metadata {
    name = "system-api-server-deployment"
    namespace = "halo-kingdom-demo"
    labels = {
      app = "system-api-server"
    }
  }

  spec {
    replicas = 3

    selector {
      match_labels = {
        app = "system-api-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "system-api-server"
        }
      }

      spec {
        container {
          image = "gcr.io/halo-cmm-dev/kindom/system-api-server:latest"
          name = "system-api-server"
        }
      }
    }
  }
}

resource "kubernetes_deployment" "v2alpha_public_api_server_deployment" {
  metadata {
    name = "v2alpha-public-api-server-deployment"
    namespace = "halo-kingdom-demo"
    labels = {
      app = "v2alpha-public-api-server"
    }
  }

  spec {
    replicas = 3

    selector {
      match_labels = {
        app = "v2alpha-public-api-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "v2alpha-public-api-server"
        }
      }

      spec {
        container {
          image = "gcr.io/halo-cmm-dev/kindom/system-api-server:latest"
          name = "v2alpha-public-api-server"
        }
      }
    }
  }
}

