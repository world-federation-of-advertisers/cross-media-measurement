/*

# Provider configuration for Google Cloud Platform
provider "google" {
  project = "your-gcp-project-id"
  region  = "us-central1"
}

# Resource for creating a GCP container registry
resource "google_container_registry_repository" "my_container_repo" {
  name       = "my-container-repo"
}

# Resource for building and pushing the Docker image to GCR
resource "google_container_builder_trigger" "my_container_build" {
  name     = "my-container-build"
  service_account_email = "your-service-account-email@your-gcp-project-id.iam.gserviceaccount.com"
  substitution {
    _IMAGE_NAME = "${google_container_registry_repository.my_container_repo.name}"
  }
  build {
    filename = "path/to/Dockerfile"
    tag_template = "latest"
    timeout = "1200s"
    source = {
      storage_source {
        bucket = "my-bucket"
        object = "path/to/source"
      }
    }
  }
  push {
    name = "${google_container_registry_repository.my_container_repo.name}:latest"
  }
}

# Resource for deploying the image to a Kubernetes cluster
resource "kubernetes_deployment" "my_container_deployment" {
  metadata {
    name = "my-container-deployment"
    labels = {
      app = "my-container"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "my-container"
      }
    }
    template {
      metadata {
        labels = {
          app = "my-container"
        }
      }
      spec {
        container {
          name = "my-container"
          image = "gcr.io/${google_container_registry_repository.my_container_repo.name}:latest"
          ports {
            container_port = 8080
          }
        }
      }
    }
  }
}


#gcr
resource "google_container_registry" "registry" {
  name     = "my-registry"
  location = "us-central1"
  project  = var.project_id
}


*/