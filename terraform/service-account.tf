module "service_accounts" {
  source        = "terraform-google-modules/service-accounts/google"
  version       = "~> 3.0"
  project_id    = var.project
  prefix        = "test-sa"
  names         = ["internal", "bazel-609","gke-cluster"]
}


