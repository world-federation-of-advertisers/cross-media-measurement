
/*
  This is a terraform provider that will tie the terraform with the GCP account that we have.
  The JSON mentioned here should never be pushed with the code.
*/
provider "google" {
  project = var.project
}


terraform {
  backend "gcs" {
  }
}



