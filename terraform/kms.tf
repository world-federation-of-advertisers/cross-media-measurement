/*
resource "google_kms_key_ring" "default" {
  name = var.ring_name
  location = var.ring_location
}


data "google_kms_key_ring" "my_key_ring" {
  name = var.ring_name
  location = var.ring_location
}


resource "google_kms_crypto_key" "default" {
  name = "gce_east1_symm_key"
  key_ring = data.google_kms_key_ring.my_key_ring.id
}
data "google_iam_policy" "default" {
  binding {
    members = [
      "serviceAccount:service-1049178966878@compute-system.iam.gserviceaccount.com"
    ]
    role = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  }
}

*/
