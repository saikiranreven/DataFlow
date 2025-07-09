provider "google" {
  project = var.project_id
  region  = var.region
}

# Pub/Sub Topic
resource "google_pubsub_topic" "stream_topic" {
  name = "stream-topic"
}

# Cloud Storage Bucket
resource "google_storage_bucket" "raw_bucket" {
  name           = "${var.project_id}-raw-backup"
  location       = var.region
  force_destroy  = true
}