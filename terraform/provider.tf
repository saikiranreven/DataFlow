terraform {
  backend "gcs" {
    bucket = "dtf-state-bucket"
    prefix = "terraform/state"
  }
}

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

resource "google_bigquery_dataset" "streaming_dataset" {
  dataset_id    = "streaming_dataset"
  friendly_name = "Streaming data dataset"
  description   = "Dataset for processed streaming data"
  location      = var.region
}

resource "google_bigquery_table" "user_events" {
  dataset_id = google_bigquery_dataset.streaming_dataset.dataset_id
  table_id   = "user_events"

  schema = <<EOF
[
  {
    "name": "user_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "action",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ingest_time",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}