output "pubsub_topic" {
  value = google_pubsub_topic.stream_topic.name
}

output "bucket_name" {
  value = google_storage_bucket.raw_bucket.name
}

output "dataflow_service_account" {
  value = "dataflow-ca@bct-project-465419.iam.gserviceaccount.com"
}