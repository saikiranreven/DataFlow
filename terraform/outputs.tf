output "pubsub_topic" {
  value = google_pubsub_topic.stream_topic.name
}

output "bucket_name" {
  value = google_storage_bucket.raw_bucket.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.streaming_dataset.dataset_id
}

output "bigquery_table" {
  value = google_bigquery_table.user_events.table_id
}